use std::{
    io::Cursor,
    path::{Component, Path, PathBuf},
};

use axum::extract::multipart::Field;
use image::{
    DynamicImage, ImageDecoder, ImageFormat, ImageReader, Limits,
    codecs::{jpeg::JpegEncoder, png::PngEncoder, webp::WebPEncoder},
};
use tokio::{fs, io::AsyncWriteExt};
use uuid::Uuid;

use crate::error::{AppError, AppResult};

#[derive(Clone, Copy, Debug)]
pub enum MediaKind {
    ProfilePhoto,
    GroupPhoto,
    MessageAudio,
    MessageImage,
}

impl MediaKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ProfilePhoto => "profile",
            Self::GroupPhoto => "group",
            Self::MessageAudio => "audio",
            Self::MessageImage => "image",
        }
    }

    fn max_bytes(self) -> usize {
        match self {
            Self::ProfilePhoto | Self::GroupPhoto | Self::MessageImage => 5 * 1024 * 1024,
            Self::MessageAudio => 20 * 1024 * 1024,
        }
    }

    fn allows(self, content_type: &str) -> bool {
        let lc = content_type.to_ascii_lowercase();
        match self {
            Self::ProfilePhoto | Self::GroupPhoto | Self::MessageImage => matches!(
                lc.as_str(),
                "image/jpeg" | "image/jpg" | "image/png" | "image/webp"
            ),
            Self::MessageAudio => {
                lc.starts_with("audio/")
                    && matches!(
                        lc.as_str(),
                        "audio/m4a"
                            | "audio/x-m4a"
                            | "audio/mp4"
                            | "audio/aac"
                            | "audio/mpeg"
                            | "audio/mp3"
                            | "audio/ogg"
                            | "audio/webm"
                    )
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct LocalStorage {
    pub root: PathBuf,
    pub public_base_url: String,
}

#[derive(Debug)]
pub struct SavedMedia {
    pub storage_path: String,
    pub public_url: String,
    pub content_type: String,
    pub size_bytes: i64,
    pub width: Option<i32>,
    pub height: Option<i32>,
}

impl LocalStorage {
    pub fn new(root: PathBuf, public_base_url: String) -> Self {
        Self {
            root,
            public_base_url: public_base_url.trim_end_matches('/').to_string(),
        }
    }

    pub async fn save(
        &self,
        kind: MediaKind,
        relative_dir: &str,
        file_stem: &str,
        field: Field<'_>,
    ) -> AppResult<SavedMedia> {
        let content_type = field
            .content_type()
            .map(|value| value.to_string())
            .unwrap_or_else(|| "application/octet-stream".to_string());
        if !kind.allows(&content_type) {
            return Err(AppError::BadRequest(format!(
                "unsupported content-type {content_type} for {}",
                kind.as_str()
            )));
        }

        let bytes = field
            .bytes()
            .await
            .map_err(|err| AppError::BadRequest(format!("invalid multipart payload: {err}")))?;
        let size_bytes = bytes.len();
        if size_bytes == 0 {
            return Err(AppError::BadRequest("empty upload".into()));
        }
        if size_bytes > kind.max_bytes() {
            return Err(AppError::BadRequest(format!(
                "file too large: {size_bytes} bytes (limit {})",
                kind.max_bytes()
            )));
        }

        // The extension decides the Content-Type this file is later served with, so it
        // must never come from the client-supplied filename. For images the real format
        // is sniffed from the bytes; for audio the allowlisted content-type is the source.
        let (bytes, content_type, extension, width, height) = match kind {
            MediaKind::MessageImage | MediaKind::ProfilePhoto | MediaKind::GroupPhoto => {
                let image = tokio::task::spawn_blocking(move || sanitize_image(&bytes))
                    .await
                    .map_err(|err| AppError::Internal(err.into()))??;
                (
                    image.bytes,
                    image.content_type.to_string(),
                    image.extension,
                    Some(image.width),
                    Some(image.height),
                )
            }
            MediaKind::MessageAudio => {
                let extension = audio_extension(&content_type);
                (bytes.to_vec(), content_type, extension, None, None)
            }
        };
        let size_bytes = bytes.len();

        let filename = format!("{file_stem}.{extension}");
        let dir = self.root.join(relative_dir);
        fs::create_dir_all(&dir)
            .await
            .map_err(|err| AppError::Internal(err.into()))?;

        let absolute = dir.join(&filename);
        let mut file = fs::File::create(&absolute)
            .await
            .map_err(|err| AppError::Internal(err.into()))?;
        file.write_all(&bytes)
            .await
            .map_err(|err| AppError::Internal(err.into()))?;
        file.flush()
            .await
            .map_err(|err| AppError::Internal(err.into()))?;

        let storage_path = format!("{relative_dir}/{filename}");
        let public_url = format!("{}/{}", self.public_base_url, storage_path);

        Ok(SavedMedia {
            storage_path,
            public_url,
            content_type,
            size_bytes: size_bytes as i64,
            width,
            height,
        })
    }

    pub async fn delete_relative(&self, storage_path: &str) {
        let Some(candidate) = self.resolve_relative(storage_path) else {
            tracing::warn!(storage_path, "refusing to delete media outside media root");
            return;
        };
        let _ = fs::remove_file(candidate).await;
    }

    pub fn resolve_relative(&self, storage_path: &str) -> Option<PathBuf> {
        if storage_path.is_empty() {
            return None;
        }
        let relative = Path::new(storage_path);
        if relative.is_absolute() {
            return None;
        }
        let safe = relative.components().all(|component| {
            matches!(
                component,
                Component::Normal(_) | Component::CurDir
            )
        });
        if !safe {
            return None;
        }
        Some(self.root.join(relative))
    }
}

fn audio_extension(content_type: &str) -> &'static str {
    match content_type.to_ascii_lowercase().as_str() {
        "audio/aac" => "aac",
        "audio/mpeg" | "audio/mp3" => "mp3",
        "audio/ogg" => "ogg",
        "audio/webm" => "webm",
        _ => "m4a",
    }
}

struct SanitizedImage {
    bytes: Vec<u8>,
    content_type: &'static str,
    extension: &'static str,
    width: i32,
    height: i32,
}

const JPEG_QUALITY: u8 = 90;
const MAX_IMAGE_SIDE: u32 = 8192;

fn sanitize_image(bytes: &[u8]) -> AppResult<SanitizedImage> {
    let invalid = || AppError::BadRequest("upload is not a valid image".into());
    let mut reader = ImageReader::new(Cursor::new(bytes))
        .with_guessed_format()
        .map_err(|_| invalid())?;
    let mut limits = Limits::default();
    limits.max_image_width = Some(MAX_IMAGE_SIDE);
    limits.max_image_height = Some(MAX_IMAGE_SIDE);
    reader.limits(limits);
    let format = reader.format().ok_or_else(invalid)?;
    let mut decoder = reader.into_decoder().map_err(|_| invalid())?;
    let orientation = decoder.orientation().map_err(|_| invalid())?;
    let mut image = DynamicImage::from_decoder(decoder).map_err(|_| invalid())?;
    image.apply_orientation(orientation);

    let mut encoded = Vec::new();
    let (content_type, extension) = match format {
        ImageFormat::Jpeg => {
            JpegEncoder::new_with_quality(&mut encoded, JPEG_QUALITY)
                .encode_image(&DynamicImage::ImageRgb8(image.to_rgb8()))
                .map_err(|err| AppError::Internal(err.into()))?;
            ("image/jpeg", "jpg")
        }
        ImageFormat::Png => {
            image
                .write_with_encoder(PngEncoder::new(&mut encoded))
                .map_err(|err| AppError::Internal(err.into()))?;
            ("image/png", "png")
        }
        ImageFormat::WebP => {
            DynamicImage::ImageRgba8(image.to_rgba8())
                .write_with_encoder(WebPEncoder::new_lossless(&mut encoded))
                .map_err(|err| AppError::Internal(err.into()))?;
            ("image/webp", "webp")
        }
        other => {
            return Err(AppError::BadRequest(format!(
                "unsupported image format {other:?}"
            )));
        }
    };

    Ok(SanitizedImage {
        bytes: encoded,
        content_type,
        extension,
        width: image.width() as i32,
        height: image.height() as i32,
    })
}

pub fn profile_photo_dir(tenant_id: Uuid, member_id: Uuid) -> String {
    format!("profile_photos/{tenant_id}/{member_id}")
}

pub fn group_photo_dir(tenant_id: Uuid, chat_id: Uuid) -> String {
    format!("group_photos/{tenant_id}/{chat_id}")
}

pub fn audio_dir(tenant_id: Uuid, chat_id: Uuid) -> String {
    format!("audios/{tenant_id}/{chat_id}")
}

pub fn image_dir(tenant_id: Uuid, chat_id: Uuid) -> String {
    format!("images/{tenant_id}/{chat_id}")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn storage() -> LocalStorage {
        LocalStorage::new(PathBuf::from("/srv/media"), "http://localhost/static/".into())
    }

    #[test]
    fn traversal_and_absolute_paths_are_rejected() {
        let storage = storage();
        assert!(storage.resolve_relative("../../etc/passwd").is_none());
        assert!(storage.resolve_relative("images/../../etc/passwd").is_none());
        assert!(storage.resolve_relative("/etc/passwd").is_none());
        assert!(storage.resolve_relative("").is_none());
    }

    #[test]
    fn plain_relative_paths_resolve_under_the_media_root() {
        let storage = storage();
        assert_eq!(
            storage.resolve_relative("images/tenant/chat/file.jpg"),
            Some(PathBuf::from("/srv/media/images/tenant/chat/file.jpg"))
        );
    }

    fn jpeg_with_exif_orientation(width: u32, height: u32, orientation: u8) -> Vec<u8> {
        let mut plain = Vec::new();
        DynamicImage::new_rgb8(width, height)
            .write_to(&mut Cursor::new(&mut plain), ImageFormat::Jpeg)
            .expect("encode test jpeg");

        let mut exif = b"Exif\0\0MM\0*\0\0\0\x08\0\x01\x01\x12\0\x03\0\0\0\x01\0".to_vec();
        exif.extend_from_slice(&[orientation, 0, 0, 0, 0, 0, 0]);
        let segment_length = (exif.len() + 2) as u16;

        let mut tagged = plain[..2].to_vec();
        tagged.extend_from_slice(&[0xFF, 0xE1]);
        tagged.extend_from_slice(&segment_length.to_be_bytes());
        tagged.extend_from_slice(&exif);
        tagged.extend_from_slice(&plain[2..]);
        tagged
    }

    #[test]
    fn uploaded_photos_lose_their_exif_but_keep_their_orientation() {
        let tagged = jpeg_with_exif_orientation(4, 2, 6);
        assert!(tagged.windows(4).any(|window| window == b"Exif"));

        let sanitized = sanitize_image(&tagged).expect("sanitize jpeg");

        assert!(!sanitized.bytes.windows(4).any(|window| window == b"Exif"));
        assert_eq!((sanitized.width, sanitized.height), (2, 4));
        assert_eq!(sanitized.content_type, "image/jpeg");
    }

    #[test]
    fn bytes_that_are_not_an_image_are_rejected() {
        assert!(sanitize_image(b"definitely not an image").is_err());
    }

    #[test]
    fn public_base_url_loses_its_trailing_slash() {
        assert_eq!(storage().public_base_url, "http://localhost/static");
    }
}
