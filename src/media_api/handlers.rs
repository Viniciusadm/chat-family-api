use axum::{
    Json,
    body::Body,
    extract::{Multipart, Path, State},
    http::header,
    response::{IntoResponse, Response},
};
use serde_json::{Value, json};
use sqlx::Row;
use tokio_util::io::ReaderStream;
use uuid::Uuid;

use crate::{
    auth::{AuthUser, assert_chat_participant, require_adult},
    error::{AppError, AppResult},
    media::{MediaKind, audio_dir, group_photo_dir, image_dir, profile_photo_dir},
    realtime::{PendingEvents, emit, emit_tx},
    state::AppState,
};

use super::uploads::{assert_owner_of_kind, cleanup_old, insert_media_file};

/// Serves an uploaded file only to members of its tenant, and — for chat media — only to
/// participants of that chat. Replaces the unauthenticated static file server.
pub(super) async fn serve_media(
    State(state): State<AppState>,
    auth: AuthUser,
    Path(storage_path): Path<String>,
) -> AppResult<Response> {
    let absolute = state
        .storage
        .resolve_relative(&storage_path)
        .ok_or(AppError::NotFound("media not found".into()))?;

    let row = sqlx::query(
        "SELECT tenant_id, chat_id, content_type FROM media_files WHERE storage_path = ? AND deleted_at IS NULL",
    )
    .bind(&storage_path)
    .fetch_optional(&state.pool)
    .await?
    .ok_or(AppError::NotFound("media not found".into()))?;

    if row.try_get::<Uuid, _>("tenant_id")? != auth.tenant_id {
        return Err(AppError::NotFound("media not found".into()));
    }
    if let Some(chat_id) = row.try_get::<Option<Uuid>, _>("chat_id")? {
        assert_chat_participant(&state.pool, chat_id, auth.member_id, auth.tenant_id).await?;
    }

    // Audio files reach 20 MB, so the response is streamed instead of being buffered in
    // memory for every concurrent download.
    let file = tokio::fs::File::open(&absolute)
        .await
        .map_err(|_| AppError::NotFound("media not found".into()))?;
    let size_bytes = file
        .metadata()
        .await
        .map_err(|err| AppError::Internal(err.into()))?
        .len();
    let content_type: String = row.try_get("content_type")?;
    let body = Body::from_stream(ReaderStream::new(file));

    Ok((
        [
            (header::CONTENT_TYPE, content_type),
            (header::CONTENT_LENGTH, size_bytes.to_string()),
            (header::ACCEPT_RANGES, "bytes".to_string()),
            (header::CONTENT_DISPOSITION, "attachment".to_string()),
            (
                header::CACHE_CONTROL,
                "private, max-age=31536000".to_string(),
            ),
        ],
        body,
    )
        .into_response())
}

pub(super) async fn upload_profile_photo(
    State(state): State<AppState>,
    auth: AuthUser,
    mut multipart: Multipart,
) -> AppResult<Json<Value>> {
    let field = multipart
        .next_field()
        .await
        .map_err(|err| AppError::BadRequest(format!("invalid multipart: {err}")))?
        .ok_or(AppError::BadRequest("missing file field".into()))?;

    let dir = profile_photo_dir(auth.tenant_id, auth.member_id);
    let stem = Uuid::new_v4().to_string();
    let saved = state
        .storage
        .save(MediaKind::ProfilePhoto, &dir, &stem, field)
        .await?;

    let mut tx = state.pool.begin().await?;
    let mut events = PendingEvents::default();
    let previous_user: Option<String> =
        sqlx::query_scalar("SELECT photo_path FROM users WHERE id = ?")
            .bind(auth.user_id)
            .fetch_optional(&mut *tx)
            .await?
            .flatten();
    let previous_member: Option<String> =
        sqlx::query_scalar("SELECT photo_path FROM members WHERE id = ?")
            .bind(auth.member_id)
            .fetch_optional(&mut *tx)
            .await?
            .flatten();
    sqlx::query("UPDATE users SET photo_url = ?, photo_path = ? WHERE id = ?")
        .bind(&saved.public_url)
        .bind(&saved.storage_path)
        .bind(auth.user_id)
        .execute(&mut *tx)
        .await?;
    sqlx::query("UPDATE members SET photo_url = ?, photo_path = ? WHERE id = ? AND tenant_id = ?")
        .bind(&saved.public_url)
        .bind(&saved.storage_path)
        .bind(auth.member_id)
        .bind(auth.tenant_id)
        .execute(&mut *tx)
        .await?;
    insert_media_file(
        &mut tx,
        auth.tenant_id,
        auth.member_id,
        None,
        None,
        MediaKind::ProfilePhoto,
        &saved,
    )
    .await?;
    emit_tx(
        &mut tx,
        &mut events,
        auth.tenant_id,
        "member.updated",
        None,
        Some(auth.member_id),
        json!({"photo": true}),
    )
    .await?;
    tx.commit().await?;
    events.publish(&state.hub);
    cleanup_old(&state.storage, previous_user.as_deref()).await;
    cleanup_old(&state.storage, previous_member.as_deref()).await;

    Ok(Json(json!({
        "url": saved.public_url,
        "path": saved.storage_path,
    })))
}

pub(super) async fn delete_profile_photo(
    State(state): State<AppState>,
    auth: AuthUser,
) -> AppResult<Json<Value>> {
    let previous_user: Option<String> =
        sqlx::query_scalar("SELECT photo_path FROM users WHERE id = ?")
            .bind(auth.user_id)
            .fetch_optional(&state.pool)
            .await?
            .flatten();
    let previous_member: Option<String> =
        sqlx::query_scalar("SELECT photo_path FROM members WHERE id = ?")
            .bind(auth.member_id)
            .fetch_optional(&state.pool)
            .await?
            .flatten();
    sqlx::query("UPDATE users SET photo_url = NULL, photo_path = NULL WHERE id = ?")
        .bind(auth.user_id)
        .execute(&state.pool)
        .await?;
    sqlx::query("UPDATE members SET photo_url = NULL, photo_path = NULL WHERE id = ?")
        .bind(auth.member_id)
        .execute(&state.pool)
        .await?;
    cleanup_old(&state.storage, previous_user.as_deref()).await;
    cleanup_old(&state.storage, previous_member.as_deref()).await;
    emit(
        &state.pool,
        &state.hub,
        auth.tenant_id,
        "member.updated",
        None,
        Some(auth.member_id),
        json!({"photo": false}),
    )
    .await?;
    Ok(Json(json!({ "ok": true })))
}

pub(super) async fn upload_chat_photo(
    State(state): State<AppState>,
    auth: AuthUser,
    Path(chat_id): Path<Uuid>,
    mut multipart: Multipart,
) -> AppResult<Json<Value>> {
    require_adult(&auth)?;
    assert_chat_participant(&state.pool, chat_id, auth.member_id, auth.tenant_id).await?;
    let field = multipart
        .next_field()
        .await
        .map_err(|err| AppError::BadRequest(format!("invalid multipart: {err}")))?
        .ok_or(AppError::BadRequest("missing file field".into()))?;
    let dir = group_photo_dir(auth.tenant_id, chat_id);
    let stem = Uuid::new_v4().to_string();
    let saved = state
        .storage
        .save(MediaKind::GroupPhoto, &dir, &stem, field)
        .await?;

    let mut tx = state.pool.begin().await?;
    let mut events = PendingEvents::default();
    let previous: Option<String> =
        sqlx::query_scalar("SELECT photo_path FROM chats WHERE id = ? AND tenant_id = ?")
            .bind(chat_id)
            .bind(auth.tenant_id)
            .fetch_optional(&mut *tx)
            .await?
            .flatten();
    sqlx::query("UPDATE chats SET photo_url = ?, photo_path = ?, updated_at = now() WHERE id = ?")
        .bind(&saved.public_url)
        .bind(&saved.storage_path)
        .bind(chat_id)
        .execute(&mut *tx)
        .await?;
    insert_media_file(
        &mut tx,
        auth.tenant_id,
        auth.member_id,
        Some(chat_id),
        None,
        MediaKind::GroupPhoto,
        &saved,
    )
    .await?;
    emit_tx(
        &mut tx,
        &mut events,
        auth.tenant_id,
        "chat.updated",
        Some(chat_id),
        Some(chat_id),
        json!({"photo": true}),
    )
    .await?;
    tx.commit().await?;
    events.publish(&state.hub);
    cleanup_old(&state.storage, previous.as_deref()).await;

    Ok(Json(json!({
        "url": saved.public_url,
        "path": saved.storage_path,
    })))
}

pub(super) async fn upload_message_audio(
    State(state): State<AppState>,
    auth: AuthUser,
    Path((chat_id, message_id)): Path<(Uuid, Uuid)>,
    mut multipart: Multipart,
) -> AppResult<Json<Value>> {
    assert_chat_participant(&state.pool, chat_id, auth.member_id, auth.tenant_id).await?;
    assert_owner_of_kind(&state.pool, chat_id, message_id, auth.member_id, "audio").await?;
    let field = multipart
        .next_field()
        .await
        .map_err(|err| AppError::BadRequest(format!("invalid multipart: {err}")))?
        .ok_or(AppError::BadRequest("missing file field".into()))?;
    let dir = audio_dir(auth.tenant_id, chat_id);
    let saved = state
        .storage
        .save(
            MediaKind::MessageAudio,
            &dir,
            &message_id.to_string(),
            field,
        )
        .await?;
    tracing::debug!(
        message_id = %message_id,
        storage_path = %saved.storage_path,
        public_url = %saved.public_url,
        "saved message audio"
    );

    let mut tx = state.pool.begin().await?;
    let mut events = PendingEvents::default();
    sqlx::query(
        "UPDATE messages SET audio_url = ? WHERE id = ? AND chat_id = ? AND sender_member_id = ?",
    )
    .bind(&saved.public_url)
    .bind(message_id)
    .bind(chat_id)
    .bind(auth.member_id)
    .execute(&mut *tx)
    .await?;
    insert_media_file(
        &mut tx,
        auth.tenant_id,
        auth.member_id,
        Some(chat_id),
        Some(message_id),
        MediaKind::MessageAudio,
        &saved,
    )
    .await?;
    emit_tx(
        &mut tx,
        &mut events,
        auth.tenant_id,
        "message.updated",
        Some(chat_id),
        Some(message_id),
        json!({"audio_url": saved.public_url}),
    )
    .await?;
    tx.commit().await?;
    events.publish(&state.hub);
    Ok(Json(json!({
        "url": saved.public_url,
        "path": saved.storage_path,
    })))
}

pub(super) async fn upload_message_image(
    State(state): State<AppState>,
    auth: AuthUser,
    Path((chat_id, message_id)): Path<(Uuid, Uuid)>,
    mut multipart: Multipart,
) -> AppResult<Json<Value>> {
    assert_chat_participant(&state.pool, chat_id, auth.member_id, auth.tenant_id).await?;
    assert_owner_of_kind(&state.pool, chat_id, message_id, auth.member_id, "image").await?;
    let field = multipart
        .next_field()
        .await
        .map_err(|err| AppError::BadRequest(format!("invalid multipart: {err}")))?
        .ok_or(AppError::BadRequest("missing file field".into()))?;
    let dir = image_dir(auth.tenant_id, chat_id);
    let saved = state
        .storage
        .save(
            MediaKind::MessageImage,
            &dir,
            &message_id.to_string(),
            field,
        )
        .await?;

    let mut tx = state.pool.begin().await?;
    let mut events = PendingEvents::default();
    sqlx::query(
        r#"
        UPDATE messages
        SET image_url = ?, image_width = ?, image_height = ?, image_file_size = ?
        WHERE id = ? AND chat_id = ? AND sender_member_id = ?
        "#,
    )
    .bind(&saved.public_url)
    .bind(saved.width)
    .bind(saved.height)
    .bind(saved.size_bytes)
    .bind(message_id)
    .bind(chat_id)
    .bind(auth.member_id)
    .execute(&mut *tx)
    .await?;
    insert_media_file(
        &mut tx,
        auth.tenant_id,
        auth.member_id,
        Some(chat_id),
        Some(message_id),
        MediaKind::MessageImage,
        &saved,
    )
    .await?;
    emit_tx(
        &mut tx,
        &mut events,
        auth.tenant_id,
        "message.updated",
        Some(chat_id),
        Some(message_id),
        json!({"image_url": saved.public_url}),
    )
    .await?;
    tx.commit().await?;
    events.publish(&state.hub);
    Ok(Json(json!({
        "url": saved.public_url,
        "path": saved.storage_path,
        "width": saved.width,
        "height": saved.height,
        "size_bytes": saved.size_bytes,
    })))
}
