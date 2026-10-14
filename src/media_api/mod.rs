use axum::{
    Router,
    extract::DefaultBodyLimit,
    routing::{get, post},
};

use crate::state::AppState;

mod handlers;
mod uploads;

/// Must stay above the largest MediaKind limit, otherwise the size check in
/// LocalStorage::save is unreachable and oversized uploads fail as malformed multipart.
const UPLOAD_BODY_LIMIT: usize = 24 * 1024 * 1024;

pub fn routes() -> Router<AppState> {
    Router::new()
        .route(
            "/media/profile-photo",
            post(handlers::upload_profile_photo).delete(handlers::delete_profile_photo),
        )
        .route(
            "/chats/{chat_id}/photo/upload",
            post(handlers::upload_chat_photo),
        )
        .route(
            "/chats/{chat_id}/messages/{message_id}/audio",
            post(handlers::upload_message_audio),
        )
        .route(
            "/chats/{chat_id}/messages/{message_id}/image",
            post(handlers::upload_message_image),
        )
        .layer(DefaultBodyLimit::max(UPLOAD_BODY_LIMIT))
        .route("/static/{*storage_path}", get(handlers::serve_media))
}
