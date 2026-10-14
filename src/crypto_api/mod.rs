use axum::{
    Router,
    routing::{get, post, put},
};

use crate::state::AppState;

mod dto;
mod handlers;

pub(crate) use dto::KeyRecipientDevice;
pub(crate) use handlers::revoke_chat_key_access;

pub fn routes() -> Router<AppState> {
    Router::new()
        .route(
            "/crypto/password-settings",
            get(handlers::get_password_settings)
                .put(handlers::put_password_settings)
                .delete(handlers::delete_password_settings),
        )
        .route(
            "/crypto/key-backups",
            get(handlers::list_key_backups).delete(handlers::delete_key_backups),
        )
        .route(
            "/crypto/key-backups/{chat_id}",
            put(handlers::put_key_backup),
        )
        .route(
            "/devices/{device_id}/key-shares",
            get(handlers::list_key_shares),
        )
        .route(
            "/devices/{device_id}/key-shares/{chat_id}",
            put(handlers::put_key_share),
        )
        .route(
            "/chats/{chat_id}/key-recipients",
            get(handlers::list_chat_key_recipients),
        )
        .route(
            "/chats/{chat_id}/key-claim",
            post(handlers::claim_chat_key),
        )
        .route(
            "/chats/{chat_id}/devices/{device_id}/key-shares",
            put(handlers::put_chat_key_share),
        )
}
