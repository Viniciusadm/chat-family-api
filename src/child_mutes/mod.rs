use axum::{
    Router,
    routing::{get, put},
};

use crate::state::AppState;

mod dto;
mod handlers;

pub use handlers::assert_child_can_send;

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/child-mutes", get(handlers::list_child_mutes))
        .route(
            "/members/{member_id}/mute",
            put(handlers::mute_child).delete(handlers::unmute_child),
        )
}
