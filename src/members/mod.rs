use axum::{
    Router,
    routing::{delete, get, patch},
};

use crate::state::AppState;

mod dto;
mod handlers;

pub fn routes() -> Router<AppState> {
    Router::new()
        .route(
            "/members",
            get(handlers::list_members).post(handlers::create_member),
        )
        .route(
            "/members/{member_id}",
            patch(handlers::update_member).delete(handlers::delete_member),
        )
        .route(
            "/members/{member_id}/photo",
            delete(handlers::delete_member_photo),
        )
}
