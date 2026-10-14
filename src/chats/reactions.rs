use axum::{
    Json,
    extract::{Path, Query, State},
};
use serde_json::{Value, json};
use uuid::Uuid;

use crate::{
    auth::{AuthUser, assert_chat_participant},
    error::{AppError, AppResult},
    realtime::emit,
    state::AppState,
};

use super::dto::{MessageQuery, ReactionRequest, ReactionResponse};

const MAX_EMOJI_CHARS: usize = 16;

pub(super) async fn list_reactions(
    State(state): State<AppState>,
    auth: AuthUser,
    Path(chat_id): Path<Uuid>,
    Query(query): Query<MessageQuery>,
) -> AppResult<Json<Vec<ReactionResponse>>> {
    assert_chat_participant(&state.pool, chat_id, auth.member_id, auth.tenant_id).await?;
    let reactions = match query.after {
        Some(after) => {
            sqlx::query_as::<_, ReactionResponse>(
                "SELECT message_id, member_id, chat_id, emoji, updated_at FROM message_reactions WHERE chat_id = ? AND updated_at > ? ORDER BY updated_at ASC LIMIT ?",
            )
            .bind(chat_id)
            .bind(after)
            .bind(query.limit.unwrap_or(500).clamp(1, 1000))
            .fetch_all(&state.pool)
            .await?
        }
        None => {
            sqlx::query_as::<_, ReactionResponse>(
                "SELECT message_id, member_id, chat_id, emoji, updated_at FROM message_reactions WHERE chat_id = ? ORDER BY updated_at ASC",
            )
                .bind(chat_id)
                .fetch_all(&state.pool)
                .await?
        }
    };
    Ok(Json(reactions))
}

pub(super) async fn upsert_reaction(
    State(state): State<AppState>,
    auth: AuthUser,
    Path((chat_id, message_id)): Path<(Uuid, Uuid)>,
    Json(req): Json<ReactionRequest>,
) -> AppResult<Json<Value>> {
    assert_chat_participant(&state.pool, chat_id, auth.member_id, auth.tenant_id).await?;
    let emoji = req.emoji.trim();
    if emoji.is_empty() || emoji.chars().count() > MAX_EMOJI_CHARS {
        return Err(AppError::BadRequest("invalid emoji".into()));
    }
    let message_in_chat = sqlx::query(
        "SELECT 1 FROM messages WHERE id = ? AND chat_id = ? AND tenant_id = ? AND is_deleted = false",
    )
    .bind(message_id)
    .bind(chat_id)
    .bind(auth.tenant_id)
    .fetch_optional(&state.pool)
    .await?
    .is_some();
    if !message_in_chat {
        return Err(AppError::NotFound("message not found".into()));
    }
    sqlx::query(
        r#"
        INSERT INTO message_reactions (message_id, member_id, chat_id, emoji)
        VALUES (?, ?, ?, ?)
        ON DUPLICATE KEY UPDATE emoji = VALUES(emoji), updated_at = now()
        "#,
    )
    .bind(message_id)
    .bind(auth.member_id)
    .bind(chat_id)
    .bind(emoji)
    .execute(&state.pool)
    .await?;
    emit(
        &state.pool,
        &state.hub,
        auth.tenant_id,
        "reaction.updated",
        Some(chat_id),
        Some(message_id),
        json!({"member_id": auth.member_id, "emoji": emoji}),
    )
    .await?;
    Ok(Json(json!({ "ok": true })))
}

pub(super) async fn delete_reaction(
    State(state): State<AppState>,
    auth: AuthUser,
    Path((chat_id, message_id)): Path<(Uuid, Uuid)>,
) -> AppResult<Json<Value>> {
    assert_chat_participant(&state.pool, chat_id, auth.member_id, auth.tenant_id).await?;
    sqlx::query("DELETE FROM message_reactions WHERE message_id = ? AND member_id = ?")
        .bind(message_id)
        .bind(auth.member_id)
        .execute(&state.pool)
        .await?;
    emit(
        &state.pool,
        &state.hub,
        auth.tenant_id,
        "reaction.deleted",
        Some(chat_id),
        Some(message_id),
        json!({"member_id": auth.member_id}),
    )
    .await?;
    Ok(Json(json!({ "ok": true })))
}
