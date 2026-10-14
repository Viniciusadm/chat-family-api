use std::sync::Arc;

use axum::{
    Json,
    extract::{Path, Query, State},
};
use serde_json::{Value, json};
use uuid::Uuid;

use crate::{
    auth::{AuthUser, assert_chat_participant},
    error::{AppError, AppResult},
    realtime::{PendingEvents, emit, emit_tx},
    state::AppState,
    utils::is_unique_violation,
};

use super::dto::{MESSAGE_COLUMNS, MessageQuery, MessageRequest, MessageResponse};

pub(super) async fn list_messages(
    State(state): State<AppState>,
    auth: AuthUser,
    Path(chat_id): Path<Uuid>,
    Query(query): Query<MessageQuery>,
) -> AppResult<Json<Vec<MessageResponse>>> {
    assert_chat_participant(&state.pool, chat_id, auth.member_id, auth.tenant_id).await?;
    let limit = query.limit.unwrap_or(100).clamp(1, 500);
    // `after` pages forward through history; `changed_after` replays every write, so a
    // client also picks up edits and soft deletes of messages older than its history
    // cursor. The two cursors need different orderings, hence the two queries.
    let messages = match (query.changed_after, query.before) {
        (Some(changed_after), _) => {
            let sql = format!(
                r#"
                SELECT {MESSAGE_COLUMNS}
                FROM messages
                WHERE chat_id = ? AND tenant_id = ? AND updated_at > ?
                ORDER BY updated_at ASC
                LIMIT ?
                "#
            );
            sqlx::query_as::<_, MessageResponse>(&sql)
                .bind(chat_id)
                .bind(auth.tenant_id)
                .bind(changed_after)
                .bind(limit)
                .fetch_all(&state.pool)
                .await?
        }
        (None, Some(before)) => {
            let sql = format!(
                r#"
                SELECT * FROM (
                    SELECT {MESSAGE_COLUMNS}
                    FROM messages
                    WHERE chat_id = ? AND tenant_id = ? AND created_at < ?
                    ORDER BY created_at DESC
                    LIMIT ?
                ) AS page
                ORDER BY created_at ASC
                "#
            );
            sqlx::query_as::<_, MessageResponse>(&sql)
                .bind(chat_id)
                .bind(auth.tenant_id)
                .bind(before)
                .bind(limit)
                .fetch_all(&state.pool)
                .await?
        }
        (None, None) => {
            let sql = format!(
                r#"
                SELECT {MESSAGE_COLUMNS}
                FROM messages
                WHERE chat_id = ? AND tenant_id = ? AND (? IS NULL OR created_at > ?)
                ORDER BY created_at ASC
                LIMIT ?
                "#
            );
            sqlx::query_as::<_, MessageResponse>(&sql)
                .bind(chat_id)
                .bind(auth.tenant_id)
                .bind(query.after)
                .bind(query.after)
                .bind(limit)
                .fetch_all(&state.pool)
                .await?
        }
    };
    Ok(Json(messages))
}

pub(super) async fn create_message(
    State(state): State<AppState>,
    auth: AuthUser,
    Path(chat_id): Path<Uuid>,
    Json(req): Json<MessageRequest>,
) -> AppResult<Json<Value>> {
    assert_chat_participant(&state.pool, chat_id, auth.member_id, auth.tenant_id).await?;
    validate_message_payload(&req)?;
    let mut tx = state.pool.begin().await?;
    let mut events = PendingEvents::default();
    // A plain INSERT keeps a client retry idempotent (duplicate key => already stored)
    // while letting real violations surface, unlike the previous INSERT IGNORE which
    // returned 200 for messages that were never persisted.
    let insert = sqlx::query(
        r#"
        INSERT INTO messages (
            id, chat_id, tenant_id, sender_member_id, type,
            ciphertext, iv, enc_version, audio_url, audio_duration,
            image_url, thumbnail_url, image_width, image_height, image_file_size,
            reply_to_message_id, reply_to_sender_id, reply_to_sender_name, reply_to_type
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        "#,
    )
    .bind(req.id)
    .bind(chat_id)
    .bind(auth.tenant_id)
    .bind(auth.member_id)
    .bind(&req.kind)
    .bind(&req.ciphertext)
    .bind(&req.iv)
    .bind(req.enc_version)
    .bind(&req.audio_url)
    .bind(req.audio_duration)
    .bind(&req.image_url)
    .bind(&req.thumbnail_url)
    .bind(req.image_width)
    .bind(req.image_height)
    .bind(req.image_file_size)
    .bind(req.reply_to_message_id)
    .bind(req.reply_to_sender_id)
    .bind(&req.reply_to_sender_name)
    .bind(&req.reply_to_type)
    .execute(&mut *tx)
    .await;

    // A retry of an already stored message must not bump the preview or the unread
    // counters again.
    if let Err(error) = insert {
        if is_unique_violation(&error) {
            tx.rollback().await?;
            return Ok(Json(json!({ "id": req.id, "duplicate": true })));
        }
        return Err(error.into());
    }

    sqlx::query(
        "UPDATE chats SET last_message_ciphertext = ?, last_message_iv = ?, last_message_type = ?, last_message_at = now(), updated_at = now() WHERE id = ?",
    )
    .bind(&req.ciphertext)
    .bind(&req.iv)
    .bind(&req.kind)
    .bind(chat_id)
    .execute(&mut *tx)
    .await?;

    sqlx::query(
        "UPDATE chat_read_state SET unread_count = unread_count + 1 WHERE chat_id = ? AND member_id <> ?",
    )
    .bind(chat_id)
    .bind(auth.member_id)
    .execute(&mut *tx)
    .await?;

    emit_tx(
        &mut tx,
        &mut events,
        auth.tenant_id,
        "message.created",
        Some(chat_id),
        Some(req.id),
        json!({"sender_member_id": auth.member_id, "type": req.kind}),
    )
    .await?;
    tx.commit().await?;
    events.publish(&state.hub);

    let push = Arc::clone(&state.push);
    let pool = state.pool.clone();
    let kind = req.kind.clone();
    let ciphertext = req.ciphertext.clone();
    let iv = req.iv.clone();
    let sender_member_id = auth.member_id;
    let tenant_id = auth.tenant_id;
    let message_id = req.id;
    tokio::spawn(async move {
        push.notify_message_created(
            pool,
            tenant_id,
            chat_id,
            message_id,
            sender_member_id,
            kind,
            ciphertext,
            iv,
        )
        .await;
    });

    Ok(Json(json!({ "id": req.id })))
}

fn validate_message_payload(req: &MessageRequest) -> AppResult<()> {
    match req.kind.as_str() {
        "text" if req.ciphertext.is_some() && req.iv.is_some() && req.enc_version.is_some() => {
            Ok(())
        }
        "audio" => Ok(()),
        "image"
            if req.image_url.is_some()
                || (req.image_width.is_none()
                    && req.image_height.is_none()
                    && req.image_file_size.is_none()) =>
        {
            Ok(())
        }
        "text" | "image" => Err(AppError::BadRequest("invalid message payload".into())),
        _ => Err(AppError::BadRequest("invalid message type".into())),
    }
}

pub(super) async fn update_message(
    State(state): State<AppState>,
    auth: AuthUser,
    Path((chat_id, message_id)): Path<(Uuid, Uuid)>,
    Json(req): Json<MessageRequest>,
) -> AppResult<Json<Value>> {
    assert_chat_participant(&state.pool, chat_id, auth.member_id, auth.tenant_id).await?;
    validate_message_payload(&req)?;
    let affected = sqlx::query(
        r#"
        UPDATE messages
        SET ciphertext = ?, iv = ?, enc_version = ?, edited_at = now()
        WHERE id = ? AND chat_id = ? AND tenant_id = ? AND sender_member_id = ?
          AND created_at >= now() - INTERVAL 1 HOUR AND is_deleted = false AND type = 'text'
        "#,
    )
    .bind(req.ciphertext)
    .bind(req.iv)
    .bind(req.enc_version)
    .bind(message_id)
    .bind(chat_id)
    .bind(auth.tenant_id)
    .bind(auth.member_id)
    .execute(&state.pool)
    .await?
    .rows_affected();
    if affected == 0 {
        return Err(AppError::Forbidden);
    }
    emit(
        &state.pool,
        &state.hub,
        auth.tenant_id,
        "message.updated",
        Some(chat_id),
        Some(message_id),
        json!({}),
    )
    .await?;
    Ok(Json(json!({ "ok": true })))
}

pub(super) async fn delete_message(
    State(state): State<AppState>,
    auth: AuthUser,
    Path((chat_id, message_id)): Path<(Uuid, Uuid)>,
) -> AppResult<Json<Value>> {
    assert_chat_participant(&state.pool, chat_id, auth.member_id, auth.tenant_id).await?;
    let affected = sqlx::query(
        r#"
        UPDATE messages
        SET is_deleted = true, deleted_at = now(), ciphertext = NULL, iv = NULL
        WHERE id = ? AND chat_id = ? AND tenant_id = ? AND sender_member_id = ?
          AND created_at >= now() - INTERVAL 1 HOUR
        "#,
    )
    .bind(message_id)
    .bind(chat_id)
    .bind(auth.tenant_id)
    .bind(auth.member_id)
    .execute(&state.pool)
    .await?
    .rows_affected();
    if affected == 0 {
        return Err(AppError::Forbidden);
    }
    emit(
        &state.pool,
        &state.hub,
        auth.tenant_id,
        "message.deleted",
        Some(chat_id),
        Some(message_id),
        json!({}),
    )
    .await?;
    Ok(Json(json!({ "ok": true })))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(kind: &str) -> MessageRequest {
        MessageRequest {
            id: Uuid::new_v4(),
            kind: kind.to_string(),
            ciphertext: None,
            iv: None,
            enc_version: None,
            audio_url: None,
            audio_duration: None,
            image_url: None,
            thumbnail_url: None,
            image_width: None,
            image_height: None,
            image_file_size: None,
            reply_to_message_id: None,
            reply_to_sender_id: None,
            reply_to_sender_name: None,
            reply_to_type: None,
        }
    }

    #[test]
    fn text_requires_the_full_encrypted_payload() {
        let mut req = request("text");
        assert!(validate_message_payload(&req).is_err());
        req.ciphertext = Some("ct".into());
        req.iv = Some("iv".into());
        assert!(validate_message_payload(&req).is_err());
        req.enc_version = Some(1);
        assert!(validate_message_payload(&req).is_ok());
    }

    #[test]
    fn image_metadata_requires_a_url() {
        let mut req = request("image");
        assert!(validate_message_payload(&req).is_ok());
        req.image_width = Some(10);
        assert!(validate_message_payload(&req).is_err());
        req.image_url = Some("http://example.com/a.jpg".into());
        assert!(validate_message_payload(&req).is_ok());
    }

    #[test]
    fn audio_is_accepted_before_its_upload_and_unknown_types_are_not() {
        assert!(validate_message_payload(&request("audio")).is_ok());
        assert!(validate_message_payload(&request("video")).is_err());
    }
}
