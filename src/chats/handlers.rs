use std::collections::HashMap;

use axum::{
    Json,
    extract::{Path, State},
};
use chrono::{DateTime, Utc};
use serde_json::{Value, json};
use sqlx::{MySqlPool, Row};
use uuid::Uuid;

use crate::{
    auth::{AuthUser, assert_chat_participant, require_adult},
    error::{AppError, AppResult},
    realtime::{emit, emit_tx},
    state::AppState,
    utils::row_to_json,
};

use super::dto::{ChatRequest, ReadRequest};

pub(super) async fn list_chats(
    State(state): State<AppState>,
    auth: AuthUser,
) -> AppResult<Json<Value>> {
    let rows = sqlx::query(
        r#"
        SELECT c.*
        FROM chats c
        JOIN chat_participants cp ON cp.chat_id = c.id
        WHERE c.tenant_id = ? AND cp.member_id = ?
        ORDER BY c.updated_at DESC
        "#,
    )
    .bind(auth.tenant_id)
    .bind(auth.member_id)
    .fetch_all(&state.pool)
    .await?;

    let mut chat_ids = Vec::with_capacity(rows.len());
    for row in &rows {
        chat_ids.push(row.try_get::<Uuid, _>("id")?);
    }
    // Two aggregate queries for the whole page instead of two per chat.
    let mut participants = load_participants(&state.pool, &chat_ids).await?;
    let mut read_state = load_read_state(&state.pool, &chat_ids).await?;

    let mut chats = Vec::with_capacity(rows.len());
    for row in rows {
        let chat_id: Uuid = row.try_get("id")?;
        chats.push(merge_chat(
            row,
            participants.remove(&chat_id).unwrap_or_default(),
            read_state.remove(&chat_id).unwrap_or_default(),
        ));
    }
    Ok(Json(Value::Array(chats)))
}

pub(super) async fn get_chat(
    State(state): State<AppState>,
    auth: AuthUser,
    Path(chat_id): Path<Uuid>,
) -> AppResult<Json<Value>> {
    assert_chat_participant(&state.pool, chat_id, auth.member_id, auth.tenant_id).await?;
    let row = sqlx::query("SELECT * FROM chats WHERE id = ? AND tenant_id = ?")
        .bind(chat_id)
        .bind(auth.tenant_id)
        .fetch_one(&state.pool)
        .await?;
    let ids = [chat_id];
    let mut participants = load_participants(&state.pool, &ids).await?;
    let mut read_state = load_read_state(&state.pool, &ids).await?;
    Ok(Json(merge_chat(
        row,
        participants.remove(&chat_id).unwrap_or_default(),
        read_state.remove(&chat_id).unwrap_or_default(),
    )))
}

type ReadStateEntry = (Uuid, Option<DateTime<Utc>>, i32);

fn placeholders(count: usize) -> String {
    vec!["?"; count].join(",")
}

async fn load_participants(
    pool: &MySqlPool,
    chat_ids: &[Uuid],
) -> AppResult<HashMap<Uuid, Vec<Uuid>>> {
    let mut map: HashMap<Uuid, Vec<Uuid>> = HashMap::new();
    if chat_ids.is_empty() {
        return Ok(map);
    }
    let sql = format!(
        "SELECT chat_id, member_id FROM chat_participants WHERE chat_id IN ({})",
        placeholders(chat_ids.len())
    );
    let mut query = sqlx::query(&sql);
    for chat_id in chat_ids {
        query = query.bind(chat_id);
    }
    for row in query.fetch_all(pool).await? {
        let chat_id: Uuid = row.try_get("chat_id")?;
        let member_id: Uuid = row.try_get("member_id")?;
        map.entry(chat_id).or_default().push(member_id);
    }
    Ok(map)
}

async fn load_read_state(
    pool: &MySqlPool,
    chat_ids: &[Uuid],
) -> AppResult<HashMap<Uuid, Vec<ReadStateEntry>>> {
    let mut map: HashMap<Uuid, Vec<ReadStateEntry>> = HashMap::new();
    if chat_ids.is_empty() {
        return Ok(map);
    }
    let sql = format!(
        "SELECT chat_id, member_id, read_up_to, unread_count FROM chat_read_state WHERE chat_id IN ({})",
        placeholders(chat_ids.len())
    );
    let mut query = sqlx::query(&sql);
    for chat_id in chat_ids {
        query = query.bind(chat_id);
    }
    for row in query.fetch_all(pool).await? {
        let chat_id: Uuid = row.try_get("chat_id")?;
        let member_id: Uuid = row.try_get("member_id")?;
        let read_up_to: Option<DateTime<Utc>> = row.try_get("read_up_to")?;
        let unread_count: i32 = row.try_get("unread_count")?;
        map.entry(chat_id)
            .or_default()
            .push((member_id, read_up_to, unread_count));
    }
    Ok(map)
}

fn merge_chat(
    row: sqlx::mysql::MySqlRow,
    participants: Vec<Uuid>,
    read_state: Vec<ReadStateEntry>,
) -> Value {
    let mut value = match row_to_json(row) {
        Value::Object(map) => map,
        _ => serde_json::Map::new(),
    };

    value.insert(
        "participant_ids".into(),
        Value::Array(participants.into_iter().map(|id| json!(id)).collect()),
    );

    let mut read_up_to = serde_json::Map::new();
    let mut unread_by = serde_json::Map::new();
    for (member_id, read_at, unread_count) in read_state {
        if let Some(read_at) = read_at {
            read_up_to.insert(member_id.to_string(), json!(read_at));
        }
        unread_by.insert(member_id.to_string(), json!(unread_count));
    }
    value.insert("read_up_to".into(), Value::Object(read_up_to));
    value.insert("unread_by".into(), Value::Object(unread_by));

    Value::Object(value)
}

/// Rejects unknown members, members of another tenant and deleted members in a single
/// round trip.
async fn assert_participants_belong(
    tx: &mut sqlx::Transaction<'_, sqlx::MySql>,
    tenant_id: Uuid,
    participants: &[Uuid],
) -> AppResult<()> {
    if participants.is_empty() {
        return Err(AppError::BadRequest("participants are required".into()));
    }
    let sql = format!(
        "SELECT COUNT(*) FROM members WHERE tenant_id = ? AND deleted_at IS NULL AND id IN ({})",
        placeholders(participants.len())
    );
    let mut query = sqlx::query_scalar::<_, i64>(&sql).bind(tenant_id);
    for participant in participants {
        query = query.bind(participant);
    }
    if query.fetch_one(&mut **tx).await? as usize != participants.len() {
        return Err(AppError::BadRequest(
            "participant does not belong to tenant".into(),
        ));
    }
    Ok(())
}

async fn find_direct_chat(
    tx: &mut sqlx::Transaction<'_, sqlx::MySql>,
    tenant_id: Uuid,
    first: Uuid,
    second: Uuid,
) -> AppResult<Option<Uuid>> {
    let existing = sqlx::query_scalar::<_, Uuid>(
        r#"
        SELECT c.id
        FROM chats c
        JOIN chat_participants a ON a.chat_id = c.id AND a.member_id = ?
        JOIN chat_participants b ON b.chat_id = c.id AND b.member_id = ?
        WHERE c.tenant_id = ?
          AND c.is_group = false
          AND (SELECT COUNT(*) FROM chat_participants cp WHERE cp.chat_id = c.id) = 2
        LIMIT 1
        "#,
    )
    .bind(first)
    .bind(second)
    .bind(tenant_id)
    .fetch_optional(&mut **tx)
    .await?;
    Ok(existing)
}

pub(super) async fn create_chat(
    State(state): State<AppState>,
    auth: AuthUser,
    Json(req): Json<ChatRequest>,
) -> AppResult<Json<Value>> {
    require_adult(&auth)?;
    let is_group = req.is_group.unwrap_or(true);
    let mut participants = req.participant_ids.unwrap_or_default();
    if !participants.contains(&auth.member_id) {
        participants.push(auth.member_id);
    }
    participants.sort_unstable();
    participants.dedup();

    if is_group && (req.name.as_deref().unwrap_or("").trim().is_empty() || participants.len() < 3) {
        return Err(AppError::BadRequest(
            "group chats require name and at least 3 participants".into(),
        ));
    }
    if !is_group && participants.len() != 2 {
        return Err(AppError::BadRequest(
            "direct chats require exactly 2 participants".into(),
        ));
    }

    let mut tx = state.pool.begin().await?;
    assert_participants_belong(&mut tx, auth.tenant_id, &participants).await?;

    // A direct chat already exists for every pair of members, so creating a second one
    // would split the same conversation into two threads.
    if !is_group
        && let Some(existing) =
            find_direct_chat(&mut tx, auth.tenant_id, participants[0], participants[1]).await?
    {
        return Ok(Json(json!({ "id": existing, "existing": true })));
    }

    let chat_id = Uuid::new_v4();
    sqlx::query("INSERT INTO chats (id, tenant_id, is_group, name) VALUES (?, ?, ?, ?)")
        .bind(chat_id)
        .bind(auth.tenant_id)
        .bind(is_group)
        .bind(req.name.unwrap_or_default())
        .execute(&mut *tx)
        .await?;

    for participant in participants {
        sqlx::query("INSERT INTO chat_participants (chat_id, member_id) VALUES (?, ?)")
            .bind(chat_id)
            .bind(participant)
            .execute(&mut *tx)
            .await?;
        sqlx::query("INSERT INTO chat_read_state (chat_id, member_id) VALUES (?, ?)")
            .bind(chat_id)
            .bind(participant)
            .execute(&mut *tx)
            .await?;
    }
    emit_tx(
        &mut tx,
        &state.hub,
        auth.tenant_id,
        "chat.created",
        Some(chat_id),
        Some(chat_id),
        json!({}),
    )
    .await?;
    tx.commit().await?;
    Ok(Json(json!({ "id": chat_id })))
}

pub(super) async fn update_chat(
    State(state): State<AppState>,
    auth: AuthUser,
    Path(chat_id): Path<Uuid>,
    Json(req): Json<ChatRequest>,
) -> AppResult<Json<Value>> {
    require_adult(&auth)?;
    assert_chat_participant(&state.pool, chat_id, auth.member_id, auth.tenant_id).await?;
    let is_group: bool =
        sqlx::query_scalar("SELECT is_group FROM chats WHERE id = ? AND tenant_id = ?")
            .bind(chat_id)
            .bind(auth.tenant_id)
            .fetch_optional(&state.pool)
            .await?
            .ok_or(AppError::NotFound("chat not found".into()))?;
    let mut tx = state.pool.begin().await?;
    sqlx::query("UPDATE chats SET name = COALESCE(?, name), updated_at = now() WHERE id = ? AND tenant_id = ?")
        .bind(req.name)
        .bind(chat_id)
        .bind(auth.tenant_id)
        .execute(&mut *tx)
        .await?;
    if let Some(mut participants) = req.participant_ids {
        if !is_group {
            return Err(AppError::BadRequest(
                "cannot change participants of a direct chat".into(),
            ));
        }
        if !participants.contains(&auth.member_id) {
            participants.push(auth.member_id);
        }
        participants.sort_unstable();
        participants.dedup();
        if participants.len() < 3 {
            return Err(AppError::BadRequest(
                "group chats require at least 3 participants".into(),
            ));
        }
        assert_participants_belong(&mut tx, auth.tenant_id, &participants).await?;

        let current = sqlx::query("SELECT member_id FROM chat_participants WHERE chat_id = ?")
            .bind(chat_id)
            .fetch_all(&mut *tx)
            .await?;
        let mut current_ids = Vec::with_capacity(current.len());
        for row in current {
            current_ids.push(row.try_get::<Uuid, _>("member_id")?);
        }

        // Only touch the rows that actually changed, so read_up_to and unread_count
        // of the members that stayed are preserved.
        for removed in current_ids.iter().filter(|id| !participants.contains(id)) {
            sqlx::query("DELETE FROM chat_participants WHERE chat_id = ? AND member_id = ?")
                .bind(chat_id)
                .bind(removed)
                .execute(&mut *tx)
                .await?;
            sqlx::query("DELETE FROM chat_read_state WHERE chat_id = ? AND member_id = ?")
                .bind(chat_id)
                .bind(removed)
                .execute(&mut *tx)
                .await?;
        }
        for added in participants.iter().filter(|id| !current_ids.contains(id)) {
            sqlx::query("INSERT INTO chat_participants (chat_id, member_id) VALUES (?, ?)")
                .bind(chat_id)
                .bind(added)
                .execute(&mut *tx)
                .await?;
            sqlx::query("INSERT INTO chat_read_state (chat_id, member_id) VALUES (?, ?)")
                .bind(chat_id)
                .bind(added)
                .execute(&mut *tx)
                .await?;
        }
    }
    emit_tx(
        &mut tx,
        &state.hub,
        auth.tenant_id,
        "chat.updated",
        Some(chat_id),
        Some(chat_id),
        json!({}),
    )
    .await?;
    tx.commit().await?;
    Ok(Json(json!({ "ok": true })))
}

pub(super) async fn delete_chat(
    State(state): State<AppState>,
    auth: AuthUser,
    Path(chat_id): Path<Uuid>,
) -> AppResult<Json<Value>> {
    require_adult(&auth)?;
    assert_chat_participant(&state.pool, chat_id, auth.member_id, auth.tenant_id).await?;
    let mut tx = state.pool.begin().await?;
    sqlx::query("DELETE FROM chats WHERE id = ? AND tenant_id = ?")
        .bind(chat_id)
        .bind(auth.tenant_id)
        .execute(&mut *tx)
        .await?;
    emit_tx(
        &mut tx,
        &state.hub,
        auth.tenant_id,
        "chat.deleted",
        None,
        Some(chat_id),
        json!({"chat_id": chat_id}),
    )
    .await?;
    tx.commit().await?;
    Ok(Json(json!({ "ok": true })))
}

pub(super) async fn delete_chat_photo(
    State(state): State<AppState>,
    auth: AuthUser,
    Path(chat_id): Path<Uuid>,
) -> AppResult<Json<Value>> {
    require_adult(&auth)?;
    assert_chat_participant(&state.pool, chat_id, auth.member_id, auth.tenant_id).await?;
    let previous: Option<String> =
        sqlx::query_scalar("SELECT photo_path FROM chats WHERE id = ? AND tenant_id = ?")
            .bind(chat_id)
            .bind(auth.tenant_id)
            .fetch_optional(&state.pool)
            .await?
            .flatten();
    sqlx::query("UPDATE chats SET photo_url = NULL, photo_path = NULL, updated_at = now() WHERE id = ? AND tenant_id = ?")
        .bind(chat_id)
        .bind(auth.tenant_id)
        .execute(&state.pool)
        .await?;
    if let Some(path) = previous {
        state.storage.delete_relative(&path).await;
    }
    emit(
        &state.pool,
        &state.hub,
        auth.tenant_id,
        "chat.updated",
        Some(chat_id),
        Some(chat_id),
        json!({"photo": false}),
    )
    .await?;
    Ok(Json(json!({ "ok": true })))
}

pub(super) async fn mark_chat_read(
    State(state): State<AppState>,
    auth: AuthUser,
    Path(chat_id): Path<Uuid>,
    Json(req): Json<ReadRequest>,
) -> AppResult<Json<Value>> {
    assert_chat_participant(&state.pool, chat_id, auth.member_id, auth.tenant_id).await?;
    // A plain UPDATE silently dropped the read receipt whenever the row was missing, which
    // is the case for any participant added before chat_read_state was seeded.
    sqlx::query(
        r#"
        INSERT INTO chat_read_state (chat_id, member_id, read_up_to, unread_count)
        VALUES (?, ?, COALESCE(?, now()), 0)
        ON DUPLICATE KEY UPDATE read_up_to = VALUES(read_up_to), unread_count = 0
        "#,
    )
    .bind(chat_id)
    .bind(auth.member_id)
    .bind(req.read_up_to)
    .execute(&state.pool)
    .await?;
    Ok(Json(json!({ "ok": true })))
}
