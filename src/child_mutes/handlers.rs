use axum::{
    Json,
    extract::{Path, Query, State},
};
use serde_json::{Value, json};
use sqlx::MySqlPool;
use uuid::Uuid;

use crate::{
    auth::{AuthUser, Role, require_adult},
    error::{AppError, AppResult},
    realtime::{PendingEvents, emit_tx},
    state::AppState,
};

use super::dto::{ChildMuteResponse, MuteRequest, MuteScope, UnmuteQuery};

const CHILD_MUTE_EVENT: &str = "child_mute.updated";

pub(super) async fn list_child_mutes(
    State(state): State<AppState>,
    auth: AuthUser,
) -> AppResult<Json<Vec<ChildMuteResponse>>> {
    let mutes = sqlx::query_as::<_, ChildMuteResponse>(
        r#"
        SELECT child_member_id, muted_by_member_id, scope, expires_at
        FROM child_mutes
        WHERE tenant_id = ? AND expires_at > now(6) AND (? OR child_member_id = ?)
        ORDER BY expires_at
        "#,
    )
    .bind(auth.tenant_id)
    .bind(auth.role == Role::Adult)
    .bind(auth.member_id)
    .fetch_all(&state.pool)
    .await?;
    Ok(Json(mutes))
}

pub(super) async fn mute_child(
    State(state): State<AppState>,
    auth: AuthUser,
    Path(child_id): Path<Uuid>,
    Json(req): Json<MuteRequest>,
) -> AppResult<Json<ChildMuteResponse>> {
    require_adult(&auth)?;
    let mut tx = state.pool.begin().await?;
    let mut events = PendingEvents::default();
    let role: String = sqlx::query_scalar(
        "SELECT role FROM members WHERE id = ? AND tenant_id = ? AND deleted_at IS NULL",
    )
    .bind(child_id)
    .bind(auth.tenant_id)
    .fetch_optional(&mut *tx)
    .await?
    .ok_or(AppError::NotFound("member not found".into()))?;
    if role != Role::Child.as_db() {
        return Err(AppError::BadRequest("only children can be muted".into()));
    }

    if req.scope == MuteScope::All {
        sqlx::query(
            "DELETE FROM child_mutes WHERE child_member_id = ? AND tenant_id = ? AND scope = 'all'",
        )
        .bind(child_id)
        .bind(auth.tenant_id)
        .execute(&mut *tx)
        .await?;
    }
    sqlx::query(
        r#"
        INSERT INTO child_mutes (tenant_id, child_member_id, muted_by_member_id, scope, expires_at)
        VALUES (?, ?, ?, ?, now(6) + INTERVAL 1 HOUR)
        ON DUPLICATE KEY UPDATE expires_at = VALUES(expires_at)
        "#,
    )
    .bind(auth.tenant_id)
    .bind(child_id)
    .bind(auth.member_id)
    .bind(req.scope.as_db())
    .execute(&mut *tx)
    .await?;

    let mute = sqlx::query_as::<_, ChildMuteResponse>(
        r#"
        SELECT child_member_id, muted_by_member_id, scope, expires_at
        FROM child_mutes
        WHERE child_member_id = ? AND muted_by_member_id = ? AND scope = ?
        "#,
    )
    .bind(child_id)
    .bind(auth.member_id)
    .bind(req.scope.as_db())
    .fetch_one(&mut *tx)
    .await?;

    emit_tx(
        &mut tx,
        &mut events,
        auth.tenant_id,
        CHILD_MUTE_EVENT,
        None,
        Some(child_id),
        json!({"scope": req.scope, "muted": true}),
    )
    .await?;
    tx.commit().await?;
    events.publish(&state.hub);
    Ok(Json(mute))
}

pub(super) async fn unmute_child(
    State(state): State<AppState>,
    auth: AuthUser,
    Path(child_id): Path<Uuid>,
    Query(query): Query<UnmuteQuery>,
) -> AppResult<Json<Value>> {
    require_adult(&auth)?;
    let mut tx = state.pool.begin().await?;
    let mut events = PendingEvents::default();
    match query.scope {
        MuteScope::Direct => {
            sqlx::query(
                "DELETE FROM child_mutes WHERE child_member_id = ? AND tenant_id = ? AND muted_by_member_id = ? AND scope = 'direct'",
            )
            .bind(child_id)
            .bind(auth.tenant_id)
            .bind(auth.member_id)
            .execute(&mut *tx)
            .await?;
        }
        MuteScope::All => {
            sqlx::query(
                "DELETE FROM child_mutes WHERE child_member_id = ? AND tenant_id = ? AND scope = 'all'",
            )
            .bind(child_id)
            .bind(auth.tenant_id)
            .execute(&mut *tx)
            .await?;
        }
    }
    emit_tx(
        &mut tx,
        &mut events,
        auth.tenant_id,
        CHILD_MUTE_EVENT,
        None,
        Some(child_id),
        json!({"scope": query.scope, "muted": false}),
    )
    .await?;
    tx.commit().await?;
    events.publish(&state.hub);
    Ok(Json(json!({ "ok": true })))
}

pub async fn assert_child_can_send(
    pool: &MySqlPool,
    auth: &AuthUser,
    chat_id: Uuid,
) -> AppResult<()> {
    if auth.role != Role::Child {
        return Ok(());
    }
    let muted = sqlx::query(
        r#"
        SELECT 1
        FROM child_mutes cm
        WHERE cm.child_member_id = ? AND cm.tenant_id = ? AND cm.expires_at > now(6)
          AND (
            cm.scope = 'all'
            OR EXISTS (
              SELECT 1
              FROM chats c
              JOIN chat_participants cp ON cp.chat_id = c.id
              WHERE c.id = ? AND c.is_group = FALSE AND cp.member_id = cm.muted_by_member_id
            )
          )
        LIMIT 1
        "#,
    )
    .bind(auth.member_id)
    .bind(auth.tenant_id)
    .bind(chat_id)
    .fetch_optional(pool)
    .await?
    .is_some();
    if muted {
        Err(AppError::ChildMuted)
    } else {
        Ok(())
    }
}
