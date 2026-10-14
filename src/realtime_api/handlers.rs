use axum::{
    Json,
    extract::{Query, State},
};
use chrono::{DateTime, Utc};
use serde::Deserialize;

use crate::{auth::AuthUser, error::AppResult, realtime::RealtimeEvent, state::AppState};

#[derive(Deserialize)]
pub(super) struct RealtimeQuery {
    after: Option<DateTime<Utc>>,
    limit: Option<i64>,
}

pub(super) async fn list_realtime_events(
    State(state): State<AppState>,
    auth: AuthUser,
    Query(query): Query<RealtimeQuery>,
) -> AppResult<Json<Vec<RealtimeEvent>>> {
    let events = sqlx::query_as::<_, RealtimeEvent>(
        r#"
        SELECT e.id, e.tenant_id, e.type, e.chat_id, e.entity_id, e.occurred_at, e.payload
        FROM realtime_events e
        WHERE e.tenant_id = ?
          AND (? IS NULL OR e.occurred_at > ?)
          AND (
            e.chat_id IS NULL
            OR EXISTS (
                SELECT 1 FROM chat_participants cp
                WHERE cp.chat_id = e.chat_id AND cp.member_id = ?
            )
          )
        ORDER BY e.occurred_at ASC, e.id ASC
        LIMIT ?
        "#,
    )
    .bind(auth.tenant_id)
    .bind(query.after)
    .bind(query.after)
    .bind(auth.member_id)
    .bind(query.limit.unwrap_or(500).clamp(1, 1000))
    .fetch_all(&state.pool)
    .await?;
    Ok(Json(events))
}
