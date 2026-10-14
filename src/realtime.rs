use std::{collections::HashSet, time::Duration};

use axum::{
    extract::{
        Query, State, WebSocketUpgrade,
        ws::{Message, WebSocket},
    },
    http::{HeaderMap, StatusCode, header},
    response::Response,
};
use chrono::{DateTime, Utc};
use futures_util::{SinkExt, StreamExt};
use jsonwebtoken::{DecodingKey, Validation, decode};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::{MySqlPool, Row};
use tokio::sync::broadcast;
use uuid::Uuid;

use crate::{
    auth::{Claims, assert_session_active},
    error::AppResult,
    state::AppState,
};

#[derive(Clone, Debug, Serialize)]
pub struct RealtimeEvent {
    pub id: Uuid,
    #[serde(rename = "type")]
    pub event_type: String,
    pub tenant_id: Uuid,
    pub chat_id: Option<Uuid>,
    pub entity_id: Option<Uuid>,
    pub occurred_at: DateTime<Utc>,
    pub payload: Value,
}

#[derive(Clone)]
pub struct Hub {
    tx: broadcast::Sender<RealtimeEvent>,
}

impl Hub {
    pub fn new() -> Self {
        let (tx, _rx) = broadcast::channel(1024);
        Self { tx }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<RealtimeEvent> {
        self.tx.subscribe()
    }

    fn publish(&self, event: RealtimeEvent) {
        let _ = self.tx.send(event);
    }
}

impl Default for Hub {
    fn default() -> Self {
        Self::new()
    }
}

const EVENT_RETENTION_INTERVAL: Duration = Duration::from_secs(3600);
const EVENT_RETENTION_BATCH: u64 = 5_000;

/// realtime_events is an append-only catch-up log. Without pruning it grows forever, and a
/// client that asks for the log without a cursor replays an ever longer prefix of it.
pub fn spawn_event_retention(pool: MySqlPool) {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(EVENT_RETENTION_INTERVAL);
        loop {
            ticker.tick().await;
            loop {
                let deleted = sqlx::query(
                    "DELETE FROM realtime_events WHERE occurred_at < now() - INTERVAL 7 DAY LIMIT 5000",
                )
                .execute(&pool)
                .await;
                match deleted {
                    Ok(done) => {
                        let rows = done.rows_affected();
                        if rows > 0 {
                            tracing::info!(rows, "pruned realtime events");
                        }
                        if rows < EVENT_RETENTION_BATCH {
                            break;
                        }
                    }
                    Err(err) => {
                        tracing::warn!(?err, "failed to prune realtime events");
                        break;
                    }
                }
            }
        }
    });
}

pub async fn emit_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::MySql>,
    hub: &Hub,
    tenant_id: Uuid,
    event_type: &str,
    chat_id: Option<Uuid>,
    entity_id: Option<Uuid>,
    payload: Value,
) -> AppResult<()> {
    let id = Uuid::new_v4();
    let occurred_at = Utc::now();
    sqlx::query(
        "INSERT INTO realtime_events (id, tenant_id, type, chat_id, entity_id, occurred_at, payload) VALUES (?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(id)
    .bind(tenant_id)
    .bind(event_type)
    .bind(chat_id)
    .bind(entity_id)
    .bind(occurred_at)
    .bind(&payload)
    .execute(&mut **tx)
    .await?;

    let event = RealtimeEvent {
        id,
        event_type: event_type.to_string(),
        tenant_id,
        chat_id,
        entity_id,
        occurred_at,
        payload,
    };
    hub.publish(event);
    Ok(())
}

pub async fn emit(
    pool: &MySqlPool,
    hub: &Hub,
    tenant_id: Uuid,
    event_type: &str,
    chat_id: Option<Uuid>,
    entity_id: Option<Uuid>,
    payload: Value,
) -> AppResult<()> {
    let mut tx = pool.begin().await?;
    emit_tx(
        &mut tx, hub, tenant_id, event_type, chat_id, entity_id, payload,
    )
    .await?;
    tx.commit().await?;
    Ok(())
}

#[derive(Deserialize)]
pub struct WsQuery {
    access_token: Option<String>,
}

pub async fn realtime_handler(
    State(state): State<AppState>,
    Query(query): Query<WsQuery>,
    headers: HeaderMap,
    ws: WebSocketUpgrade,
) -> Result<Response, StatusCode> {
    let token = query
        .access_token
        .or_else(|| {
            headers
                .get(header::AUTHORIZATION)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.strip_prefix("Bearer "))
                .map(|value| value.to_string())
        })
        .ok_or(StatusCode::UNAUTHORIZED)?;

    let claims = decode::<Claims>(
        &token,
        &DecodingKey::from_secret(state.config.jwt_secret.as_bytes()),
        &Validation::default(),
    )
    .map_err(|_| StatusCode::UNAUTHORIZED)?
    .claims;

    assert_session_active(&state.pool, &claims)
        .await
        .map_err(|(status, _)| status)?;

    Ok(ws.on_upgrade(move |socket| connection(state, claims, socket)))
}

async fn connection(state: AppState, claims: Claims, socket: WebSocket) {
    let tenant_id = claims.tenant_id;
    let member_id = claims.member_id;
    let mut rx = state.hub.subscribe();

    let chats = match load_member_chats(&state.pool, tenant_id, member_id).await {
        Ok(set) => set,
        Err(err) => {
            tracing::warn!(?err, "ws chat preload failed");
            return;
        }
    };
    let mut chats: HashSet<Uuid> = chats;

    let (mut sender, mut receiver) = socket.split();
    let hello = json!({"type": "hello", "member_id": member_id, "tenant_id": tenant_id});
    if sender
        .send(Message::Text(hello.to_string().into()))
        .await
        .is_err()
    {
        return;
    }

    let mut device_check = tokio::time::interval(Duration::from_secs(15));
    device_check.tick().await;

    loop {
        tokio::select! {
            event = rx.recv() => {
                match event {
                    Ok(event) => {
                        if event.tenant_id != tenant_id {
                            continue;
                        }
                        if event.event_type == "chat.deleted" {
                            // The chat row (and its participants) are already gone, so
                            // membership is decided by the set loaded before the deletion.
                            match event.entity_id {
                                Some(deleted) if chats.remove(&deleted) => {}
                                _ => continue,
                            }
                        } else if event.event_type == "member.updated" || event.event_type == "chat.created" {
                            if let Ok(set) = load_member_chats(&state.pool, tenant_id, member_id).await {
                                chats = set;
                            }
                        }
                        if let Some(chat_id) = event.chat_id {
                            if !chats.contains(&chat_id) {
                                continue;
                            }
                        }
                        if let Ok(text) = serde_json::to_string(&event) {
                            if sender.send(Message::Text(text.into())).await.is_err() {
                                break;
                            }
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        // The dropped events cannot be replayed from the channel, so the
                        // client has to reconcile over HTTP or it silently misses them.
                        tracing::warn!(skipped, %member_id, "realtime subscriber lagged");
                        let resync = json!({"type": "resync"});
                        if sender.send(Message::Text(resync.to_string().into())).await.is_err() {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
            _ = device_check.tick() => {
                if assert_session_active(&state.pool, &claims).await.is_err() {
                    let _ = sender.send(Message::Close(None)).await;
                    break;
                }
            }
            msg = receiver.next() => {
                match msg {
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Ok(Message::Ping(p))) => {
                        if sender.send(Message::Pong(p)).await.is_err() { break; }
                    }
                    Some(Err(_)) => break,
                    _ => {}
                }
            }
        }
    }
}

async fn load_member_chats(
    pool: &MySqlPool,
    tenant_id: Uuid,
    member_id: Uuid,
) -> Result<HashSet<Uuid>, sqlx::Error> {
    let rows = sqlx::query(
        r#"
        SELECT cp.chat_id
        FROM chat_participants cp
        JOIN chats c ON c.id = cp.chat_id
        WHERE cp.member_id = ? AND c.tenant_id = ?
        "#,
    )
    .bind(member_id)
    .bind(tenant_id)
    .fetch_all(pool)
    .await?;

    let mut set = HashSet::new();
    for row in rows {
        set.insert(row.try_get::<Uuid, _>("chat_id")?);
    }
    Ok(set)
}
