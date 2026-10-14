use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
    time::Duration,
};

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

#[derive(Clone, Debug, Serialize, sqlx::FromRow)]
pub struct RealtimeEvent {
    pub id: Uuid,
    #[serde(rename = "type")]
    #[sqlx(rename = "type")]
    pub event_type: String,
    pub tenant_id: Uuid,
    pub chat_id: Option<Uuid>,
    pub entity_id: Option<Uuid>,
    pub occurred_at: DateTime<Utc>,
    pub payload: Value,
}

const TENANT_CHANNEL_CAPACITY: usize = 256;

#[derive(Clone, Default)]
pub struct Hub {
    tenants: Arc<Mutex<HashMap<Uuid, broadcast::Sender<RealtimeEvent>>>>,
}

impl Hub {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn subscribe(&self, tenant_id: Uuid) -> broadcast::Receiver<RealtimeEvent> {
        let mut tenants = self.tenants.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        tenants.retain(|_, sender| sender.receiver_count() > 0);
        tenants
            .entry(tenant_id)
            .or_insert_with(|| broadcast::channel(TENANT_CHANNEL_CAPACITY).0)
            .subscribe()
    }

    fn publish(&self, event: RealtimeEvent) {
        let tenants = self.tenants.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(sender) = tenants.get(&event.tenant_id) {
            let _ = sender.send(event);
        }
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

#[derive(Default)]
#[must_use = "events have to be published after the transaction commits"]
pub struct PendingEvents(Vec<RealtimeEvent>);

impl PendingEvents {
    pub fn publish(self, hub: &Hub) {
        for event in self.0 {
            hub.publish(event);
        }
    }
}

pub async fn emit_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::MySql>,
    events: &mut PendingEvents,
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
    events.0.push(event);
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
    let mut events = PendingEvents::default();
    emit_tx(
        &mut tx, &mut events, tenant_id, event_type, chat_id, entity_id, payload,
    )
    .await?;
    tx.commit().await?;
    events.publish(hub);
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
    let mut rx = state.hub.subscribe(tenant_id);

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
    let token_expiry = tokio::time::sleep(seconds_until(claims.exp));
    tokio::pin!(token_expiry);

    loop {
        tokio::select! {
            event = rx.recv() => {
                match event {
                    Ok(event) => {
                        let was_in_chat = event.chat_id.is_some_and(|chat_id| chats.contains(&chat_id));
                        if event.event_type == "chat.deleted" {
                            // The chat row (and its participants) are already gone, so
                            // membership is decided by the set loaded before the deletion.
                            match event.entity_id {
                                Some(deleted) if chats.remove(&deleted) => {}
                                _ => continue,
                            }
                        } else if matches!(event.event_type.as_str(), "chat.created" | "chat.updated") {
                            if let Ok(set) = load_member_chats(&state.pool, tenant_id, member_id).await {
                                chats = set;
                            }
                        }
                        if let Some(chat_id) = event.chat_id {
                            if !chats.contains(&chat_id) && !was_in_chat {
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
            _ = &mut token_expiry => {
                let _ = sender.send(Message::Close(None)).await;
                break;
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

fn seconds_until(unix_seconds: usize) -> Duration {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    Duration::from_secs((unix_seconds as u64).saturating_sub(now))
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

#[cfg(test)]
mod tests {
    use super::*;

    fn event_for(tenant_id: Uuid) -> RealtimeEvent {
        RealtimeEvent {
            id: Uuid::new_v4(),
            event_type: "chat.updated".into(),
            tenant_id,
            chat_id: None,
            entity_id: None,
            occurred_at: Utc::now(),
            payload: json!({}),
        }
    }

    #[test]
    fn a_subscriber_only_receives_its_own_tenant_events() {
        let hub = Hub::new();
        let tenant = Uuid::new_v4();
        let other_tenant = Uuid::new_v4();
        let mut rx = hub.subscribe(tenant);
        let _other_rx = hub.subscribe(other_tenant);

        hub.publish(event_for(other_tenant));
        assert!(rx.try_recv().is_err());

        let own = event_for(tenant);
        hub.publish(own.clone());
        assert_eq!(rx.try_recv().expect("own event").id, own.id);
    }

    #[test]
    fn channels_without_subscribers_are_dropped() {
        let hub = Hub::new();
        drop(hub.subscribe(Uuid::new_v4()));
        let _rx = hub.subscribe(Uuid::new_v4());
        assert_eq!(hub.tenants.lock().expect("lock").len(), 1);
    }
}
