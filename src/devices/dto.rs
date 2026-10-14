use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use uuid::Uuid;

#[derive(Deserialize)]
pub(super) struct DeviceRequest {
    pub device_id: Uuid,
    pub push_token: Option<String>,
    pub public_key: Option<String>,
}

#[derive(Deserialize)]
pub(super) struct KeyRecipientQuery {
    pub member_ids: Option<String>,
}

#[derive(Serialize, FromRow)]
pub(super) struct PendingDeviceResponse {
    pub id: Uuid,
    pub user_id: Uuid,
    pub member_id: Uuid,
    pub name: String,
    pub public_key: Option<String>,
    pub created_at: DateTime<Utc>,
}
