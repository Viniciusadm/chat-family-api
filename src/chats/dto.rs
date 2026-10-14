use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use uuid::Uuid;

#[derive(Deserialize)]
pub(super) struct ChatRequest {
    pub name: Option<String>,
    pub is_group: Option<bool>,
    pub participant_ids: Option<Vec<Uuid>>,
}

#[derive(Deserialize)]
pub(super) struct ReadRequest {
    pub read_up_to: Option<DateTime<Utc>>,
}

#[derive(Deserialize)]
pub(super) struct MessageQuery {
    pub after: Option<DateTime<Utc>>,
    pub before: Option<DateTime<Utc>>,
    pub changed_after: Option<DateTime<Utc>>,
    pub limit: Option<i64>,
}

#[derive(Deserialize)]
pub(super) struct MessageRequest {
    pub id: Uuid,
    #[serde(rename = "type")]
    pub kind: String,
    pub ciphertext: Option<String>,
    pub iv: Option<String>,
    pub enc_version: Option<i32>,
    pub audio_url: Option<String>,
    pub audio_duration: Option<f64>,
    pub image_url: Option<String>,
    pub thumbnail_url: Option<String>,
    pub image_width: Option<i32>,
    pub image_height: Option<i32>,
    pub image_file_size: Option<i64>,
    pub reply_to_message_id: Option<Uuid>,
    pub reply_to_sender_id: Option<Uuid>,
    pub reply_to_sender_name: Option<String>,
    pub reply_to_type: Option<String>,
}

#[derive(Deserialize)]
pub(super) struct ReactionRequest {
    pub emoji: String,
}

#[derive(Serialize, FromRow)]
pub(super) struct ChatRow {
    pub id: Uuid,
    pub tenant_id: Uuid,
    pub is_group: bool,
    pub name: String,
    pub photo_url: Option<String>,
    pub photo_path: Option<String>,
    pub last_message_ciphertext: Option<String>,
    pub last_message_iv: Option<String>,
    pub last_message_type: Option<String>,
    pub last_message_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Serialize)]
pub(super) struct ChatResponse {
    #[serde(flatten)]
    pub chat: ChatRow,
    pub participant_ids: Vec<Uuid>,
    pub read_up_to: HashMap<Uuid, DateTime<Utc>>,
    pub unread_by: HashMap<Uuid, i32>,
}

pub(super) const CHAT_COLUMNS: &str = "c.id, c.tenant_id, c.is_group, c.name, c.photo_url, c.photo_path, \
     c.last_message_ciphertext, c.last_message_iv, c.last_message_type, c.last_message_at, \
     c.created_at, c.updated_at";

#[derive(Serialize, FromRow)]
pub(super) struct MessageResponse {
    pub id: Uuid,
    pub chat_id: Uuid,
    pub sender_member_id: Uuid,
    #[serde(rename = "type")]
    #[sqlx(rename = "type")]
    pub message_type: String,
    pub kind: String,
    pub ciphertext: Option<String>,
    pub iv: Option<String>,
    pub enc_version: Option<i32>,
    pub audio_url: Option<String>,
    pub audio_duration: Option<f64>,
    pub image_url: Option<String>,
    pub thumbnail_url: Option<String>,
    pub image_width: Option<i32>,
    pub image_height: Option<i32>,
    pub image_file_size: Option<i64>,
    pub reply_to_message_id: Option<Uuid>,
    pub reply_to_sender_id: Option<Uuid>,
    pub reply_to_sender_name: Option<String>,
    pub reply_to_type: Option<String>,
    pub reply_kind: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub edited_at: Option<DateTime<Utc>>,
    pub is_deleted: bool,
    pub deleted_at: Option<DateTime<Utc>>,
}

pub(super) const MESSAGE_COLUMNS: &str = "id, chat_id, sender_member_id, type, type AS kind, \
     ciphertext, iv, enc_version, audio_url, audio_duration, image_url, thumbnail_url, \
     image_width, image_height, image_file_size, reply_to_message_id, reply_to_sender_id, \
     reply_to_sender_name, reply_to_type, reply_to_type AS reply_kind, created_at, updated_at, \
     edited_at, is_deleted, deleted_at";

#[derive(Serialize, FromRow)]
pub(super) struct ReactionResponse {
    pub message_id: Uuid,
    pub member_id: Uuid,
    pub chat_id: Uuid,
    pub emoji: String,
    pub updated_at: DateTime<Utc>,
}
