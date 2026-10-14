use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use uuid::Uuid;

#[derive(Deserialize)]
pub(super) struct PasswordSettingsRequest {
    pub password_salt: Option<String>,
    pub password_verifier_ciphertext: Option<String>,
    pub password_verifier_iv: Option<String>,
}

#[derive(Deserialize)]
pub(super) struct KeyBackupRequest {
    pub ciphertext: String,
    pub iv: String,
    pub enc_version: i32,
}

#[derive(Deserialize)]
pub(super) struct KeyShareRequest {
    pub ephemeral_public_key: String,
    pub iv: String,
    pub ciphertext: String,
}

#[derive(Serialize, FromRow)]
pub(super) struct PasswordSettingsResponse {
    pub password_salt: Option<String>,
    pub password_verifier_ciphertext: Option<String>,
    pub password_verifier_iv: Option<String>,
}

#[derive(Serialize, FromRow)]
pub(super) struct KeyBackupResponse {
    pub chat_id: Uuid,
    pub ciphertext: String,
    pub iv: String,
    pub enc_version: i32,
    pub created_at: DateTime<Utc>,
}

#[derive(Serialize, FromRow)]
pub(super) struct KeyShareResponse {
    pub device_id: Uuid,
    pub chat_id: Uuid,
    pub ephemeral_public_key: String,
    pub iv: String,
    pub ciphertext: String,
    pub wrapped_by_member_id: Uuid,
    pub created_at: DateTime<Utc>,
}

#[derive(Serialize, FromRow)]
pub(crate) struct KeyRecipientDevice {
    pub id: Uuid,
    pub user_id: Uuid,
    pub member_id: Uuid,
    pub public_key: Option<String>,
    pub approved: bool,
    pub active: bool,
    pub created_at: DateTime<Utc>,
}
