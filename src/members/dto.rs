use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use uuid::Uuid;

use crate::auth::Role;

#[derive(Deserialize)]
pub(super) struct MemberRequest {
    pub name: Option<String>,
    pub role: Option<Role>,
}

#[derive(Serialize, FromRow)]
pub(super) struct MemberResponse {
    pub id: Uuid,
    pub name: String,
    pub role: String,
    pub login_code: Option<String>,
    pub photo_url: Option<String>,
    pub photo_path: Option<String>,
    pub created_at: DateTime<Utc>,
}
