use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum MuteScope {
    Direct,
    All,
}

impl MuteScope {
    pub(super) fn as_db(self) -> &'static str {
        match self {
            Self::Direct => "direct",
            Self::All => "all",
        }
    }
}

#[derive(Deserialize)]
pub(super) struct MuteRequest {
    pub scope: MuteScope,
}

#[derive(Deserialize)]
pub(super) struct UnmuteQuery {
    pub scope: MuteScope,
}

#[derive(Serialize, FromRow)]
pub(super) struct ChildMuteResponse {
    pub child_member_id: Uuid,
    pub muted_by_member_id: Uuid,
    pub scope: String,
    pub expires_at: DateTime<Utc>,
}
