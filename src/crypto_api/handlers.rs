use axum::{
    Json,
    extract::{Path, State},
};
use serde_json::{Value, json};
use sqlx::Row;
use uuid::Uuid;

use crate::{
    auth::{AuthUser, Role, assert_chat_participant, require_adult},
    error::{AppError, AppResult},
    realtime::{PendingEvents, emit_tx},
    state::AppState,
};

use super::dto::{
    KeyBackupRequest, KeyBackupResponse, KeyRecipientDevice, KeyShareRequest, KeyShareResponse,
    PasswordSettingsRequest, PasswordSettingsResponse,
};

pub(super) async fn get_password_settings(
    State(state): State<AppState>,
    auth: AuthUser,
) -> AppResult<Json<PasswordSettingsResponse>> {
    let settings = sqlx::query_as::<_, PasswordSettingsResponse>(
        "SELECT password_salt, password_verifier_ciphertext, password_verifier_iv FROM users WHERE id = ?",
    )
    .bind(auth.user_id)
    .fetch_one(&state.pool)
    .await?;
    Ok(Json(settings))
}

pub(super) async fn put_password_settings(
    State(state): State<AppState>,
    auth: AuthUser,
    Json(req): Json<PasswordSettingsRequest>,
) -> AppResult<Json<Value>> {
    sqlx::query("UPDATE users SET password_salt = ?, password_verifier_ciphertext = ?, password_verifier_iv = ? WHERE id = ?")
        .bind(req.password_salt)
        .bind(req.password_verifier_ciphertext)
        .bind(req.password_verifier_iv)
        .bind(auth.user_id)
        .execute(&state.pool)
        .await?;
    Ok(Json(json!({ "ok": true })))
}

pub(super) async fn delete_password_settings(
    State(state): State<AppState>,
    auth: AuthUser,
) -> AppResult<Json<Value>> {
    sqlx::query("UPDATE users SET password_salt = NULL, password_verifier_ciphertext = NULL, password_verifier_iv = NULL WHERE id = ?")
        .bind(auth.user_id)
        .execute(&state.pool)
        .await?;
    Ok(Json(json!({ "ok": true })))
}

pub(super) async fn list_key_backups(
    State(state): State<AppState>,
    auth: AuthUser,
) -> AppResult<Json<Vec<KeyBackupResponse>>> {
    let backups = sqlx::query_as::<_, KeyBackupResponse>(
        "SELECT chat_id, ciphertext, iv, enc_version, created_at FROM key_backups WHERE user_id = ?",
    )
    .bind(auth.user_id)
    .fetch_all(&state.pool)
    .await?;
    Ok(Json(backups))
}

pub(super) async fn put_key_backup(
    State(state): State<AppState>,
    auth: AuthUser,
    Path(chat_id): Path<Uuid>,
    Json(req): Json<KeyBackupRequest>,
) -> AppResult<Json<Value>> {
    assert_chat_participant(&state.pool, chat_id, auth.member_id, auth.tenant_id).await?;
    sqlx::query(
        r#"
        INSERT INTO key_backups (user_id, chat_id, ciphertext, iv, enc_version)
        VALUES (?, ?, ?, ?, ?)
        ON DUPLICATE KEY UPDATE ciphertext = VALUES(ciphertext), iv = VALUES(iv), enc_version = VALUES(enc_version), created_at = now()
        "#,
    )
    .bind(auth.user_id)
    .bind(chat_id)
    .bind(req.ciphertext)
    .bind(req.iv)
    .bind(req.enc_version)
    .execute(&state.pool)
    .await?;
    Ok(Json(json!({ "ok": true })))
}

pub(super) async fn delete_key_backups(
    State(state): State<AppState>,
    auth: AuthUser,
) -> AppResult<Json<Value>> {
    sqlx::query("DELETE FROM key_backups WHERE user_id = ?")
        .bind(auth.user_id)
        .execute(&state.pool)
        .await?;
    Ok(Json(json!({ "ok": true })))
}

pub(super) async fn list_key_shares(
    State(state): State<AppState>,
    auth: AuthUser,
    Path(device_id): Path<Uuid>,
) -> AppResult<Json<Vec<KeyShareResponse>>> {
    if auth.device_id != Some(device_id) && auth.role != Role::Adult {
        return Err(AppError::Forbidden);
    }
    // The tenant join is what stops an adult from reading another family's key shares.
    let shares = sqlx::query_as::<_, KeyShareResponse>(
        r#"
        SELECT ks.device_id, ks.chat_id, ks.ephemeral_public_key, ks.iv, ks.ciphertext,
               ks.wrapped_by_member_id, ks.created_at
        FROM key_shares ks
        JOIN devices d ON d.id = ks.device_id
        JOIN users u ON u.id = d.user_id
        JOIN chat_participants cp ON cp.chat_id = ks.chat_id AND cp.member_id = u.member_id
        WHERE ks.device_id = ? AND d.tenant_id = ?
        ORDER BY ks.created_at DESC
        "#,
    )
    .bind(device_id)
    .bind(auth.tenant_id)
    .fetch_all(&state.pool)
    .await?;
    Ok(Json(shares))
}

pub(super) async fn list_chat_key_recipients(
    State(state): State<AppState>,
    auth: AuthUser,
    Path(chat_id): Path<Uuid>,
) -> AppResult<Json<Vec<KeyRecipientDevice>>> {
    assert_chat_participant(&state.pool, chat_id, auth.member_id, auth.tenant_id).await?;
    let devices = sqlx::query_as::<_, KeyRecipientDevice>(
        r#"
        SELECT d.id, d.user_id, d.public_key, d.approved, d.active, d.created_at, u.member_id
        FROM chat_participants cp
        JOIN members m ON m.id = cp.member_id AND m.deleted_at IS NULL
        JOIN users u ON u.member_id = cp.member_id AND u.tenant_id = ? AND u.deleted_at IS NULL
        JOIN devices d ON d.user_id = u.id AND d.tenant_id = ?
        WHERE cp.chat_id = ?
          AND d.approved = true
          AND d.active = true
          AND d.public_key IS NOT NULL
        ORDER BY d.created_at DESC
        "#,
    )
    .bind(auth.tenant_id)
    .bind(auth.tenant_id)
    .bind(chat_id)
    .fetch_all(&state.pool)
    .await?;
    Ok(Json(devices))
}

pub(super) async fn put_key_share(
    State(state): State<AppState>,
    auth: AuthUser,
    Path((device_id, chat_id)): Path<(Uuid, Uuid)>,
    Json(req): Json<KeyShareRequest>,
) -> AppResult<Json<Value>> {
    require_adult(&auth)?;
    write_key_share(&state, &auth, chat_id, device_id, req).await
}

pub(super) async fn put_chat_key_share(
    State(state): State<AppState>,
    auth: AuthUser,
    Path((chat_id, device_id)): Path<(Uuid, Uuid)>,
    Json(req): Json<KeyShareRequest>,
) -> AppResult<Json<Value>> {
    write_key_share(&state, &auth, chat_id, device_id, req).await
}

async fn write_key_share(
    state: &AppState,
    auth: &AuthUser,
    chat_id: Uuid,
    device_id: Uuid,
    req: KeyShareRequest,
) -> AppResult<Json<Value>> {
    assert_chat_participant(&state.pool, chat_id, auth.member_id, auth.tenant_id).await?;
    let mut tx = state.pool.begin().await?;
    let mut events = PendingEvents::default();
    let device_allowed = sqlx::query(
        r#"
        SELECT 1
        FROM devices d
        JOIN users u ON u.id = d.user_id AND u.deleted_at IS NULL
        JOIN chat_participants cp ON cp.member_id = u.member_id
        WHERE d.id = ?
          AND d.tenant_id = ?
          AND d.approved = true
          AND d.active = true
          AND cp.chat_id = ?
        "#,
    )
    .bind(device_id)
    .bind(auth.tenant_id)
    .bind(chat_id)
    .fetch_optional(&mut *tx)
    .await?
    .is_some();
    if !device_allowed {
        return Err(AppError::NotFound("device not found".into()));
    }

    sqlx::query(
        r#"
        INSERT INTO key_shares (device_id, chat_id, ephemeral_public_key, iv, ciphertext, wrapped_by_member_id)
        VALUES (?, ?, ?, ?, ?, ?)
        ON DUPLICATE KEY UPDATE
            ephemeral_public_key = IF(wrapped_by_member_id = VALUES(wrapped_by_member_id), VALUES(ephemeral_public_key), ephemeral_public_key),
            iv = IF(wrapped_by_member_id = VALUES(wrapped_by_member_id), VALUES(iv), iv),
            ciphertext = IF(wrapped_by_member_id = VALUES(wrapped_by_member_id), VALUES(ciphertext), ciphertext),
            created_at = IF(wrapped_by_member_id = VALUES(wrapped_by_member_id), now(), created_at)
        "#,
    )
    .bind(device_id)
    .bind(chat_id)
    .bind(req.ephemeral_public_key)
    .bind(req.iv)
    .bind(req.ciphertext)
    .bind(auth.member_id)
    .execute(&mut *tx)
    .await?;

    let wrapper: Uuid = sqlx::query_scalar(
        "SELECT wrapped_by_member_id FROM key_shares WHERE device_id = ? AND chat_id = ?",
    )
    .bind(device_id)
    .bind(chat_id)
    .fetch_one(&mut *tx)
    .await?;
    if wrapper != auth.member_id {
        tx.commit().await?;
        return Ok(Json(json!({ "ok": true, "kept_existing": true })));
    }

    emit_tx(
        &mut tx,
        &mut events,
        auth.tenant_id,
        "key_share.updated",
        Some(chat_id),
        Some(device_id),
        json!({ "device_id": device_id }),
    )
    .await?;
    tx.commit().await?;
    events.publish(&state.hub);
    Ok(Json(json!({ "ok": true })))
}

const STALE_KEY_CLAIM_MINUTES: i64 = 10;

pub(super) async fn claim_chat_key(
    State(state): State<AppState>,
    auth: AuthUser,
    Path(chat_id): Path<Uuid>,
) -> AppResult<Json<Value>> {
    assert_chat_participant(&state.pool, chat_id, auth.member_id, auth.tenant_id).await?;
    let device_id = auth
        .device_id
        .ok_or(AppError::BadRequest("a device is required to claim a chat key".into()))?;

    let mut tx = state.pool.begin().await?;
    sqlx::query("SELECT id FROM chats WHERE id = ? AND tenant_id = ? FOR UPDATE")
        .bind(chat_id)
        .bind(auth.tenant_id)
        .fetch_one(&mut *tx)
        .await?;

    let claim = sqlx::query(
        r#"
        SELECT device_id, claimed_at < now(6) - INTERVAL ? MINUTE AS stale
        FROM chat_key_claims
        WHERE chat_id = ?
        "#,
    )
    .bind(STALE_KEY_CLAIM_MINUTES)
    .bind(chat_id)
    .fetch_optional(&mut *tx)
    .await?;

    if let Some(row) = &claim {
        if row.try_get::<Uuid, _>("device_id")? == device_id {
            tx.commit().await?;
            return Ok(Json(json!({ "granted": true })));
        }
    }

    let key_already_shared = sqlx::query(
        r#"
        SELECT 1 FROM key_shares WHERE chat_id = ?
        UNION ALL
        SELECT 1 FROM key_backups WHERE chat_id = ?
        LIMIT 1
        "#,
    )
    .bind(chat_id)
    .bind(chat_id)
    .fetch_optional(&mut *tx)
    .await?
    .is_some();

    let claim_is_open = match &claim {
        None => true,
        Some(row) => row.try_get::<i64, _>("stale")? != 0,
    };
    if key_already_shared || !claim_is_open {
        tx.commit().await?;
        return Ok(Json(json!({ "granted": false })));
    }

    sqlx::query(
        r#"
        INSERT INTO chat_key_claims (chat_id, tenant_id, device_id)
        VALUES (?, ?, ?)
        ON DUPLICATE KEY UPDATE device_id = VALUES(device_id), claimed_at = now(6)
        "#,
    )
    .bind(chat_id)
    .bind(auth.tenant_id)
    .bind(device_id)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(Json(json!({ "granted": true })))
}

pub(crate) async fn revoke_chat_key_access(
    tx: &mut sqlx::Transaction<'_, sqlx::MySql>,
    chat_id: Option<Uuid>,
    member_id: Uuid,
) -> AppResult<()> {
    sqlx::query(
        r#"
        DELETE ks FROM key_shares ks
        JOIN devices d ON d.id = ks.device_id
        JOIN users u ON u.id = d.user_id
        WHERE u.member_id = ? AND (? IS NULL OR ks.chat_id = ?)
        "#,
    )
    .bind(member_id)
    .bind(chat_id)
    .bind(chat_id)
    .execute(&mut **tx)
    .await?;
    sqlx::query(
        r#"
        DELETE kb FROM key_backups kb
        JOIN users u ON u.id = kb.user_id
        WHERE u.member_id = ? AND (? IS NULL OR kb.chat_id = ?)
        "#,
    )
    .bind(member_id)
    .bind(chat_id)
    .bind(chat_id)
    .execute(&mut **tx)
    .await?;
    Ok(())
}
