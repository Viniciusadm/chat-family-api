use std::net::{IpAddr, SocketAddr};

use axum::{
    Json,
    extract::{ConnectInfo, State},
};
use serde_json::{Value, json};
use sqlx::Row;
use uuid::Uuid;

use crate::{
    auth::{AuthUser, Role, hash_password, hash_refresh_token, rate_limit, verify_password},
    devices::activate_device,
    error::{AppError, AppResult},
    realtime::{emit, emit_tx},
    state::AppState,
};

use super::dto::{
    ChildLoginRequest, LoginRequest, RefreshRequest, RegisterRequest, TokenResponse, UserResponse,
};
use super::tokens::issue_tokens;

const IP_ATTEMPT_MULTIPLIER: u32 = 6;

/// The subject must identify the single account or credential being attacked. A constant
/// string here would make every caller in the deployment share one counter, so the first
/// handful of legitimate attempts per window would lock out everyone else.
fn login_rate_limit_buckets(
    subject: &[&str],
    peer_ip: IpAddr,
    max_attempts: u32,
) -> Vec<(String, u32)> {
    // The whole family shares one address on home WiFi, so the per-address bucket has to
    // be looser than the per-subject one or legitimate logins would lock each other out.
    vec![
        (format!("login:{}", subject.join(":")), max_attempts),
        (
            format!("login:ip:{peer_ip}"),
            max_attempts.saturating_mul(IP_ATTEMPT_MULTIPLIER),
        ),
    ]
}

/// Limits an unauthenticated attempt both by its own subject (email, device, token) and by
/// the calling address, so neither a single target nor a single source can be hammered.
async fn enforce_login_rate_limit(
    state: &AppState,
    peer: &SocketAddr,
    subject: &[&str],
) -> AppResult<()> {
    let window_secs = state.config.child_login_window_secs;
    let buckets = login_rate_limit_buckets(
        subject,
        peer.ip(),
        state.config.child_login_max_attempts,
    );
    for (bucket, limit) in buckets {
        rate_limit(state.redis.as_ref(), &bucket, limit, window_secs).await?;
    }
    Ok(())
}

pub(super) async fn register(
    State(state): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    Json(req): Json<RegisterRequest>,
) -> AppResult<Json<TokenResponse>> {
    if req.email.trim().is_empty() || req.password.len() < 8 || req.name.trim().is_empty() {
        return Err(AppError::BadRequest(
            "email, name and password >= 8 are required".into(),
        ));
    }

    let email = req.email.trim().to_lowercase();
    enforce_login_rate_limit(&state, &peer, &["register", &email]).await?;
    tracing::debug!(email = %email, "register: checking existing email");
    let email_exists = sqlx::query_scalar::<_, i64>(
        "SELECT 1 FROM users WHERE email = ? AND deleted_at IS NULL LIMIT 1",
    )
    .bind(&email)
    .fetch_optional(&state.pool)
    .await
    .map_err(|error| {
        tracing::error!(?error, email = %email, "register: failed to check existing email");
        error
    })?
    .is_some();
    if email_exists {
        return Err(AppError::Conflict("email already registered".into()));
    }

    tracing::debug!(email = %email, "register: starting transaction");
    let mut tx = state.pool.begin().await?;
    let tenant_id = Uuid::new_v4();
    tracing::debug!(%tenant_id, email = %email, "register: inserting tenant");
    sqlx::query("INSERT INTO tenants (id, name) VALUES (?, ?)")
        .bind(tenant_id)
        .bind(format!("Família {}", req.name.trim()))
        .execute(&mut *tx)
        .await
        .map_err(|error| {
            tracing::error!(?error, %tenant_id, email = %email, "register: failed to insert tenant");
            error
        })?;

    let member_id = Uuid::new_v4();
    tracing::debug!(%tenant_id, %member_id, email = %email, "register: inserting member");
    sqlx::query("INSERT INTO members (id, tenant_id, name, role) VALUES (?, ?, ?, 'adult')")
        .bind(member_id)
        .bind(tenant_id)
        .bind(req.name.trim())
        .execute(&mut *tx)
        .await
        .map_err(|error| {
            tracing::error!(?error, %tenant_id, %member_id, email = %email, "register: failed to insert member");
            error
        })?;

    let password_hash = hash_password(req.password.clone()).await?;
    let user_id = Uuid::new_v4();
    tracing::debug!(%tenant_id, %member_id, %user_id, email = %email, "register: inserting user");
    sqlx::query(
        r#"
        INSERT INTO users (id, member_id, tenant_id, email, password_hash, name, role)
        VALUES (?, ?, ?, ?, ?, ?, 'adult')
        "#,
    )
    .bind(user_id)
    .bind(member_id)
    .bind(tenant_id)
    .bind(&email)
    .bind(password_hash)
    .bind(req.name.trim())
    .execute(&mut *tx)
    .await
    .map_err(|error| match error {
        sqlx::Error::Database(db_error) if db_error.code().as_deref() == Some("1062") => {
            AppError::Conflict("email already registered".into())
        }
        other => AppError::Database(other),
    })?;

    tracing::debug!(%tenant_id, %user_id, email = %email, "register: updating tenant owner");
    sqlx::query("UPDATE tenants SET owner_user_id = ? WHERE id = ?")
        .bind(user_id)
        .bind(tenant_id)
        .execute(&mut *tx)
        .await
        .map_err(|error| {
            tracing::error!(?error, %tenant_id, %user_id, email = %email, "register: failed to update tenant owner");
            error
        })?;

    let device_id = req.device_id.unwrap_or_else(Uuid::new_v4);
    tracing::debug!(%tenant_id, %user_id, %device_id, email = %email, "register: activating device");
    activate_device(
        &mut tx,
        tenant_id,
        user_id,
        device_id,
        true,
        req.push_token,
        req.public_key,
    )
    .await
    .map_err(|error| {
        tracing::error!(?error, %tenant_id, %user_id, %device_id, email = %email, "register: failed to activate device");
        error
    })?;

    tracing::debug!(%tenant_id, %user_id, email = %email, "register: committing transaction");
    tx.commit().await?;

    tracing::debug!(%tenant_id, %user_id, email = %email, "register: issuing tokens");
    issue_tokens(
        &state,
        AuthUser {
            user_id,
            member_id,
            tenant_id,
            device_id: Some(device_id),
            role: Role::Adult,
        },
        Some(email),
        req.name,
    )
    .await
}

pub(super) async fn login(
    State(state): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    Json(req): Json<LoginRequest>,
) -> AppResult<Json<TokenResponse>> {
    let email = req.email.trim().to_lowercase();
    enforce_login_rate_limit(&state, &peer, &["email", &email]).await?;
    let row = sqlx::query(
        r#"
        SELECT id, member_id, tenant_id, password_hash, name, role AS role, email
        FROM users
        WHERE email = ? AND deleted_at IS NULL
        "#,
    )
    .bind(&email)
    .fetch_optional(&state.pool)
    .await?
    .ok_or(AppError::Unauthorized)?;

    let Some(password_hash) = row.try_get::<Option<String>, _>("password_hash")? else {
        return Err(AppError::Unauthorized);
    };
    if !verify_password(req.password.clone(), password_hash).await? {
        return Err(AppError::Unauthorized);
    }

    let tenant_id: Uuid = row.try_get("tenant_id")?;
    let user_id: Uuid = row.try_get("id")?;
    let member_id: Uuid = row.try_get("member_id")?;

    let mut tx = state.pool.begin().await?;
    activate_device(
        &mut tx,
        tenant_id,
        user_id,
        req.device_id,
        true,
        req.push_token,
        req.public_key,
    )
    .await?;
    emit_tx(
        &mut tx,
        &state.hub,
        tenant_id,
        "device.updated",
        None,
        Some(req.device_id),
        json!({"user_id": user_id, "active": true}),
    )
    .await?;
    tx.commit().await?;

    let role_str: String = row.try_get("role")?;
    issue_tokens(
        &state,
        AuthUser {
            user_id,
            member_id,
            tenant_id,
            device_id: Some(req.device_id),
            role: Role::try_from(role_str.as_str())?,
        },
        row.try_get("email")?,
        row.try_get("name")?,
    )
    .await
}

pub(super) async fn child_login(
    State(state): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    Json(req): Json<ChildLoginRequest>,
) -> AppResult<Json<TokenResponse>> {
    let code = req.code.trim().to_string();
    // The code must never be part of the bucket key: an attacker cycling codes would
    // get a fresh counter on every attempt and never hit the limit.
    enforce_login_rate_limit(&state, &peer, &["device", &req.device_id.to_string()]).await?;

    let row = sqlx::query(
        r#"
        SELECT lc.member_id, lc.tenant_id, m.name
        FROM login_codes lc
        JOIN members m ON m.id = lc.member_id
        WHERE lc.code = ? AND lc.revoked_at IS NULL AND m.role = 'child' AND m.deleted_at IS NULL
        "#,
    )
    .bind(&code)
    .fetch_optional(&state.pool)
    .await?
    .ok_or(AppError::Unauthorized)?;

    let mut tx = state.pool.begin().await?;
    let tenant_id: Uuid = row.try_get("tenant_id")?;
    let member_id: Uuid = row.try_get("member_id")?;
    let name: String = row.try_get("name")?;

    let existing: Option<Uuid> = sqlx::query_scalar(
        "SELECT id FROM users WHERE member_id = ? AND tenant_id = ? AND deleted_at IS NULL LIMIT 1",
    )
    .bind(member_id)
    .bind(tenant_id)
    .fetch_optional(&mut *tx)
    .await?;

    let user_id = match existing {
        Some(user_id) => {
            sqlx::query("UPDATE users SET name = ? WHERE id = ?")
                .bind(&name)
                .bind(user_id)
                .execute(&mut *tx)
                .await?;
            user_id
        }
        None => {
            let user_id = Uuid::new_v4();
            sqlx::query(
                r#"
                INSERT INTO users (id, member_id, tenant_id, name, role)
                VALUES (?, ?, ?, ?, 'child')
                "#,
            )
            .bind(user_id)
            .bind(member_id)
            .bind(tenant_id)
            .bind(&name)
            .execute(&mut *tx)
            .await?;
            user_id
        }
    };

    activate_device(
        &mut tx,
        tenant_id,
        user_id,
        req.device_id,
        false,
        req.push_token,
        req.public_key,
    )
    .await?;
    emit_tx(
        &mut tx,
        &state.hub,
        tenant_id,
        "device.updated",
        None,
        Some(req.device_id),
        json!({"user_id": user_id, "approved": false}),
    )
    .await?;
    tx.commit().await?;

    issue_tokens(
        &state,
        AuthUser {
            user_id,
            member_id,
            tenant_id,
            device_id: Some(req.device_id),
            role: Role::Child,
        },
        None,
        name,
    )
    .await
}

pub(super) async fn refresh(
    State(state): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    Json(req): Json<RefreshRequest>,
) -> AppResult<Json<TokenResponse>> {
    // The token hash is the subject being attacked here; anything shared between callers
    // would let one device's refresh cadence lock every other device out.
    let token_hash = hash_refresh_token(&req.refresh_token);
    enforce_login_rate_limit(&state, &peer, &["refresh", &token_hash]).await?;

    // Select and revoke in one transaction with the row locked, so two concurrent
    // refreshes with the same token cannot both mint a new token family.
    let mut tx = state.pool.begin().await?;
    let row = sqlx::query(
        r#"
        SELECT u.id, u.member_id, u.tenant_id, u.name, u.email, u.role AS role, rt.device_id
        FROM refresh_tokens rt
        JOIN users u ON u.id = rt.user_id
        LEFT JOIN devices d ON d.id = rt.device_id
        WHERE rt.token_hash = ?
          AND rt.revoked_at IS NULL
          AND rt.expires_at > now()
          AND u.deleted_at IS NULL
          AND (rt.device_id IS NULL OR (d.approved = true AND d.active = true))
        FOR UPDATE
        "#,
    )
    .bind(&token_hash)
    .fetch_optional(&mut *tx)
    .await?
    .ok_or(AppError::Unauthorized)?;

    let revoked = sqlx::query(
        "UPDATE refresh_tokens SET revoked_at = now() WHERE token_hash = ? AND revoked_at IS NULL",
    )
    .bind(&token_hash)
    .execute(&mut *tx)
    .await?
    .rows_affected();
    if revoked == 0 {
        return Err(AppError::Unauthorized);
    }
    tx.commit().await?;

    let role_str: String = row.try_get("role")?;
    issue_tokens(
        &state,
        AuthUser {
            user_id: row.try_get("id")?,
            member_id: row.try_get("member_id")?,
            tenant_id: row.try_get("tenant_id")?,
            device_id: row.try_get("device_id")?,
            role: Role::try_from(role_str.as_str())?,
        },
        row.try_get("email")?,
        row.try_get("name")?,
    )
    .await
}

pub(super) async fn logout(
    State(state): State<AppState>,
    auth: AuthUser,
) -> AppResult<Json<Value>> {
    match auth.device_id {
        Some(device_id) => {
            sqlx::query(
                "UPDATE refresh_tokens SET revoked_at = now() WHERE user_id = ? AND device_id = ? AND revoked_at IS NULL",
            )
            .bind(auth.user_id)
            .bind(device_id)
            .execute(&state.pool)
            .await?;
        }
        None => {
            sqlx::query(
                "UPDATE refresh_tokens SET revoked_at = now() WHERE user_id = ? AND revoked_at IS NULL",
            )
            .bind(auth.user_id)
            .execute(&state.pool)
            .await?;
        }
    }
    if let Some(device_id) = auth.device_id {
        sqlx::query("UPDATE devices SET active = false, deactivation_reason = 'logout' WHERE id = ? AND user_id = ?")
            .bind(device_id)
            .bind(auth.user_id)
            .execute(&state.pool)
            .await?;
        emit(
            &state.pool,
            &state.hub,
            auth.tenant_id,
            "device.updated",
            None,
            Some(device_id),
            json!({"active": false, "reason": "logout"}),
        )
        .await?;
    }
    Ok(Json(json!({ "ok": true })))
}

pub(super) async fn me(
    State(state): State<AppState>,
    auth: AuthUser,
) -> AppResult<Json<UserResponse>> {
    let row = sqlx::query(
        "SELECT id, member_id, tenant_id, name, email, role AS role FROM users WHERE id = ?",
    )
    .bind(auth.user_id)
    .fetch_one(&state.pool)
    .await?;
    let role: String = row.try_get("role")?;

    Ok(Json(UserResponse {
        id: row.try_get("id")?,
        member_id: row.try_get("member_id")?,
        tenant_id: row.try_get("tenant_id")?,
        name: row.try_get("name")?,
        role: Role::try_from(role.as_str())?,
        email: row.try_get("email")?,
        device_id: auth.device_id,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    const IP: IpAddr = IpAddr::V4(std::net::Ipv4Addr::LOCALHOST);

    #[test]
    fn subject_bucket_is_scoped_to_the_subject() {
        let first = login_rate_limit_buckets(&["refresh", "hash-a"], IP, 5);
        let second = login_rate_limit_buckets(&["refresh", "hash-b"], IP, 5);
        assert_ne!(first[0].0, second[0].0);
        assert_eq!(first[0].1, 5);
    }

    #[test]
    fn ip_bucket_is_shared_and_looser_than_the_subject_bucket() {
        let first = login_rate_limit_buckets(&["email", "a@example.com"], IP, 5);
        let second = login_rate_limit_buckets(&["email", "b@example.com"], IP, 5);
        assert_eq!(first[1].0, second[1].0);
        assert_eq!(first[1].1, 5 * IP_ATTEMPT_MULTIPLIER);
    }

    #[test]
    fn ip_bucket_limit_saturates_instead_of_overflowing() {
        let buckets = login_rate_limit_buckets(&["register", "a@example.com"], IP, u32::MAX);
        assert_eq!(buckets[1].1, u32::MAX);
    }
}
