use std::{env, path::PathBuf};

use anyhow::{Context, bail};

const REJECTED_JWT_SECRETS: [&str; 2] = ["dev-secret-change-me", "change-me"];

#[derive(Clone, Debug)]
pub struct Config {
    pub database_url: String,
    pub bind_addr: String,
    pub jwt_secret: String,
    pub access_token_ttl_minutes: i64,
    pub refresh_token_ttl_days: i64,
    pub media_root: PathBuf,
    pub media_public_base_url: String,
    pub redis_url: Option<String>,
    pub expo_access_token: Option<String>,
    pub child_login_max_attempts: u32,
    pub child_login_window_secs: u64,
    pub allowed_origins: Vec<String>,
}

impl Config {
    pub fn from_env() -> anyhow::Result<Self> {
        let database_url = env::var("DATABASE_URL").context("DATABASE_URL must be set")?;
        let jwt_secret = env::var("JWT_SECRET").context("JWT_SECRET must be set")?;
        if REJECTED_JWT_SECRETS.contains(&jwt_secret.as_str()) || jwt_secret.len() < 32 {
            bail!("JWT_SECRET must be a private value with at least 32 characters");
        }

        Ok(Self {
            database_url,
            bind_addr: env::var("BIND_ADDR").unwrap_or_else(|_| "127.0.0.1:3000".to_string()),
            jwt_secret,
            access_token_ttl_minutes: env::var("ACCESS_TOKEN_TTL_MINUTES")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(15),
            refresh_token_ttl_days: env::var("REFRESH_TOKEN_TTL_DAYS")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(30),
            media_root: env::var("MEDIA_ROOT")
                .map(PathBuf::from)
                .unwrap_or_else(|_| PathBuf::from("./media")),
            media_public_base_url: env::var("MEDIA_PUBLIC_BASE_URL")
                .unwrap_or_else(|_| "http://localhost:3000/static".to_string()),
            redis_url: non_empty("REDIS_URL"),
            expo_access_token: non_empty("EXPO_ACCESS_TOKEN"),
            child_login_max_attempts: env::var("CHILD_LOGIN_MAX_ATTEMPTS")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(5),
            child_login_window_secs: env::var("CHILD_LOGIN_WINDOW_SECS")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(60),
            allowed_origins: non_empty("ALLOWED_ORIGINS")
                .map(|value| {
                    value
                        .split(',')
                        .map(|origin| origin.trim().to_string())
                        .filter(|origin| !origin.is_empty())
                        .collect()
                })
                .unwrap_or_default(),
        })
    }
}

fn non_empty(key: &str) -> Option<String> {
    env::var(key)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}
