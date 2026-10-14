use std::net::SocketAddr;

use axum::{
    Router,
    body::Body,
    extract::ConnectInfo,
    http::{Request, StatusCode},
};
use chat_back::{
    app,
    auth::{AuthUser, Role, encode_access_token},
    config::Config,
    state::AppState,
};
use serde_json::{Value, json};
use tower::ServiceExt;
use uuid::Uuid;

pub const DATABASE_URL_ENV: &str = "DATABASE_URL_TEST";

pub struct TestServer {
    pub state: AppState,
    pub router: Router,
}

/// Returns None when the test database is not configured, so the suite stays green on a
/// machine without MySQL. Run these with `cargo test -- --include-ignored`.
pub async fn start() -> Option<TestServer> {
    let Ok(database_url) = std::env::var(DATABASE_URL_ENV) else {
        eprintln!("{DATABASE_URL_ENV} is not set; skipping");
        return None;
    };

    let config = Config {
        database_url,
        bind_addr: "127.0.0.1:0".to_string(),
        jwt_secret: "integration-test-secret-with-enough-length".to_string(),
        access_token_ttl_minutes: 15,
        refresh_token_ttl_days: 30,
        media_root: std::env::temp_dir().join("chat_back_tests"),
        media_public_base_url: "http://localhost:3000/static".to_string(),
        redis_url: None,
        expo_access_token: None,
        child_login_max_attempts: 5,
        child_login_window_secs: 60,
        allowed_origins: Vec::new(),
    };

    let state = AppState::connect(config)
        .await
        .expect("connect to the test database");
    sqlx::migrate!("./migrations")
        .run(&state.pool)
        .await
        .expect("run migrations on the test database");

    Some(TestServer {
        router: app::router(state.clone()),
        state,
    })
}

impl TestServer {
    pub async fn call(
        &self,
        method: &str,
        path: &str,
        token: Option<&str>,
        body: Option<Value>,
    ) -> (StatusCode, Value) {
        let mut builder = Request::builder().method(method).uri(path);
        if let Some(token) = token {
            builder = builder.header("authorization", format!("Bearer {token}"));
        }
        let mut request = match body {
            Some(body) => builder
                .header("content-type", "application/json")
                .body(Body::from(body.to_string())),
            None => builder.body(Body::empty()),
        }
        .expect("build request");
        // The unauthenticated handlers extract ConnectInfo, which oneshot does not populate.
        request
            .extensions_mut()
            .insert(ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 4321))));

        let response = self
            .router
            .clone()
            .oneshot(request)
            .await
            .expect("route request");
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read response body");
        let value = if bytes.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&bytes).unwrap_or(Value::Null)
        };
        (status, value)
    }

    pub async fn get(&self, path: &str, token: &str) -> (StatusCode, Value) {
        self.call("GET", path, Some(token), None).await
    }

    pub async fn post(&self, path: &str, token: &str, body: Value) -> (StatusCode, Value) {
        self.call("POST", path, Some(token), Some(body)).await
    }

    pub async fn patch(&self, path: &str, token: &str, body: Value) -> (StatusCode, Value) {
        self.call("PATCH", path, Some(token), Some(body)).await
    }

    pub async fn delete(&self, path: &str, token: &str) -> (StatusCode, Value) {
        self.call("DELETE", path, Some(token), None).await
    }
}

pub struct Actor {
    pub user_id: Uuid,
    pub member_id: Uuid,
    pub device_id: Uuid,
    pub token: String,
}

pub struct Family {
    pub tenant_id: Uuid,
    pub adult: Actor,
    pub child: Actor,
    pub direct_chat_id: Uuid,
}

impl Family {
    /// Removing the tenant cascades to every row seeded here, so tests can share a database.
    pub async fn cleanup(&self, server: &TestServer) {
        sqlx::query("UPDATE tenants SET owner_user_id = NULL WHERE id = ?")
            .bind(self.tenant_id)
            .execute(&server.state.pool)
            .await
            .ok();
        sqlx::query("DELETE FROM tenants WHERE id = ?")
            .bind(self.tenant_id)
            .execute(&server.state.pool)
            .await
            .ok();
    }
}

pub async fn seed_member(server: &TestServer, tenant_id: Uuid, role: Role) -> Actor {
    let member_id = Uuid::new_v4();
    let user_id = Uuid::new_v4();
    let device_id = Uuid::new_v4();
    let name = format!("member-{member_id}");

    sqlx::query("INSERT INTO members (id, tenant_id, name, role) VALUES (?, ?, ?, ?)")
        .bind(member_id)
        .bind(tenant_id)
        .bind(&name)
        .bind(role.as_db())
        .execute(&server.state.pool)
        .await
        .expect("insert member");

    sqlx::query(
        "INSERT INTO users (id, member_id, tenant_id, email, name, role) VALUES (?, ?, ?, ?, ?, ?)",
    )
    .bind(user_id)
    .bind(member_id)
    .bind(tenant_id)
    .bind(format!("{user_id}@example.test"))
    .bind(&name)
    .bind(role.as_db())
    .execute(&server.state.pool)
    .await
    .expect("insert user");

    sqlx::query(
        r#"
        INSERT INTO devices (id, tenant_id, user_id, approved, active, public_key)
        VALUES (?, ?, ?, true, true, ?)
        "#,
    )
    .bind(device_id)
    .bind(tenant_id)
    .bind(user_id)
    .bind(format!("public-key-{device_id}"))
    .execute(&server.state.pool)
    .await
    .expect("insert device");

    let token = encode_access_token(
        &server.state,
        &AuthUser {
            user_id,
            member_id,
            tenant_id,
            device_id: Some(device_id),
            role,
        },
    )
    .expect("encode access token");

    Actor {
        user_id,
        member_id,
        device_id,
        token,
    }
}

/// One adult, one child and the direct chat between them. Read state is seeded only for the
/// adult, so tests can exercise the missing-row path of the read receipt endpoint.
pub async fn seed_family(server: &TestServer) -> Family {
    let tenant_id = Uuid::new_v4();
    sqlx::query("INSERT INTO tenants (id, name) VALUES (?, ?)")
        .bind(tenant_id)
        .bind("Família de teste")
        .execute(&server.state.pool)
        .await
        .expect("insert tenant");

    let adult = seed_member(server, tenant_id, Role::Adult).await;
    let child = seed_member(server, tenant_id, Role::Child).await;

    sqlx::query("UPDATE tenants SET owner_user_id = ? WHERE id = ?")
        .bind(adult.user_id)
        .bind(tenant_id)
        .execute(&server.state.pool)
        .await
        .expect("set tenant owner");

    let direct_chat_id = Uuid::new_v4();
    sqlx::query("INSERT INTO chats (id, tenant_id, is_group, name) VALUES (?, ?, false, '')")
        .bind(direct_chat_id)
        .bind(tenant_id)
        .execute(&server.state.pool)
        .await
        .expect("insert chat");
    for member_id in [adult.member_id, child.member_id] {
        sqlx::query("INSERT INTO chat_participants (chat_id, member_id) VALUES (?, ?)")
            .bind(direct_chat_id)
            .bind(member_id)
            .execute(&server.state.pool)
            .await
            .expect("insert chat participant");
    }
    sqlx::query("INSERT INTO chat_read_state (chat_id, member_id) VALUES (?, ?)")
        .bind(direct_chat_id)
        .bind(adult.member_id)
        .execute(&server.state.pool)
        .await
        .expect("insert chat read state");

    Family {
        tenant_id,
        adult,
        child,
        direct_chat_id,
    }
}

pub fn text_message(id: Uuid) -> Value {
    json!({
        "id": id,
        "type": "text",
        "ciphertext": "Y2lwaGVydGV4dA==",
        "iv": "aXY=",
        "enc_version": 1,
    })
}

pub fn find_message(messages: &Value, id: Uuid) -> Option<&Value> {
    messages
        .as_array()?
        .iter()
        .find(|item| item.get("id").and_then(Value::as_str) == Some(&id.to_string()))
}

pub fn encode_cursor(timestamp: &str) -> String {
    timestamp.replace('+', "%2B")
}
