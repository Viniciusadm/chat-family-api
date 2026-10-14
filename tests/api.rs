mod common;

use axum::http::StatusCode;
use serde_json::{Value, json};
use uuid::Uuid;

use common::{Family, TestServer, encode_cursor, find_message, seed_family, seed_member, text_message};

macro_rules! server {
    () => {
        match common::start().await {
            Some(server) => server,
            None => return,
        }
    };
}

async fn message_field(
    server: &TestServer,
    family: &Family,
    message_id: Uuid,
    field: &str,
) -> Value {
    let (status, body) = server
        .get(
            &format!("/chats/{}/messages", family.direct_chat_id),
            &family.adult.token,
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    find_message(&body, message_id)
        .and_then(|message| message.get(field))
        .cloned()
        .unwrap_or(Value::Null)
}

#[tokio::test]
#[ignore = "requires DATABASE_URL_TEST"]
async fn changed_after_returns_edits_and_deletions_the_history_cursor_misses() {
    let server = server!();
    let family = seed_family(&server).await;
    let message_id = Uuid::new_v4();

    let (status, _) = server
        .post(
            &format!("/chats/{}/messages", family.direct_chat_id),
            &family.adult.token,
            text_message(message_id),
        )
        .await;
    assert_eq!(status, StatusCode::OK);

    let created_at = message_field(&server, &family, message_id, "created_at")
        .await
        .as_str()
        .expect("created_at")
        .to_string();
    let cursor = encode_cursor(&created_at);

    let (_, after_history) = server
        .get(
            &format!(
                "/chats/{}/messages?after={cursor}",
                family.direct_chat_id
            ),
            &family.adult.token,
        )
        .await;
    assert!(
        find_message(&after_history, message_id).is_none(),
        "the history cursor must not return a message it already synced"
    );

    let (status, _) = server
        .patch(
            &format!(
                "/chats/{}/messages/{message_id}",
                family.direct_chat_id
            ),
            &family.adult.token,
            json!({
                "id": message_id,
                "type": "text",
                "ciphertext": "ZWRpdGVk",
                "iv": "aXYy",
                "enc_version": 1,
            }),
        )
        .await;
    assert_eq!(status, StatusCode::OK);

    let (_, still_hidden) = server
        .get(
            &format!(
                "/chats/{}/messages?after={cursor}",
                family.direct_chat_id
            ),
            &family.adult.token,
        )
        .await;
    assert!(
        find_message(&still_hidden, message_id).is_none(),
        "editing must not move created_at"
    );

    let (_, changed) = server
        .get(
            &format!(
                "/chats/{}/messages?changed_after={cursor}",
                family.direct_chat_id
            ),
            &family.adult.token,
        )
        .await;
    let edited = find_message(&changed, message_id).expect("edit is visible to changed_after");
    assert_eq!(edited["ciphertext"], json!("ZWRpdGVk"));
    assert!(!edited["edited_at"].is_null());

    let (status, _) = server
        .delete(
            &format!(
                "/chats/{}/messages/{message_id}",
                family.direct_chat_id
            ),
            &family.adult.token,
        )
        .await;
    assert_eq!(status, StatusCode::OK);

    let (_, deleted) = server
        .get(
            &format!(
                "/chats/{}/messages?changed_after={cursor}",
                family.direct_chat_id
            ),
            &family.adult.token,
        )
        .await;
    let removed = find_message(&deleted, message_id).expect("deletion is visible to changed_after");
    assert_eq!(removed["is_deleted"], json!(true));
    assert!(removed["ciphertext"].is_null());

    family.cleanup(&server).await;
}

#[tokio::test]
#[ignore = "requires DATABASE_URL_TEST"]
async fn children_cannot_administer_chats() {
    let server = server!();
    let family = seed_family(&server).await;
    let chat = format!("/chats/{}", family.direct_chat_id);

    let (status, _) = server
        .post(
            "/chats",
            &family.child.token,
            json!({
                "name": "Grupo da criança",
                "is_group": true,
                "participant_ids": [family.adult.member_id, family.child.member_id],
            }),
        )
        .await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    let (status, _) = server
        .patch(&chat, &family.child.token, json!({ "name": "renomeado" }))
        .await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    let (status, _) = server
        .delete(&format!("{chat}/photo"), &family.child.token)
        .await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    let (status, _) = server.delete(&chat, &family.child.token).await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    let (status, _) = server
        .patch(&chat, &family.adult.token, json!({ "name": "Casa" }))
        .await;
    assert_eq!(status, StatusCode::OK);

    family.cleanup(&server).await;
}

#[tokio::test]
#[ignore = "requires DATABASE_URL_TEST"]
async fn marking_a_chat_read_creates_the_missing_read_state_row() {
    let server = server!();
    let family = seed_family(&server).await;

    let (status, _) = server
        .post(
            &format!("/chats/{}/read", family.direct_chat_id),
            &family.child.token,
            json!({}),
        )
        .await;
    assert_eq!(status, StatusCode::OK);

    let stored: Option<i32> = sqlx::query_scalar(
        "SELECT unread_count FROM chat_read_state WHERE chat_id = ? AND member_id = ?",
    )
    .bind(family.direct_chat_id)
    .bind(family.child.member_id)
    .fetch_optional(&server.state.pool)
    .await
    .expect("query read state");
    assert_eq!(stored, Some(0));

    let (_, chat) = server
        .get(
            &format!("/chats/{}", family.direct_chat_id),
            &family.child.token,
        )
        .await;
    assert!(
        chat["read_up_to"][family.child.member_id.to_string()].is_string(),
        "the read receipt has to reach the chat payload"
    );

    family.cleanup(&server).await;
}

#[tokio::test]
#[ignore = "requires DATABASE_URL_TEST"]
async fn a_deleted_member_disappears_from_listings_and_key_recipients() {
    let server = server!();
    let family = seed_family(&server).await;
    let leaving = seed_member(&server, family.tenant_id, chat_back::auth::Role::Child).await;

    let (status, group) = server
        .post(
            "/chats",
            &family.adult.token,
            json!({
                "name": "Família",
                "is_group": true,
                "participant_ids": [
                    family.adult.member_id,
                    family.child.member_id,
                    leaving.member_id,
                ],
            }),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    let group_id = group["id"].as_str().expect("group id").to_string();

    let (_, recipients) = server
        .get(&format!("/chats/{group_id}/key-recipients"), &family.adult.token)
        .await;
    assert_eq!(recipients.as_array().expect("recipients").len(), 3);

    let (status, _) = server
        .delete(&format!("/members/{}", leaving.member_id), &family.adult.token)
        .await;
    assert_eq!(status, StatusCode::OK);

    let (_, members) = server.get("/members", &family.adult.token).await;
    let ids: Vec<&str> = members
        .as_array()
        .expect("members")
        .iter()
        .filter_map(|member| member["id"].as_str())
        .collect();
    assert!(!ids.contains(&leaving.member_id.to_string().as_str()));

    let (_, recipients) = server
        .get(&format!("/chats/{group_id}/key-recipients"), &family.adult.token)
        .await;
    let devices: Vec<&str> = recipients
        .as_array()
        .expect("recipients")
        .iter()
        .filter_map(|device| device["id"].as_str())
        .collect();
    assert_eq!(devices.len(), 2);
    assert!(!devices.contains(&leaving.device_id.to_string().as_str()));

    let (status, _) = server
        .post(
            "/chats",
            &family.adult.token,
            json!({
                "name": "Outro grupo",
                "is_group": true,
                "participant_ids": [
                    family.adult.member_id,
                    family.child.member_id,
                    leaving.member_id,
                ],
            }),
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    family.cleanup(&server).await;
}

#[tokio::test]
#[ignore = "requires DATABASE_URL_TEST"]
async fn a_second_direct_chat_for_the_same_pair_is_not_created() {
    let server = server!();
    let family = seed_family(&server).await;

    let (status, body) = server
        .post(
            "/chats",
            &family.adult.token,
            json!({
                "is_group": false,
                "participant_ids": [family.adult.member_id, family.child.member_id],
            }),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body["id"].as_str(),
        Some(family.direct_chat_id.to_string().as_str()),
        "the existing direct chat has to be reused"
    );

    let (status, _) = server
        .post(
            "/chats",
            &family.adult.token,
            json!({
                "is_group": false,
                "participant_ids": [family.child.member_id],
            }),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "the caller is added to the pair");

    family.cleanup(&server).await;
}

#[tokio::test]
#[ignore = "requires DATABASE_URL_TEST"]
async fn a_sixteen_character_iv_comes_back_unchanged() {
    let server = server!();
    let family = seed_family(&server).await;
    let message_id = Uuid::new_v4();
    let iv = "AAECAwQFBgcICQoL";

    let mut message = text_message(message_id);
    message["iv"] = json!(iv);
    let (status, _) = server
        .post(
            &format!("/chats/{}/messages", family.direct_chat_id),
            &family.adult.token,
            message,
        )
        .await;
    assert_eq!(status, StatusCode::OK);

    assert_eq!(
        message_field(&server, &family, message_id, "iv").await,
        json!(iv),
        "a 12 byte IV is 16 base64 characters and must not be read as a UUID"
    );

    family.cleanup(&server).await;
}

#[tokio::test]
#[ignore = "requires DATABASE_URL_TEST"]
async fn only_adults_see_login_codes() {
    let server = server!();
    let family = seed_family(&server).await;
    sqlx::query("UPDATE members SET login_code = 'SECRETCODE' WHERE id = ?")
        .bind(family.child.member_id)
        .execute(&server.state.pool)
        .await
        .expect("set login code");

    let login_code_seen_by = |members: &Value| {
        members
            .as_array()
            .expect("members")
            .iter()
            .find(|member| member["id"].as_str() == Some(&family.child.member_id.to_string()))
            .map(|member| member["login_code"].clone())
            .unwrap_or(Value::Null)
    };

    let (_, members) = server.get("/members", &family.adult.token).await;
    assert_eq!(login_code_seen_by(&members), json!("SECRETCODE"));

    let (_, members) = server.get("/members", &family.child.token).await;
    assert_eq!(login_code_seen_by(&members), Value::Null);

    family.cleanup(&server).await;
}

#[tokio::test]
#[ignore = "requires DATABASE_URL_TEST"]
async fn only_the_first_device_may_create_a_chat_key() {
    let server = server!();
    let family = seed_family(&server).await;
    let path = format!("/chats/{}/key-claim", family.direct_chat_id);

    let (status, body) = server.post(&path, &family.adult.token, json!({})).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["granted"], json!(true));

    let (_, body) = server.post(&path, &family.child.token, json!({})).await;
    assert_eq!(body["granted"], json!(false), "the key is already being created elsewhere");

    let (_, body) = server.post(&path, &family.adult.token, json!({})).await;
    assert_eq!(body["granted"], json!(true), "the claiming device can retry");

    family.cleanup(&server).await;
}

#[tokio::test]
#[ignore = "requires DATABASE_URL_TEST"]
async fn a_device_waiting_for_approval_can_refresh_its_session() {
    let server = server!();
    let family = seed_family(&server).await;
    sqlx::query("UPDATE devices SET approved = false, active = false WHERE id = ?")
        .bind(family.child.device_id)
        .execute(&server.state.pool)
        .await
        .expect("mark device pending");

    let refresh_token = Uuid::new_v4().to_string();
    sqlx::query(
        "INSERT INTO refresh_tokens (id, user_id, device_id, token_hash, expires_at) VALUES (?, ?, ?, ?, now() + INTERVAL 1 DAY)",
    )
    .bind(Uuid::new_v4())
    .bind(family.child.user_id)
    .bind(family.child.device_id)
    .bind(chat_back::auth::hash_refresh_token(&refresh_token))
    .execute(&server.state.pool)
    .await
    .expect("insert refresh token");

    let (status, body) = server
        .call(
            "POST",
            "/auth/refresh",
            None,
            Some(json!({ "refresh_token": refresh_token })),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    let rotated = body["refresh_token"].as_str().expect("rotated token").to_string();

    sqlx::query("UPDATE devices SET deactivation_reason = 'admin_removed' WHERE id = ?")
        .bind(family.child.device_id)
        .execute(&server.state.pool)
        .await
        .expect("remove device");
    let (status, _) = server
        .call(
            "POST",
            "/auth/refresh",
            None,
            Some(json!({ "refresh_token": rotated })),
        )
        .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED, "a removed device is not pending");

    family.cleanup(&server).await;
}

fn key_share_body(ciphertext: &str) -> Value {
    json!({
        "ephemeral_public_key": "ZXBoZW1lcmFs",
        "iv": "AAECAwQFBgcICQoL",
        "ciphertext": ciphertext,
    })
}

async fn put_chat_key_share(
    server: &TestServer,
    token: &str,
    chat_id: Uuid,
    device_id: Uuid,
    ciphertext: &str,
) -> (StatusCode, Value) {
    server
        .call(
            "PUT",
            &format!("/chats/{chat_id}/devices/{device_id}/key-shares"),
            Some(token),
            Some(key_share_body(ciphertext)),
        )
        .await
}

async fn shared_ciphertexts(server: &TestServer, actor_token: &str, device_id: Uuid) -> Vec<String> {
    let (status, shares) = server
        .get(&format!("/devices/{device_id}/key-shares"), actor_token)
        .await;
    assert_eq!(status, StatusCode::OK);
    shares
        .as_array()
        .expect("shares")
        .iter()
        .filter_map(|share| share["ciphertext"].as_str().map(str::to_string))
        .collect()
}

#[tokio::test]
#[ignore = "requires DATABASE_URL_TEST"]
async fn a_key_share_wrapped_by_another_member_is_never_replaced() {
    let server = server!();
    let family = seed_family(&server).await;
    let chat_id = family.direct_chat_id;

    let (status, _) =
        put_chat_key_share(&server, &family.adult.token, chat_id, family.child.device_id, "b3JpZ2luYWw=").await;
    assert_eq!(status, StatusCode::OK);

    let (status, body) =
        put_chat_key_share(&server, &family.child.token, chat_id, family.child.device_id, "cGxhbnRlZA==").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["kept_existing"], json!(true));

    assert_eq!(
        shared_ciphertexts(&server, &family.child.token, family.child.device_id).await,
        vec!["b3JpZ2luYWw=".to_string()]
    );

    family.cleanup(&server).await;
}

#[tokio::test]
#[ignore = "requires DATABASE_URL_TEST"]
async fn key_shares_only_target_active_devices_of_participants() {
    let server = server!();
    let family = seed_family(&server).await;
    let outsider = seed_member(&server, family.tenant_id, chat_back::auth::Role::Child).await;

    let (status, _) = put_chat_key_share(
        &server,
        &family.adult.token,
        family.direct_chat_id,
        outsider.device_id,
        "c2VjcmV0",
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "the outsider is not in the chat");

    let (status, _) = server
        .call(
            "PUT",
            &format!("/devices/{}/key-shares/{}", outsider.device_id, family.direct_chat_id),
            Some(&family.adult.token),
            Some(key_share_body("c2VjcmV0")),
        )
        .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "the admin route follows the same rule");

    family.cleanup(&server).await;
}

#[tokio::test]
#[ignore = "requires DATABASE_URL_TEST"]
async fn a_member_removed_from_a_group_loses_its_key_shares() {
    let server = server!();
    let family = seed_family(&server).await;
    let third = seed_member(&server, family.tenant_id, chat_back::auth::Role::Child).await;
    let leaving = seed_member(&server, family.tenant_id, chat_back::auth::Role::Child).await;
    let everyone = [
        family.adult.member_id,
        family.child.member_id,
        third.member_id,
        leaving.member_id,
    ];

    let (status, group) = server
        .post(
            "/chats",
            &family.adult.token,
            json!({ "name": "Família", "is_group": true, "participant_ids": everyone }),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    let group_id: Uuid = group["id"].as_str().expect("group id").parse().expect("uuid");

    let (status, _) =
        put_chat_key_share(&server, &family.adult.token, group_id, leaving.device_id, "a2V5").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(shared_ciphertexts(&server, &leaving.token, leaving.device_id).await.len(), 1);

    let (status, _) = server
        .patch(
            &format!("/chats/{group_id}"),
            &family.adult.token,
            json!({ "participant_ids": &everyone[..3] }),
        )
        .await;
    assert_eq!(status, StatusCode::OK);

    let remaining: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM key_shares WHERE device_id = ?")
        .bind(leaving.device_id)
        .fetch_one(&server.state.pool)
        .await
        .expect("count shares");
    assert_eq!(remaining, 0);

    family.cleanup(&server).await;
}

#[tokio::test]
#[ignore = "requires DATABASE_URL_TEST"]
async fn reactions_must_target_a_message_of_the_same_chat() {
    let server = server!();
    let family = seed_family(&server).await;
    let message_id = Uuid::new_v4();
    let (status, _) = server
        .post(
            &format!("/chats/{}/messages", family.direct_chat_id),
            &family.adult.token,
            text_message(message_id),
        )
        .await;
    assert_eq!(status, StatusCode::OK);

    let reaction_path = |message: Uuid| {
        format!("/chats/{}/messages/{message}/reaction", family.direct_chat_id)
    };
    let react = |message: Uuid, emoji: &'static str| {
        let path = reaction_path(message);
        let token = family.child.token.clone();
        let server = &server;
        async move {
            server
                .call("PUT", &path, Some(&token), Some(json!({ "emoji": emoji })))
                .await
                .0
        }
    };

    assert_eq!(react(message_id, "👍").await, StatusCode::OK);
    assert_eq!(react(Uuid::new_v4(), "👍").await, StatusCode::NOT_FOUND);
    assert_eq!(react(message_id, "  ").await, StatusCode::BAD_REQUEST);
    assert_eq!(
        react(message_id, "this is definitely not an emoji").await,
        StatusCode::BAD_REQUEST
    );

    family.cleanup(&server).await;
}

#[tokio::test]
#[ignore = "requires DATABASE_URL_TEST"]
async fn the_last_adult_and_the_caller_cannot_be_deleted() {
    let server = server!();
    let family = seed_family(&server).await;

    let (status, _) = server
        .delete(&format!("/members/{}", family.adult.member_id), &family.adult.token)
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "deleting yourself");

    let other_adult = seed_member(&server, family.tenant_id, chat_back::auth::Role::Adult).await;
    let (status, _) = server
        .delete(&format!("/members/{}", other_adult.member_id), &family.adult.token)
        .await;
    assert_eq!(status, StatusCode::OK, "another adult remains");

    let (status, _) = server
        .delete(&format!("/members/{}", Uuid::new_v4()), &family.adult.token)
        .await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    let reused_email: Option<String> = sqlx::query_scalar("SELECT email FROM users WHERE id = ?")
        .bind(other_adult.user_id)
        .fetch_one(&server.state.pool)
        .await
        .expect("deleted user");
    assert_eq!(reused_email, None, "the e-mail is released for a new account");

    family.cleanup(&server).await;
}

#[tokio::test]
#[ignore = "requires DATABASE_URL_TEST"]
async fn the_before_cursor_pages_backwards_from_the_newest_message() {
    let server = server!();
    let family = seed_family(&server).await;
    let mut ids = Vec::new();
    for _ in 0..3 {
        let message_id = Uuid::new_v4();
        let (status, _) = server
            .post(
                &format!("/chats/{}/messages", family.direct_chat_id),
                &family.adult.token,
                text_message(message_id),
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        ids.push(message_id.to_string());
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }

    let page_ids = |body: &Value| -> Vec<String> {
        body.as_array()
            .expect("messages")
            .iter()
            .filter_map(|message| message["id"].as_str().map(str::to_string))
            .collect()
    };

    let far_future = encode_cursor("2999-01-01T00:00:00+00:00");
    let (status, newest) = server
        .get(
            &format!("/chats/{}/messages?before={far_future}&limit=2", family.direct_chat_id),
            &family.adult.token,
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(page_ids(&newest), ids[1..].to_vec(), "newest page, oldest first");

    let cursor = encode_cursor(newest[0]["created_at"].as_str().expect("created_at"));
    let (_, older) = server
        .get(
            &format!("/chats/{}/messages?before={cursor}&limit=2", family.direct_chat_id),
            &family.adult.token,
        )
        .await;
    assert_eq!(page_ids(&older), ids[..1].to_vec());

    family.cleanup(&server).await;
}

#[tokio::test]
#[ignore = "requires DATABASE_URL_TEST"]
async fn listing_reactions_without_a_cursor_returns_all_of_them() {
    let server = server!();
    let family = seed_family(&server).await;
    let mut message_ids = Vec::new();
    for _ in 0..3 {
        let message_id = Uuid::new_v4();
        server
            .post(
                &format!("/chats/{}/messages", family.direct_chat_id),
                &family.adult.token,
                text_message(message_id),
            )
            .await;
        let (status, _) = server
            .call(
                "PUT",
                &format!("/chats/{}/messages/{message_id}/reaction", family.direct_chat_id),
                Some(&family.child.token),
                Some(json!({ "emoji": "❤️" })),
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        message_ids.push(message_id);
    }

    let (status, reactions) = server
        .get(
            &format!("/chats/{}/reactions?limit=1", family.direct_chat_id),
            &family.adult.token,
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        reactions.as_array().expect("reactions").len(),
        3,
        "a full listing must not be truncated, the client prunes what is missing"
    );

    family.cleanup(&server).await;
}

fn unique_email() -> String {
    format!("adult-{}@example.test", Uuid::new_v4())
}

fn tenant_of(token_response: &Value) -> Uuid {
    token_response["user"]["tenant_id"]
        .as_str()
        .expect("tenant id")
        .parse()
        .expect("tenant uuid")
}

async fn register(server: &TestServer, email: &str, password: &str) -> Value {
    let (status, body) = server
        .call(
            "POST",
            "/auth/register",
            None,
            Some(json!({
                "email": email,
                "password": password,
                "name": "Ana",
                "device_id": Uuid::new_v4(),
            })),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    body
}

async fn refresh(server: &TestServer, refresh_token: &str) -> (StatusCode, Value) {
    server
        .call(
            "POST",
            "/auth/refresh",
            None,
            Some(json!({ "refresh_token": refresh_token })),
        )
        .await
}

fn assert_denied(status: StatusCode, what: &str) {
    assert!(
        status == StatusCode::FORBIDDEN || status == StatusCode::NOT_FOUND,
        "{what} must be denied, got {status}"
    );
}

fn ids_of(list: &Value) -> Vec<String> {
    list.as_array()
        .expect("array")
        .iter()
        .filter_map(|item| item["id"].as_str().map(str::to_string))
        .collect()
}

#[tokio::test]
#[ignore = "requires DATABASE_URL_TEST"]
async fn a_registered_adult_can_log_in_and_read_its_profile() {
    let server = server!();
    let email = unique_email();
    let password = "correct horse battery";
    let registered = register(&server, &email, password).await;
    let tenant_id = tenant_of(&registered);

    let device_id = Uuid::new_v4();
    let (status, _) = server
        .call(
            "POST",
            "/auth/login",
            None,
            Some(json!({ "email": email, "password": "wrong password", "device_id": device_id })),
        )
        .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    let (status, logged_in) = server
        .call(
            "POST",
            "/auth/login",
            None,
            Some(json!({ "email": email.to_uppercase(), "password": password, "device_id": device_id })),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    let token = logged_in["access_token"].as_str().expect("access token");

    let (status, me) = server.get("/auth/me", token).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(me["id"], registered["user"]["id"]);
    assert_eq!(me["member_id"], registered["user"]["member_id"]);
    assert_eq!(me["tenant_id"], json!(tenant_id));
    assert_eq!(me["role"], json!("adult"));
    assert_eq!(me["name"], json!("Ana"));
    assert_eq!(me["email"], json!(email));
    assert_eq!(me["device_id"], json!(device_id));

    common::delete_tenant(&server, tenant_id).await;
}

#[tokio::test]
#[ignore = "requires DATABASE_URL_TEST"]
async fn refresh_tokens_rotate_and_logout_revokes_the_session() {
    let server = server!();
    let registered = register(&server, &unique_email(), "correct horse battery").await;
    let tenant_id = tenant_of(&registered);
    let first = registered["refresh_token"].as_str().expect("refresh token");

    let (status, rotated) = refresh(&server, first).await;
    assert_eq!(status, StatusCode::OK);
    let second = rotated["refresh_token"].as_str().expect("rotated token");
    assert_ne!(first, second);

    let (status, _) = refresh(&server, first).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED, "a used refresh token is rejected");

    let (status, latest) = refresh(&server, second).await;
    assert_eq!(status, StatusCode::OK);
    let third = latest["refresh_token"].as_str().expect("refresh token");
    let access = latest["access_token"].as_str().expect("access token");
    assert_eq!(server.get("/auth/me", access).await.0, StatusCode::OK);

    let (status, _) = server.post("/auth/logout", access, json!({})).await;
    assert_eq!(status, StatusCode::OK);

    let (status, _) = refresh(&server, third).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED, "logout revokes the refresh token");
    let (status, _) = server.get("/auth/me", access).await;
    assert_eq!(status, StatusCode::FORBIDDEN, "logout deactivates the device");

    common::delete_tenant(&server, tenant_id).await;
}

#[tokio::test]
#[ignore = "requires DATABASE_URL_TEST"]
async fn a_child_login_code_creates_a_pending_device() {
    let server = server!();
    let family = seed_family(&server).await;

    let (status, created) = server
        .post(
            "/members",
            &family.adult.token,
            json!({ "name": "Bia", "role": "child" }),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    let member_id = created["id"].as_str().expect("member id").to_string();
    let code = created["login_code"].as_str().expect("login code").to_string();

    let device_id = Uuid::new_v4();
    let (status, _) = server
        .call(
            "POST",
            "/auth/child-login",
            None,
            Some(json!({ "code": "WRONGCODE1", "device_id": device_id })),
        )
        .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    let (status, logged_in) = server
        .call(
            "POST",
            "/auth/child-login",
            None,
            Some(json!({ "code": code, "device_id": device_id, "public_key": "cHVibGlj" })),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(logged_in["user"]["role"], json!("child"));
    assert_eq!(logged_in["user"]["member_id"], json!(member_id));
    assert_eq!(logged_in["user"]["device_id"], json!(device_id));
    assert!(logged_in["refresh_token"].is_string());

    let (approved, active): (bool, bool) =
        sqlx::query_as("SELECT approved, active FROM devices WHERE id = ?")
            .bind(device_id)
            .fetch_one(&server.state.pool)
            .await
            .expect("device row");
    assert!(!approved && !active, "a child device waits for an adult");

    let (status, pending) = server.get("/admin/devices/pending", &family.adult.token).await;
    assert_eq!(status, StatusCode::OK);
    let device = pending
        .as_array()
        .expect("pending devices")
        .iter()
        .find(|device| device["id"] == json!(device_id))
        .expect("the new device is pending");
    assert_eq!(device["member_id"], json!(member_id));
    assert_eq!(device["name"], json!("Bia"));
    assert_eq!(device["public_key"], json!("cHVibGlj"));
    assert!(device["user_id"].is_string());
    assert!(device["created_at"].is_string());

    let (status, _) = server.get("/admin/devices/pending", &family.child.token).await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    family.cleanup(&server).await;
}

#[tokio::test]
#[ignore = "requires DATABASE_URL_TEST"]
async fn a_member_of_another_tenant_cannot_reach_a_chat() {
    let server = server!();
    let family = seed_family(&server).await;
    let other = seed_family(&server).await;
    let chat_id = family.direct_chat_id;
    let message_id = Uuid::new_v4();

    let (status, _) = server
        .post(
            &format!("/chats/{chat_id}/messages"),
            &family.adult.token,
            text_message(message_id),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = server
        .call(
            "PUT",
            &format!("/chats/{chat_id}/messages/{message_id}/reaction"),
            Some(&family.child.token),
            Some(json!({ "emoji": "👍" })),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) =
        put_chat_key_share(&server, &family.adult.token, chat_id, family.child.device_id, "a2V5").await;
    assert_eq!(status, StatusCode::OK);

    let (status, messages) = server
        .get(&format!("/chats/{chat_id}/messages"), &family.child.token)
        .await;
    assert_eq!(status, StatusCode::OK);
    let message = find_message(&messages, message_id).expect("message");
    assert_eq!(message["enc_version"], json!(1), "integer columns stay numbers");
    assert_eq!(message["is_deleted"], json!(false));
    assert_eq!(message["type"], json!("text"));

    let intruder = &other.adult.token;
    let (status, _) = server.get(&format!("/chats/{chat_id}"), intruder).await;
    assert_denied(status, "reading the chat");
    let (status, _) = server.get(&format!("/chats/{chat_id}/messages"), intruder).await;
    assert_denied(status, "listing messages");
    let (status, _) = server
        .post(
            &format!("/chats/{chat_id}/messages"),
            intruder,
            text_message(Uuid::new_v4()),
        )
        .await;
    assert_denied(status, "posting a message");
    let (status, _) = server.get(&format!("/chats/{chat_id}/reactions"), intruder).await;
    assert_denied(status, "listing reactions");
    let (status, _) = server
        .call(
            "PUT",
            &format!("/chats/{chat_id}/messages/{message_id}/reaction"),
            Some(intruder),
            Some(json!({ "emoji": "👎" })),
        )
        .await;
    assert_denied(status, "reacting");
    let (status, _) = server
        .get(&format!("/chats/{chat_id}/key-recipients"), intruder)
        .await;
    assert_denied(status, "listing key recipients");
    let (status, _) =
        put_chat_key_share(&server, intruder, chat_id, other.adult.device_id, "cGxhbnRlZA==").await;
    assert_denied(status, "planting a key share");

    let (status, shares) = server
        .get(&format!("/devices/{}/key-shares", family.child.device_id), intruder)
        .await;
    assert!(
        status != StatusCode::OK || shares.as_array().is_some_and(Vec::is_empty),
        "another tenant's key shares must not leak, got {status} {shares}"
    );

    let (_, chats) = server.get("/chats", intruder).await;
    assert!(!ids_of(&chats).contains(&chat_id.to_string()));

    let (_, members) = server.get("/members", intruder).await;
    let listed = ids_of(&members);
    assert!(!listed.contains(&family.adult.member_id.to_string()));
    assert!(!listed.contains(&family.child.member_id.to_string()));
    assert!(listed.contains(&other.adult.member_id.to_string()));

    family.cleanup(&server).await;
    other.cleanup(&server).await;
}

fn small_png() -> Vec<u8> {
    let image = image::RgbImage::from_pixel(4, 3, image::Rgb([200, 30, 30]));
    let mut bytes = std::io::Cursor::new(Vec::new());
    image
        .write_to(&mut bytes, image::ImageFormat::Png)
        .expect("encode png");
    bytes.into_inner()
}

#[tokio::test]
#[ignore = "requires DATABASE_URL_TEST"]
async fn chat_media_is_only_served_to_participants() {
    let server = server!();
    let family = seed_family(&server).await;
    let other = seed_family(&server).await;
    let outsider = seed_member(&server, family.tenant_id, chat_back::auth::Role::Child).await;
    let chat_id = family.direct_chat_id;
    let message_id = Uuid::new_v4();

    let (status, _) = server
        .post(
            &format!("/chats/{chat_id}/messages"),
            &family.adult.token,
            json!({ "id": message_id, "type": "image" }),
        )
        .await;
    assert_eq!(status, StatusCode::OK);

    let (status, uploaded) = server
        .upload(
            &format!("/chats/{chat_id}/messages/{message_id}/image"),
            &family.adult.token,
            "photo.png",
            "image/png",
            &small_png(),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{uploaded}");
    assert_eq!(uploaded["width"], json!(4));
    assert_eq!(uploaded["height"], json!(3));
    let storage_path = uploaded["path"].as_str().expect("storage path").to_string();
    let static_path = format!("/static/{storage_path}");

    let (_, messages) = server
        .get(&format!("/chats/{chat_id}/messages"), &family.child.token)
        .await;
    let message = find_message(&messages, message_id).expect("image message");
    assert_eq!(message["image_width"], json!(4));
    assert_eq!(message["image_height"], json!(3));
    assert_eq!(message["image_url"], uploaded["url"]);

    let (status, bytes) = server
        .call_raw("GET", &static_path, Some(&family.child.token), None, Vec::new())
        .await;
    assert_eq!(status, StatusCode::OK);
    let downloaded = image::load_from_memory(&bytes).expect("a valid image");
    assert_eq!((downloaded.width(), downloaded.height()), (4, 3));

    let (status, _) = server
        .call_raw("GET", &static_path, Some(&outsider.token), None, Vec::new())
        .await;
    assert_denied(status, "a member outside the chat downloading its media");
    let (status, _) = server
        .call_raw("GET", &static_path, Some(&other.adult.token), None, Vec::new())
        .await;
    assert_denied(status, "another tenant downloading the media");
    let (status, _) = server
        .call_raw("GET", &static_path, None, None, Vec::new())
        .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    family.cleanup(&server).await;
    other.cleanup(&server).await;
    let absolute = server
        .state
        .storage
        .resolve_relative(&storage_path)
        .expect("resolvable path");
    assert!(!absolute.exists(), "cleanup removes the uploaded file");
}

#[tokio::test]
#[ignore = "requires DATABASE_URL_TEST"]
async fn realtime_events_are_limited_to_the_chats_of_the_member() {
    let server = server!();
    let family = seed_family(&server).await;
    let other = seed_family(&server).await;
    let outsider = seed_member(&server, family.tenant_id, chat_back::auth::Role::Child).await;

    for (actor, chat_id) in [
        (&family.adult, family.direct_chat_id),
        (&other.adult, other.direct_chat_id),
    ] {
        let (status, _) = server
            .post(
                &format!("/chats/{chat_id}/messages"),
                &actor.token,
                text_message(Uuid::new_v4()),
            )
            .await;
        assert_eq!(status, StatusCode::OK);
    }

    let events_for = |token: String| {
        let server = &server;
        async move {
            let (status, events) = server.get("/realtime/events", &token).await;
            assert_eq!(status, StatusCode::OK);
            events.as_array().expect("events").clone()
        }
    };
    let family_chat = json!(family.direct_chat_id);

    let child_events = events_for(family.child.token.clone()).await;
    let created = child_events
        .iter()
        .find(|event| event["type"] == json!("message.created") && event["chat_id"] == family_chat)
        .expect("a participant sees the message event");
    assert_eq!(created["tenant_id"], json!(family.tenant_id));
    assert!(created["occurred_at"].is_string());
    assert!(created["payload"].is_object());
    assert!(
        child_events
            .iter()
            .all(|event| event["tenant_id"] == json!(family.tenant_id)),
        "events of another tenant never show up"
    );

    let outsider_events = events_for(outsider.token.clone()).await;
    assert!(
        outsider_events.iter().all(|event| event["chat_id"].is_null()),
        "a member outside every chat only sees tenant wide events, got {outsider_events:?}"
    );

    let other_events = events_for(other.adult.token.clone()).await;
    assert!(other_events.iter().all(|event| event["chat_id"] != family_chat));
    assert!(
        other_events
            .iter()
            .any(|event| event["chat_id"] == json!(other.direct_chat_id))
    );

    family.cleanup(&server).await;
    other.cleanup(&server).await;
}
