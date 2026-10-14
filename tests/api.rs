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
