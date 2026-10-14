# Chat Family API

Backend of Chat Family, a closed group messenger where the server carries the conversations without being able to read them. Written in Rust with axum and MySQL. This repository holds the HTTP API, the WebSocket hub and the database migrations.

| Part | What it is |
| --- | --- |
| [`chat-family-api`](https://github.com/Viniciusadm/chat-family-api) | This repository. Accounts, devices, chats, messages, media, key exchange and realtime events. |
| [`chat-family-app`](https://github.com/Viniciusadm/chat-family-app) | The Expo app that encrypts the messages and talks to this API. |

## What is encrypted

Text messages are end-to-end encrypted. The `messages` table stores `ciphertext`, `iv` and `enc_version`, and a `CHECK` constraint refuses a text message without them unless it is deleted. The rule lives in the database, so neither a route bug nor a manual `INSERT` can store plain text.

Audio and images are not. Files are written as they arrive and served by `GET /static/{path}`, protected by authorization (valid token, same tenant and chat membership checked on every download), but the server can open them.

## How keys move

Each chat has a symmetric key that only exists on devices. The server stores two kinds of opaque blobs and never sees the key:

- `key_shares`: the chat key wrapped by one device for another device's public key, with an ephemeral public key per share. Only the target device's private key opens it. `wrapped_by_member_id` is `ON DELETE RESTRICT`, so the record of who gave access to whom cannot be deleted with the member.
- `key_backups`: one blob per `(user_id, chat_id)`, encrypted with a key derived from the user's password. The server keeps the password salt and an encrypted verifier, never the password or the key. This restores conversations on a new device without another device being online.

## Accounts and devices

A `tenant` is the closed group. Every row carries `tenant_id` and every query filters by it.

Members are `adult` or `child`. Adults sign in with a password (`POST /auth/login`, Argon2 hash); children sign in with a code from `login_codes` (`POST /auth/child-login`).

Devices start unapproved. An adult lists them at `GET /admin/devices/pending` and approves one with `POST /admin/devices/{id}/approve`. Approving a device deactivates the user's other devices and records why in `deactivation_reason` (`new_active_device`, `logout`, `admin_removed`, `account_deleted`).

Access tokens last 15 minutes and refresh tokens 30 days, both configurable. Refresh tokens are stored as hashes.

Login attempts are rate limited with Redis. The child login limit counts by IP and by device, never by code, since a counter per code would give an attacker a fresh counter for every guess. Without `REDIS_URL` the server starts, but `/auth/login` and `/auth/child-login` have no brute force protection.

CORS uses an explicit origin list. Native clients send no `Origin` and are not affected; browsers need their origin in `ALLOWED_ORIGINS`.

`JWT_SECRET` must have at least 32 characters, and known development values (`change-me`, `dev-secret-change-me`) are rejected at startup.

## Realtime

`GET /realtime` upgrades to a WebSocket and delivers live events. A client that reconnects reads what it missed from `GET /realtime/events`, which returns the append-only `realtime_events` log from a cursor. A task prunes events older than 7 days every hour, in batches of 5,000 rows.

## Requirements

- Rust with edition 2024 support (1.85+)
- MySQL 8
- Redis, optional, for rate limiting

## Running locally

Copy the environment file, set `DATABASE_URL` and generate a secret (for example with `openssl rand -base64 48`):

```bash
cp .env.example .env
```

Start the server; migrations run on startup:

```bash
cargo run
```

It listens on `127.0.0.1:3000`. Set `BIND_ADDR=0.0.0.0:3000` to reach it from a physical device on the same network.

| Variable | Required | Default | Purpose |
| --- | --- | --- | --- |
| `DATABASE_URL` | yes | | MySQL connection |
| `JWT_SECRET` | yes | | Token signing, at least 32 characters |
| `BIND_ADDR` | no | `127.0.0.1:3000` | Listen address |
| `ACCESS_TOKEN_TTL_MINUTES` | no | `15` | Access token lifetime |
| `REFRESH_TOKEN_TTL_DAYS` | no | `30` | Refresh token lifetime |
| `MEDIA_ROOT` | no | `./media` | Where uploaded files are written |
| `MEDIA_PUBLIC_BASE_URL` | no | `http://localhost:3000/static` | Base of media URLs |
| `ALLOWED_ORIGINS` | for web | empty | Comma separated origins; empty blocks browsers |
| `REDIS_URL` | recommended | empty | Rate limiting; empty disables it |
| `EXPO_ACCESS_TOKEN` | no | empty | Push notifications through Expo |
| `CHILD_LOGIN_MAX_ATTEMPTS` | no | `5` | Child login attempts per window |
| `CHILD_LOGIN_WINDOW_SECS` | no | `60` | Window size in seconds |

## API

| Area | Routes |
| --- | --- |
| Session | `POST /auth/register` `/auth/login` `/auth/child-login` `/auth/refresh` `/auth/logout`, `GET /auth/me` |
| Devices | `POST /devices`, `GET PATCH /devices/{id}`, `POST /devices/{id}/heartbeat` |
| Admin | `GET /admin/devices/pending` `/admin/devices/key-recipients`, `POST /admin/devices/{id}/approve`, `DELETE /admin/devices/{id}` |
| Members | `GET POST /members`, `PATCH DELETE /members/{id}`, `DELETE /members/{id}/photo` |
| Chats | `GET POST /chats`, `GET PATCH DELETE /chats/{id}`, `DELETE /chats/{id}/photo`, `POST /chats/{id}/read` |
| Messages | `GET POST /chats/{id}/messages`, `PATCH DELETE /chats/{id}/messages/{id}` |
| Reactions | `GET /chats/{id}/reactions`, `PUT DELETE /chats/{id}/messages/{id}/reaction` |
| Media | `POST /media/profile-photo`, `POST /chats/{id}/photo/upload`, `POST /chats/{chat}/messages/{message}/audio`, `POST /chats/{chat}/messages/{message}/image`, `GET /static/{path}` |
| Keys | `GET PUT DELETE /crypto/password-settings`, `GET DELETE /crypto/key-backups`, `PUT /crypto/key-backups/{chat}`, `GET /devices/{device}/key-shares`, `PUT /devices/{device}/key-shares/{chat}`, `GET /chats/{chat}/key-recipients`, `PUT /chats/{chat}/devices/{device}/key-shares` |
| Realtime | `GET /realtime` (WebSocket), `GET /realtime/events` |
| Health | `GET /health` |

## Structure

```
src/
├── main.rs         startup: tracing, CORS, migrations, listener
├── app.rs          router composition
├── config.rs       environment loading and validation
├── auth.rs         roles, Argon2, JWT, rate limiting, access guards
├── realtime.rs     broadcast hub, WebSocket, event log retention
├── media.rs        disk storage and public URLs
├── push.rs         notifications through Expo
├── auth_api/       register, login, refresh
├── devices/        device registration and approval
├── members/        group members
├── chats/          chats, messages, reactions
├── media_api/      file upload and delivery
├── crypto_api/     key shares, backups, password settings
└── realtime_api/   realtime routes
```

Each module splits routes (`mod.rs`), input (`dto.rs`) and logic (`handlers.rs`). Database access is plain SQL with bind parameters through sqlx, without an ORM.

## Tests

The unit tests need no database:

```bash
cargo test
```

The API tests in `tests/api.rs` are `#[ignore]` and read a separate `DATABASE_URL_TEST`, so a test run never points at the real database:

```bash
DATABASE_URL_TEST=mysql://user:pass@localhost:3306/chat_back_test cargo test -- --ignored
```

The unit tests cover message payload validation, media path resolution and the rate limit buckets. The API tests run against a real MySQL and cover the message history cursor, child permissions on chats, read state, member deletion and direct chat deduplication.
