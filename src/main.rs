use std::net::SocketAddr;

use anyhow::Context;
use axum::http::{HeaderValue, Method, header};
use tower_http::{cors::CorsLayer, trace::TraceLayer};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use chat_back::{app, config::Config, realtime, state::AppState};

/// A permissive CORS policy would let any site drive the API with a stolen token, so
/// browser origins must be listed explicitly. Native clients send no Origin and are
/// unaffected by an empty allowlist.
fn cors_layer(config: &Config) -> anyhow::Result<CorsLayer> {
    let mut origins = Vec::with_capacity(config.allowed_origins.len());
    for origin in &config.allowed_origins {
        origins.push(
            origin
                .parse::<HeaderValue>()
                .with_context(|| format!("invalid ALLOWED_ORIGINS entry {origin}"))?,
        );
    }
    if origins.is_empty() {
        tracing::warn!("ALLOWED_ORIGINS is empty; browser clients will be blocked by CORS");
    }

    Ok(CorsLayer::new()
        .allow_origin(origins)
        .allow_methods([
            Method::GET,
            Method::POST,
            Method::PATCH,
            Method::PUT,
            Method::DELETE,
        ])
        .allow_headers([header::AUTHORIZATION, header::CONTENT_TYPE])
        .allow_credentials(true))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();

    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                "chat_back=debug,tower_http=debug,axum::rejection=trace".into()
            }),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let config = Config::from_env()?;
    let bind_addr: SocketAddr = config
        .bind_addr
        .parse()
        .with_context(|| format!("invalid BIND_ADDR {}", config.bind_addr))?;

    let state = AppState::connect(config).await?;
    sqlx::migrate!("./migrations").run(&state.pool).await?;
    realtime::spawn_event_retention(state.pool.clone());

    let app = app::router(state.clone())
        .layer(cors_layer(&state.config)?)
        .layer(TraceLayer::new_for_http());

    let listener = tokio::net::TcpListener::bind(bind_addr).await?;
    tracing::info!("listening on http://{bind_addr}");
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await?;

    Ok(())
}
