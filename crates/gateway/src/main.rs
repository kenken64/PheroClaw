mod auth;
mod routes;

use std::collections::HashMap;
use std::sync::Arc;

use axum::middleware;
use axum::routing::{get, post};
use axum::Router;
use clap::Parser;
use fred::prelude::*;
use pheroclaw_messaging::acl::AclCache;
use tracing::info;

use routes::GatewayState;

#[derive(Parser, Debug)]
#[command(
    name = "pheroclaw-gateway",
    about = "PheroClaw gateway — Redis proxy for agents"
)]
struct Cli {
    /// Redis connection URL (gateway connects to Redis on behalf of agents).
    #[arg(long, env = "REDIS_URL", default_value = "redis://127.0.0.1:6379")]
    redis_url: String,

    /// Address the gateway listens on.
    #[arg(long, env = "GATEWAY_LISTEN", default_value = "0.0.0.0:8443")]
    listen: String,

    /// Comma-separated list of API key bindings: `key=agent_id`.
    /// Example: `key1=agent-alpha,key2=agent-beta,orch-key=orchestrator`
    #[arg(long, env = "GATEWAY_API_KEYS", value_delimiter = ',')]
    api_keys: Vec<String>,
}

/// Parse `key=agent_id` pairs from the CLI argument.
fn parse_api_keys(raw: Vec<String>) -> HashMap<String, String> {
    let mut map = HashMap::new();
    for entry in raw {
        if let Some((key, agent_id)) = entry.split_once('=') {
            map.insert(key.to_string(), agent_id.to_string());
        } else {
            // Backwards compat: bare keys map to themselves.
            map.insert(entry.clone(), entry);
        }
    }
    map
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,pheroclaw=debug".into()),
        )
        .init();

    let cli = Cli::parse();

    let api_keys = parse_api_keys(cli.api_keys);

    info!(
        listen = %cli.listen,
        redis_url = %cli.redis_url,
        api_key_count = api_keys.len(),
        "pheroclaw-gateway starting"
    );

    // Connect to Redis.
    let config = Config::from_url(&cli.redis_url)?;
    let redis = Client::new(config, None, None, None);
    redis.init().await?;
    info!("connected to Redis");

    // Build shared state.
    let state = Arc::new(GatewayState {
        redis: redis.clone(),
        api_keys,
        acl_cache: AclCache::new(),
    });

    // Authenticated API routes.
    let api_routes = Router::new()
        .route("/v1/send", post(routes::send_message))
        .route("/v1/register", post(routes::register))
        .route("/v1/heartbeat", post(routes::heartbeat))
        .route("/v1/deregister", post(routes::deregister))
        .route("/v1/poll", post(routes::poll))
        .route("/v1/ack", post(routes::ack))
        .route("/v1/roster", get(routes::roster))
        .route("/v1/broadcast-group", post(routes::broadcast_group))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            auth::require_api_key,
        ));

    // Top-level router: health is unauthenticated.
    let app = Router::new()
        .route("/health", get(routes::health))
        .nest("/api", api_routes)
        .with_state(state);

    // Bind and serve.
    let listener = tokio::net::TcpListener::bind(&cli.listen).await?;
    info!(addr = %cli.listen, "gateway listening");

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    // Clean up Redis connection.
    redis.quit().await?;
    info!("pheroclaw-gateway shut down");
    Ok(())
}

async fn shutdown_signal() {
    tokio::signal::ctrl_c()
        .await
        .expect("failed to install Ctrl+C handler");
    info!("shutdown signal received");
}
