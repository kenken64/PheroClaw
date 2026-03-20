mod channels;
mod correlation;
mod dispatcher;
mod gateway_client;
mod handler;
mod heartbeat;
mod inbox;
mod p2p;

use std::sync::Arc;

use axum::routing::{get, post};
use axum::Router;
use clap::Parser;
use tokio::net::TcpListener;
use tracing::{error, info};

use correlation::CorrelationRegistry;
use gateway_client::GatewayClient;

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

#[derive(Parser, Debug)]
#[command(name = "pheroclaw-agent", about = "PheroClaw agent sidecar")]
struct Cli {
    /// Gateway URL (agents connect to Redis through the gateway, never directly).
    #[arg(long, env = "GATEWAY_URL", default_value = "http://127.0.0.1:8443")]
    gateway_url: String,

    /// API key for authenticating with the gateway.
    #[arg(long, env = "GATEWAY_API_KEY")]
    gateway_api_key: String,

    /// Agent identifier.
    #[arg(long, env = "OPENCLAW_AGENT_ID", default_value = "local-dev-001")]
    agent_id: String,

    /// Address the sidecar listens on.
    #[arg(long, env = "SIDECAR_LISTEN", default_value = "0.0.0.0:9090")]
    listen: String,

    /// OpenClaw core URL (forwarded-to backend).
    #[arg(
        long,
        env = "OPENCLAW_CORE_URL",
        default_value = "http://127.0.0.1:8080"
    )]
    core_url: String,

    /// Telegram bot token (optional — only needed if serving Telegram webhook).
    #[arg(long, env = "TELEGRAM_TOKEN")]
    telegram_token: Option<String>,

    /// Whether to replay broadcast messages from the beginning.
    #[arg(long, env = "REPLAY_BROADCAST", default_value_t = false)]
    replay_broadcast: bool,
}

// ---------------------------------------------------------------------------
// Shared state
// ---------------------------------------------------------------------------

pub struct AgentState {
    pub agent_id: String,
    pub gateway: GatewayClient,
    pub core_url: String,
    pub http_client: reqwest::Client,
    pub telegram_token: Option<String>,
    pub correlations: CorrelationRegistry,
}

// ---------------------------------------------------------------------------
// Health endpoint
// ---------------------------------------------------------------------------

async fn health() -> axum::Json<serde_json::Value> {
    axum::Json(serde_json::json!({ "status": "ok" }))
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,pheroclaw=debug".into()),
        )
        .init();

    let cli = Cli::parse();

    info!(
        agent_id = %cli.agent_id,
        gateway_url = %cli.gateway_url,
        listen = %cli.listen,
        core_url = %cli.core_url,
        telegram = cli.telegram_token.is_some(),
        replay_broadcast = cli.replay_broadcast,
        "pheroclaw-agent starting"
    );

    // Build shared state.
    let gateway = GatewayClient::new(cli.gateway_url.clone(), cli.gateway_api_key.clone());
    let state = Arc::new(AgentState {
        agent_id: cli.agent_id.clone(),
        gateway,
        core_url: cli.core_url.clone(),
        http_client: reqwest::Client::new(),
        telegram_token: cli.telegram_token.clone(),
        correlations: CorrelationRegistry::new(),
    });

    // Register with the gateway.
    info!("registering agent with gateway");
    state
        .gateway
        .register(&cli.agent_id, None)
        .await
        .map_err(|e| {
            error!(error = %e, "failed to register with gateway");
            e
        })?;
    info!("agent registered");

    // Spawn background tasks.
    let hb_state = state.clone();
    tokio::spawn(async move {
        heartbeat::heartbeat_loop(hb_state).await;
    });

    let inbox_state = state.clone();
    tokio::spawn(async move {
        inbox::inbox_loop(inbox_state).await;
    });

    // Build Axum router.
    let app = Router::new()
        .route("/health", get(health))
        .route("/webhook/telegram", post(channels::telegram::webhook))
        .route("/webchat/message", post(channels::webchat::message))
        .route("/a2a/messages", post(channels::a2a::receive))
        .with_state(state.clone());

    // Bind and serve.
    let listener = TcpListener::bind(&cli.listen).await?;
    info!(listen = %cli.listen, "HTTP server listening");

    // Graceful shutdown on Ctrl+C.
    let shutdown_state = state.clone();
    let shutdown_agent_id = cli.agent_id.clone();

    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            tokio::signal::ctrl_c()
                .await
                .expect("failed to listen for ctrl+c");
            info!("shutting down — deregistering agent");
            if let Err(e) = shutdown_state.gateway.deregister(&shutdown_agent_id).await {
                error!(error = %e, "failed to deregister on shutdown");
            }
            info!("agent deregistered, goodbye");
        })
        .await?;

    Ok(())
}
