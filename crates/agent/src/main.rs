mod channels;

use clap::Parser;
use tracing::info;

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
}

fn main() {
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
        "pheroclaw-agent starting"
    );

    info!("pheroclaw-agent scaffold ready — exiting");
}
