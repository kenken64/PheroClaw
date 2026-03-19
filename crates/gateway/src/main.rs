use clap::Parser;
use tracing::info;

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
        listen = %cli.listen,
        redis_url = %cli.redis_url,
        "pheroclaw-gateway starting"
    );

    info!("pheroclaw-gateway scaffold ready — exiting");
}
