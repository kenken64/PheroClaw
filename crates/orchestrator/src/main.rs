use clap::Parser;
use tracing::info;

#[derive(Parser, Debug)]
#[command(name = "pheroclaw-orchestrator", about = "PheroClaw orchestrator")]
struct Cli {
    /// Redis connection URL.
    #[arg(long, env = "REDIS_URL", default_value = "redis://127.0.0.1:6379")]
    redis_url: String,
}

fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,pheroclaw=debug".into()),
        )
        .init();

    let cli = Cli::parse();

    info!(redis_url = %cli.redis_url, "pheroclaw-orchestrator starting");
    info!("pheroclaw-orchestrator scaffold ready — exiting");
}
