use clap::Parser;
use tracing::info;

#[derive(Parser, Debug)]
#[command(name = "pheroclaw-tracer", about = "PheroClaw message tracer")]
struct Cli {
    /// Redis connection URL (read-only consumer).
    #[arg(long, env = "REDIS_URL", default_value = "redis://127.0.0.1:6379")]
    redis_url: String,

    /// PostgreSQL connection URL.
    #[arg(long, env = "DATABASE_URL")]
    database_url: Option<String>,

    /// How often to re-scan the roster for new/removed agent streams (seconds).
    #[arg(long, default_value_t = 15)]
    discovery_interval_secs: u64,
}

fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,pheroclaw=debug".into()),
        )
        .init();

    let cli = Cli::parse();

    info!(redis_url = %cli.redis_url, "pheroclaw-tracer starting");
    info!("pheroclaw-tracer scaffold ready — exiting");
}
