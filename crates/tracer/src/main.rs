mod consumer;
mod discovery;
mod writer;

use clap::Parser;
use fred::prelude::*;
use tracing::info;

#[derive(Parser, Debug)]
#[command(name = "pheroclaw-tracer", about = "PheroClaw message tracer")]
struct Cli {
    /// Redis connection URL (read-only consumer).
    #[arg(long, env = "REDIS_URL", default_value = "redis://127.0.0.1:6379")]
    redis_url: String,

    /// PostgreSQL connection URL.
    #[arg(long, env = "DATABASE_URL")]
    database_url: String,

    /// How often to re-scan the roster for new/removed agent streams (seconds).
    #[arg(long, default_value_t = 15)]
    discovery_interval_secs: u64,

    /// Internal mpsc buffer size for trace events.
    #[arg(long, default_value_t = 20_000)]
    buffer_size: usize,
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

    info!(redis_url = %cli.redis_url, "pheroclaw-tracer starting");

    // Connect to Redis
    let config = Config::from_url(&cli.redis_url)?;
    let redis = Client::new(config, None, None, None);
    redis.init().await?;
    info!("connected to Redis");

    // Connect to PostgreSQL
    let pool = sqlx::PgPool::connect(&cli.database_url).await?;
    info!("connected to PostgreSQL");

    // Run migrations
    sqlx::query(include_str!("../migrations/001_create_trace_tables.sql"))
        .execute(&pool)
        .await?;
    info!("database migrations applied");

    // Spawn the batched writer (returns mpsc sender)
    let tx = writer::spawn_writer(pool, cli.buffer_size);
    info!(buffer_size = cli.buffer_size, "writer task spawned");

    // Spawn the consumer loop
    let consumer_redis = redis.clone();
    let discovery_interval = cli.discovery_interval_secs;
    tokio::spawn(async move {
        consumer::consume_loop(consumer_redis, tx, discovery_interval).await;
    });
    info!("consumer loop spawned");

    // Wait for shutdown signal
    tokio::signal::ctrl_c().await?;
    info!("shutdown signal received, exiting");

    Ok(())
}
