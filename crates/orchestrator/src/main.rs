#[allow(dead_code)]
mod acl;
mod dlq;
mod health;
mod outbox_consumer;
mod task_router;
mod watch;
mod writer;

use clap::Parser;
use fred::prelude::*;
use tracing::{info, warn};

#[derive(Parser, Debug)]
#[command(name = "pheroclaw-orchestrator", about = "PheroClaw orchestrator")]
struct Cli {
    /// Redis connection URL.
    #[arg(long, env = "REDIS_URL", default_value = "redis://127.0.0.1:6379")]
    redis_url: String,

    /// Maximum retries before a pending message is moved to the DLQ.
    #[arg(long, env = "MAX_RETRIES", default_value = "3")]
    max_retries: u32,

    /// Seconds a message can sit pending before the DLQ sweep considers it stale.
    #[arg(long, env = "PENDING_TIMEOUT_SECS", default_value = "120")]
    pending_timeout_secs: u64,

    /// Live-tail a specific agent's inbox/outbox (debug mode). Runs instead of the main loops.
    #[arg(long)]
    watch: Option<String>,

    /// PostgreSQL connection URL. If omitted, task lifecycle persistence is disabled.
    #[arg(long, env = "DATABASE_URL")]
    database_url: Option<String>,

    /// Internal mpsc buffer size for task lifecycle events.
    #[arg(long, default_value_t = 10_000)]
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
    info!(redis_url = %cli.redis_url, "pheroclaw-orchestrator starting");

    // Connect to Redis
    let config = Config::from_url(&cli.redis_url)?;
    let client = Client::new(config, None, None, None);
    client.init().await?;
    info!("connected to Redis");

    // If --watch is set, run the live-tail and exit when it finishes (Ctrl+C)
    if let Some(agent_id) = &cli.watch {
        return watch::watch_agent(&client, agent_id).await;
    }

    // Connect to PostgreSQL (optional)
    let writer_tx = if let Some(db_url) = &cli.database_url {
        let pool = sqlx::PgPool::connect(db_url).await?;
        info!("connected to PostgreSQL");

        sqlx::query(include_str!(
            "../migrations/001_create_task_lifecycle.sql"
        ))
        .execute(&pool)
        .await?;
        info!("task_lifecycle migration applied");

        let tx = writer::spawn_writer(pool, cli.buffer_size);
        info!(buffer_size = cli.buffer_size, "task lifecycle writer spawned");
        Some(tx)
    } else {
        warn!("no --database-url provided, task lifecycle persistence disabled");
        None
    };

    // Spawn background loops
    let health_client = client.clone();
    tokio::spawn(async move {
        health::health_loop(health_client).await;
    });

    let dlq_client = client.clone();
    let max_retries = cli.max_retries;
    let pending_timeout_secs = cli.pending_timeout_secs;
    tokio::spawn(async move {
        dlq::dlq_loop(dlq_client, max_retries, pending_timeout_secs).await;
    });

    let outbox_client = client.clone();
    tokio::spawn(async move {
        outbox_consumer::outbox_loop(outbox_client, writer_tx).await;
    });

    info!("orchestrator running — health(30s), DLQ(60s), outbox(5s). Ctrl+C to stop");

    // Wait for shutdown signal
    tokio::signal::ctrl_c().await?;
    info!("shutdown signal received, exiting");

    Ok(())
}
