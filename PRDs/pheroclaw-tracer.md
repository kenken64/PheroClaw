# OpenClaw Message Tracer — Dedicated Consumer

A standalone Rust binary that taps every Redis Stream in the system and permanently records all messages to PostgreSQL. No agent or orchestrator touches the database.

---

## Architecture

```
┌──────────┐         ┌──────────┐         ┌──────────┐
│  Agent   │         │  Agent   │         │  Orch.   │
│  alpha   │         │  beta    │         │          │
└────┬─────┘         └────┬─────┘         └────┬─────┘
     │ XADD               │ XADD               │ XADD
     ▼                     ▼                     ▼
┌──────────────────────────────────────────────────────┐
│                     Redis Streams                     │
│                                                      │
│  openclaw:orch:broadcast                             │
│  openclaw:agent:alpha:inbox                          │
│  openclaw:agent:alpha:outbox                         │
│  openclaw:agent:beta:inbox                           │
│  openclaw:agent:beta:outbox                          │
│  openclaw:dlq                                        │
│                                                      │
│  Each stream has MULTIPLE consumer groups:            │
│                                                      │
│  broadcast ──▶ cg-alpha   (Agent alpha reads here)   │
│            ──▶ cg-beta    (Agent beta reads here)    │
│            ──▶ cg-tracer  (Tracer reads here)  ◀──── │  ← NEW
│                                                      │
│  agent:alpha:inbox ──▶ cg-alpha   (Agent alpha)      │
│                    ──▶ cg-tracer  (Tracer)     ◀──── │  ← NEW
│                                                      │
│  agent:alpha:outbox ──▶ orchestrator (Orch reads)    │
│                     ──▶ cg-tracer  (Tracer)    ◀──── │  ← NEW
└──────────────────────────────────────────────────────┘
           │
           │ XREADGROUP (cg-tracer consumer group)
           │ The tracer is just another consumer —
           │ agents don't know it exists
           ▼
┌──────────────────────┐
│   openclaw-tracer    │
│   (Rust binary)      │
│                      │
│   • Discovers all    │
│     streams via      │
│     roster           │
│   • Creates its own  │
│     consumer group   │
│     on each stream   │
│   • Batches writes   │
│     to PostgreSQL    │
│   • Single instance, │
│     single DB conn   │
└──────────┬───────────┘
           │
           │ Batched INSERT (every 500ms or 100 msgs)
           ▼
┌──────────────────────┐
│     PostgreSQL       │
│                      │
│  message_trace       │
│  agent_events        │
│  dlq_audit           │
└──────────────────────┘
```

**What each component knows about:**

| Component | Redis | PostgreSQL | Each other |
|-----------|-------|------------|------------|
| Agent | ✅ Direct or via gateway | ❌ No access | Discovers peers via roster |
| Orchestrator | ✅ Direct | ❌ No access | Manages agents |
| **Tracer** | ✅ Read-only consumer | ✅ Only writer | Invisible to agents |

This is the key: **agents and orchestrator are completely unaware of PostgreSQL**. The tracer is a passive observer that reads the same streams through its own consumer group.

---

## Step 1: Add the Tracer Crate

Add to workspace `Cargo.toml`:

```toml
[workspace]
members = [
    "crates/messaging",
    "crates/orchestrator",
    "crates/agent",
    "crates/tracer",        # ← NEW
]
```

**`crates/tracer/Cargo.toml`**:

```toml
[package]
name = "openclaw-tracer"
version = "0.1.0"
edition = "2021"

[[bin]]
name = "tracer"
path = "src/main.rs"

[dependencies]
openclaw-messaging = { path = "../messaging" }
tokio.workspace = true
fred.workspace = true
serde.workspace = true
serde_json.workspace = true
tracing.workspace = true
tracing-subscriber.workspace = true
clap.workspace = true
anyhow.workspace = true
uuid.workspace = true
chrono.workspace = true

# Tracer-specific — only this binary has DB access
sqlx = { version = "0.8", features = [
    "runtime-tokio",
    "postgres",
    "chrono",
    "uuid",
    "json",
] }
```

Notice: `sqlx` only appears in the tracer's `Cargo.toml`. The messaging lib, agent, and orchestrator crates have zero database dependencies.

---

## Step 2: PostgreSQL Schema

**`crates/tracer/migrations/001_create_trace_tables.sql`**:

```sql
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ═══════════════════════════════════════════════
-- Core trace table: one row per message observed
-- ═══════════════════════════════════════════════
CREATE TABLE message_trace (
    trace_id        BIGSERIAL    PRIMARY KEY,

    -- From the ClawMessage envelope
    message_id      UUID         NOT NULL,
    correlation_id  UUID,
    from_agent      VARCHAR(128) NOT NULL,
    to_target_type  VARCHAR(32)  NOT NULL,   -- 'broadcast', 'agent', 'orchestrator'
    to_target_id    VARCHAR(128),            -- NULL for broadcast/orchestrator
    msg_type        VARCHAR(64)  NOT NULL,   -- 'task', 'p2p', 'skill:xxx', 'system:xxx'
    payload         JSONB        NOT NULL,
    retry_count     INT          NOT NULL DEFAULT 0,

    -- Which stream and entry this came from
    redis_stream    VARCHAR(256) NOT NULL,
    redis_entry_id  VARCHAR(64)  NOT NULL,

    -- Timestamps
    message_ts      TIMESTAMPTZ  NOT NULL,   -- from ClawMessage.ts (agent's clock)
    recorded_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW()  -- when tracer wrote this
);

-- ═══════════════════════════════════════════════
-- Indexes for common query patterns
-- ═══════════════════════════════════════════════

-- "Show me every trace for this message"
CREATE UNIQUE INDEX idx_trace_stream_entry
    ON message_trace (redis_stream, redis_entry_id);

-- "Trace a full request-reply chain"
CREATE INDEX idx_trace_correlation
    ON message_trace (correlation_id, recorded_at)
    WHERE correlation_id IS NOT NULL;

-- "What did agent alpha send/receive today?"
CREATE INDEX idx_trace_from_time
    ON message_trace (from_agent, recorded_at DESC);
CREATE INDEX idx_trace_to_time
    ON message_trace (to_target_id, recorded_at DESC)
    WHERE to_target_id IS NOT NULL;

-- "Show all P2P between two agents"
CREATE INDEX idx_trace_p2p_pair
    ON message_trace (from_agent, to_target_id, recorded_at DESC)
    WHERE msg_type = 'p2p';

-- "Show all tasks of a specific type"
CREATE INDEX idx_trace_msg_type
    ON message_trace (msg_type, recorded_at DESC);

-- Time-range scans (dashboards, cleanup)
CREATE INDEX idx_trace_recorded
    ON message_trace (recorded_at DESC);

-- ═══════════════════════════════════════════════
-- Agent lifecycle events
-- ═══════════════════════════════════════════════
CREATE TABLE agent_events (
    event_id    BIGSERIAL    PRIMARY KEY,
    agent_id    VARCHAR(128) NOT NULL,
    event_type  VARCHAR(32)  NOT NULL,   -- 'joined', 'left', 'heartbeat_lost'
    metadata    JSONB        DEFAULT '{}'::JSONB,
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_agent_events_agent
    ON agent_events (agent_id, created_at DESC);

-- ═══════════════════════════════════════════════
-- DLQ audit trail
-- ═══════════════════════════════════════════════
CREATE TABLE dlq_audit (
    dlq_id          BIGSERIAL    PRIMARY KEY,
    message_id      UUID         NOT NULL,
    original_stream VARCHAR(256) NOT NULL,
    agent_id        VARCHAR(128) NOT NULL,
    delivery_count  INT          NOT NULL,
    payload         JSONB        NOT NULL,
    reason          VARCHAR(256),
    moved_at        TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    resolved_at     TIMESTAMPTZ,
    resolution      VARCHAR(256)
);

CREATE INDEX idx_dlq_open
    ON dlq_audit (moved_at DESC)
    WHERE resolved_at IS NULL;
```

---

## Step 3: Stream Discovery

The tracer needs to know which streams to read from. It discovers them dynamically from the roster — as agents register and deregister, the tracer picks up their inbox and outbox streams automatically.

**`crates/tracer/src/discovery.rs`**:

```rust
use anyhow::Result;
use fred::prelude::*;
use openclaw_messaging as msg;
use std::collections::HashSet;
use tracing::{debug, info};

/// All streams the tracer needs to consume.
#[derive(Debug, Clone)]
pub struct TracedStreams {
    pub streams: Vec<String>,
}

/// Discover all streams that currently exist by reading the roster
/// and building the full list of broadcast + inbox + outbox + DLQ streams.
pub async fn discover_streams(redis: &RedisClient) -> Result<TracedStreams> {
    let mut streams = Vec::new();

    // 1. Broadcast stream — always exists
    streams.push(msg::BROADCAST_STREAM.to_string());

    // 2. DLQ stream
    streams.push(msg::DLQ_STREAM.to_string());

    // 3. Per-agent streams (inbox + outbox for each known agent)
    let roster: Vec<String> = redis.smembers(msg::ROSTER_KEY).await.unwrap_or_default();

    for agent_id in &roster {
        streams.push(msg::agent_inbox(agent_id));
        streams.push(msg::agent_outbox(agent_id));
    }

    info!(
        stream_count = streams.len(),
        agent_count = roster.len(),
        "discovered streams"
    );

    Ok(TracedStreams { streams })
}

/// Compare current streams against what we're already consuming.
/// Returns (new_streams, removed_streams).
pub fn diff_streams(
    current: &[String],
    discovered: &[String],
) -> (Vec<String>, Vec<String>) {
    let current_set: HashSet<&str> = current.iter().map(|s| s.as_str()).collect();
    let discovered_set: HashSet<&str> = discovered.iter().map(|s| s.as_str()).collect();

    let new: Vec<String> = discovered_set
        .difference(&current_set)
        .map(|s| s.to_string())
        .collect();

    let removed: Vec<String> = current_set
        .difference(&discovered_set)
        .map(|s| s.to_string())
        .collect();

    new.iter().for_each(|s| debug!(stream = %s, "new stream discovered"));
    removed.iter().for_each(|s| debug!(stream = %s, "stream removed"));

    (new, removed)
}
```

---

## Step 4: Batch Writer (the PostgreSQL side)

Non-blocking, batched writes. The consumer loop sends trace events into a channel; the writer drains the channel and flushes to PostgreSQL in batches.

**`crates/tracer/src/writer.rs`**:

```rust
use anyhow::Result;
use chrono::{DateTime, Utc};
use openclaw_messaging::types::*;
use sqlx::PgPool;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

/// A single trace event to be written.
#[derive(Debug)]
pub struct TraceEvent {
    pub message_id: Uuid,
    pub correlation_id: Option<Uuid>,
    pub from_agent: String,
    pub to_target_type: String,
    pub to_target_id: Option<String>,
    pub msg_type: String,
    pub payload: serde_json::Value,
    pub retry_count: i32,
    pub redis_stream: String,
    pub redis_entry_id: String,
    pub message_ts: DateTime<Utc>,
}

impl TraceEvent {
    /// Convert a ClawMessage + stream metadata into a TraceEvent.
    pub fn from_message(
        msg: &ClawMessage,
        stream: &str,
        entry_id: &str,
    ) -> Self {
        let (to_type, to_id) = match &msg.to {
            MessageTarget::Broadcast => ("broadcast".to_string(), None),
            MessageTarget::Agent(id) => ("agent".to_string(), Some(id.clone())),
            MessageTarget::Orchestrator => ("orchestrator".to_string(), None),
        };

        let msg_type_str = match &msg.msg_type {
            MsgType::Task => "task".to_string(),
            MsgType::TaskResult => "task_result".to_string(),
            MsgType::P2P => "p2p".to_string(),
            MsgType::Heartbeat => "heartbeat".to_string(),
            MsgType::Skill(name) => format!("skill:{name}"),
            MsgType::System(cmd) => format!("system:{cmd}"),
        };

        let message_ts = DateTime::from_timestamp_millis(msg.ts)
            .unwrap_or_else(Utc::now);

        Self {
            message_id: msg.id,
            correlation_id: msg.correlation_id,
            from_agent: msg.from.clone(),
            to_target_type: to_type,
            to_target_id: to_id,
            msg_type: msg_type_str,
            payload: msg.payload.clone(),
            retry_count: msg.retry_count as i32,
            redis_stream: stream.to_string(),
            redis_entry_id: entry_id.to_string(),
            message_ts,
        }
    }
}

/// Spawn the batch writer background task.
/// Returns a sender that the consumer loop uses to submit trace events.
pub fn spawn_writer(pool: PgPool, buffer_size: usize) -> mpsc::Sender<TraceEvent> {
    let (tx, rx) = mpsc::channel::<TraceEvent>(buffer_size);

    tokio::spawn(async move {
        writer_loop(rx, pool).await;
    });

    tx
}

/// Background loop: collect events, flush in batches.
async fn writer_loop(mut rx: mpsc::Receiver<TraceEvent>, pool: PgPool) {
    let max_batch = 200;
    let flush_interval = tokio::time::Duration::from_millis(500);
    let mut batch: Vec<TraceEvent> = Vec::with_capacity(max_batch);

    info!("trace writer started");

    loop {
        let deadline = tokio::time::sleep(flush_interval);
        tokio::pin!(deadline);

        // Fill the batch until full or timeout
        loop {
            tokio::select! {
                Some(event) = rx.recv() => {
                    batch.push(event);
                    if batch.len() >= max_batch {
                        break;
                    }
                }
                _ = &mut deadline => {
                    break;
                }
            }
        }

        if batch.is_empty() {
            continue;
        }

        let count = batch.len();
        match flush_batch(&pool, &batch).await {
            Ok(_) => {
                debug!(count, "flushed trace batch to PostgreSQL");
            }
            Err(e) => {
                error!(count, error = %e, "failed to flush trace batch");
                // In production, consider writing to a local WAL file
                // so events aren't lost when PostgreSQL is unreachable.
            }
        }

        batch.clear();
    }
}

/// Bulk insert using a single query with unnested arrays.
/// Much faster than individual INSERTs for batches.
async fn flush_batch(pool: &PgPool, events: &[TraceEvent]) -> Result<()> {
    if events.is_empty() {
        return Ok(());
    }

    // Build parallel arrays for unnest-based bulk insert
    let mut message_ids: Vec<Uuid> = Vec::with_capacity(events.len());
    let mut correlation_ids: Vec<Option<Uuid>> = Vec::with_capacity(events.len());
    let mut from_agents: Vec<String> = Vec::with_capacity(events.len());
    let mut to_types: Vec<String> = Vec::with_capacity(events.len());
    let mut to_ids: Vec<Option<String>> = Vec::with_capacity(events.len());
    let mut msg_types: Vec<String> = Vec::with_capacity(events.len());
    let mut payloads: Vec<serde_json::Value> = Vec::with_capacity(events.len());
    let mut retry_counts: Vec<i32> = Vec::with_capacity(events.len());
    let mut streams: Vec<String> = Vec::with_capacity(events.len());
    let mut entry_ids: Vec<String> = Vec::with_capacity(events.len());
    let mut timestamps: Vec<DateTime<Utc>> = Vec::with_capacity(events.len());

    for e in events {
        message_ids.push(e.message_id);
        correlation_ids.push(e.correlation_id);
        from_agents.push(e.from_agent.clone());
        to_types.push(e.to_target_type.clone());
        to_ids.push(e.to_target_id.clone());
        msg_types.push(e.msg_type.clone());
        payloads.push(e.payload.clone());
        retry_counts.push(e.retry_count);
        streams.push(e.redis_stream.clone());
        entry_ids.push(e.redis_entry_id.clone());
        timestamps.push(e.message_ts);
    }

    sqlx::query(r#"
        INSERT INTO message_trace
            (message_id, correlation_id, from_agent, to_target_type,
             to_target_id, msg_type, payload, retry_count,
             redis_stream, redis_entry_id, message_ts)
        SELECT * FROM UNNEST(
            $1::UUID[],
            $2::UUID[],
            $3::VARCHAR[],
            $4::VARCHAR[],
            $5::VARCHAR[],
            $6::VARCHAR[],
            $7::JSONB[],
            $8::INT[],
            $9::VARCHAR[],
            $10::VARCHAR[],
            $11::TIMESTAMPTZ[]
        )
        ON CONFLICT (redis_stream, redis_entry_id) DO NOTHING
    "#)
    .bind(&message_ids)
    .bind(&correlation_ids)
    .bind(&from_agents)
    .bind(&to_types)
    .bind(&to_ids)
    .bind(&msg_types)
    .bind(&payloads)
    .bind(&retry_counts)
    .bind(&streams)
    .bind(&entry_ids)
    .bind(&timestamps)
    .execute(pool)
    .await?;

    Ok(())
}
```

---

## Step 5: Stream Consumer (the Redis side)

The consumer reads from all discovered streams using its own consumer group (`cg-tracer`). It doesn't ACK messages in the agents' consumer groups — it has its own independent cursor.

**`crates/tracer/src/consumer.rs`**:

```rust
use anyhow::Result;
use fred::prelude::*;
use openclaw_messaging as msg;
use tokio::sync::mpsc;
use tracing::{debug, error, warn};

use crate::writer::TraceEvent;

const TRACER_GROUP: &str = "cg-tracer";
const TRACER_CONSUMER: &str = "tracer-0";

/// Ensure the tracer's consumer group exists on a stream.
pub async fn ensure_tracer_group(redis: &RedisClient, stream: &str) -> Result<()> {
    // "0" = replay all existing messages from the beginning
    // This ensures the tracer captures the full history on first run
    msg::ensure_consumer_group(redis, stream, TRACER_GROUP, "0").await
}

/// Consume from a set of streams and send trace events to the writer.
/// This function runs in a loop, blocking on XREADGROUP.
pub async fn consume_streams(
    redis: &RedisClient,
    streams: &[String],
    tx: &mpsc::Sender<TraceEvent>,
) {
    if streams.is_empty() {
        return;
    }

    // Build the stream-key + ">" pairs for XREADGROUP
    // ">" means: give me messages not yet delivered to this consumer
    let stream_refs: Vec<(&str, &str)> = streams
        .iter()
        .map(|s| (s.as_str(), ">"))
        .collect();

    loop {
        let result: Result<Vec<(String, Vec<(String, Vec<(String, String)>)>)>, _> =
            redis.xreadgroup(
                TRACER_GROUP,
                TRACER_CONSUMER,
                Some(100),        // COUNT — larger batch for throughput
                Some(5_000),      // BLOCK 5s
                false,            // NOACK = false, we ACK after writing to channel
                &stream_refs,
            )
            .await;

        let result_streams = match result {
            Ok(s) => s,
            Err(e) => {
                // Common error: stream doesn't exist yet (agent hasn't sent anything)
                // Just retry after a short sleep
                debug!(error = %e, "XREADGROUP error, retrying...");
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                continue;
            }
        };

        for (stream_name, entries) in result_streams {
            for (entry_id, fields) in entries {
                let raw = match fields.iter().find(|(k, _)| k == "msg") {
                    Some((_, v)) => v,
                    None => {
                        // ACK non-message entries to prevent PEL buildup
                        let _ = redis
                            .xack::<i64, _, _, _>(&stream_name, TRACER_GROUP, &entry_id)
                            .await;
                        continue;
                    }
                };

                match msg::decode_message(raw) {
                    Ok(claw_msg) => {
                        // Skip heartbeat messages — too noisy for the trace DB
                        if matches!(claw_msg.msg_type, msg::types::MsgType::Heartbeat) {
                            let _ = redis
                                .xack::<i64, _, _, _>(
                                    &stream_name, TRACER_GROUP, &entry_id
                                )
                                .await;
                            continue;
                        }

                        let event = TraceEvent::from_message(
                            &claw_msg,
                            &stream_name,
                            &entry_id,
                        );

                        // Non-blocking send to the writer channel
                        match tx.try_send(event) {
                            Ok(_) => {
                                // ACK — event is in the writer's buffer
                                let _ = redis
                                    .xack::<i64, _, _, _>(
                                        &stream_name, TRACER_GROUP, &entry_id
                                    )
                                    .await;
                            }
                            Err(mpsc::error::TrySendError::Full(_)) => {
                                warn!(
                                    stream = %stream_name,
                                    entry_id,
                                    "writer buffer full — NOT acking, will retry"
                                );
                                // Don't ACK — message stays in PEL
                                // Next XREADGROUP iteration will re-deliver it
                            }
                            Err(mpsc::error::TrySendError::Closed(_)) => {
                                error!("writer channel closed — tracer shutting down");
                                return;
                            }
                        }
                    }
                    Err(e) => {
                        warn!(
                            error = %e,
                            stream = %stream_name,
                            entry_id,
                            "failed to decode message — acking to skip"
                        );
                        let _ = redis
                            .xack::<i64, _, _, _>(
                                &stream_name, TRACER_GROUP, &entry_id
                            )
                            .await;
                    }
                }
            }
        }
    }
}
```

---

## Step 6: Main — Wire Everything Together

**`crates/tracer/src/main.rs`**:

```rust
mod consumer;
mod discovery;
mod writer;

use anyhow::Result;
use clap::Parser;
use fred::prelude::*;
use sqlx::postgres::PgPoolOptions;
use std::sync::Arc;
use tracing::info;

#[derive(Parser, Debug)]
#[command(name = "openclaw-tracer", about = "OpenClaw Message Tracer")]
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

    /// Writer channel buffer size.
    #[arg(long, default_value_t = 20_000)]
    buffer_size: usize,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter("openclaw=debug,info")
        .json()
        .init();

    let cli = Cli::parse();
    info!("starting OpenClaw tracer");

    // ─── Redis ───
    let config = RedisConfig::from_url(&cli.redis_url)?;
    let redis = RedisClient::new(config, None, None, None);
    redis.connect();
    redis.wait_for_connect().await?;
    let redis = Arc::new(redis);
    info!("connected to Redis");

    // ─── PostgreSQL ───
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&cli.database_url)
        .await?;
    info!("connected to PostgreSQL");

    // Run migrations
    sqlx::migrate!("./migrations").run(&pool).await?;
    info!("migrations applied");

    // ─── Spawn the batch writer ───
    let tx = writer::spawn_writer(pool, cli.buffer_size);

    // ─── Discovery + consumer loop ───
    // The tracer re-discovers streams periodically so it picks up
    // new agents without restarting.
    let discovery_redis = Arc::clone(&redis);
    let consumer_redis = Arc::clone(&redis);
    let discovery_interval = cli.discovery_interval_secs;

    // Track currently consumed streams
    let mut current_streams: Vec<String> = Vec::new();
    // Handle for the current consumer task
    let mut consumer_handle: Option<tokio::task::JoinHandle<()>> = None;

    let mut interval = tokio::time::interval(
        std::time::Duration::from_secs(discovery_interval),
    );

    info!("entering discovery loop");

    loop {
        tokio::select! {
            _ = interval.tick() => {
                // Re-discover streams
                let discovered = match discovery::discover_streams(&discovery_redis).await {
                    Ok(d) => d,
                    Err(e) => {
                        tracing::error!(error = %e, "discovery failed");
                        continue;
                    }
                };

                let (new_streams, _removed) =
                    discovery::diff_streams(&current_streams, &discovered.streams);

                if !new_streams.is_empty() || current_streams.is_empty() {
                    // Ensure tracer consumer group on all streams
                    for stream in &discovered.streams {
                        if let Err(e) = consumer::ensure_tracer_group(
                            &consumer_redis, stream
                        ).await {
                            tracing::warn!(
                                stream, error = %e,
                                "failed to create tracer group"
                            );
                        }
                    }

                    // Restart the consumer with the updated stream list
                    if let Some(handle) = consumer_handle.take() {
                        handle.abort();
                    }

                    let streams = discovered.streams.clone();
                    let redis_clone = Arc::clone(&consumer_redis);
                    let tx_clone = tx.clone();

                    consumer_handle = Some(tokio::spawn(async move {
                        consumer::consume_streams(
                            &redis_clone, &streams, &tx_clone
                        ).await;
                    }));

                    current_streams = discovered.streams;
                    info!(
                        stream_count = current_streams.len(),
                        "consumer restarted with updated streams"
                    );
                }
            }
            _ = tokio::signal::ctrl_c() => {
                info!("shutting down tracer");
                if let Some(h) = consumer_handle.take() {
                    h.abort();
                }
                break;
            }
        }
    }

    info!("tracer stopped");
    Ok(())
}
```

---

## Step 7: Deployment — Where Each Binary Runs

```
┌──────────────────────────────────────────────────────────────┐
│                    Your Infrastructure Host                    │
│                  (e.g. DigitalOcean SGP1 VPC)                 │
│                                                               │
│  ┌─────────────┐  ┌──────────────────┐  ┌─────────────────┐  │
│  │   Redis 7   │  │  Orchestrator    │  │    Tracer       │  │
│  │  (private)  │◀─│  (direct Redis)  │  │  (direct Redis  │  │
│  │  port 6379  │  │                  │  │   + PostgreSQL) │  │
│  │  VPC only   │  └──────────────────┘  └────────┬────────┘  │
│  └──────┬──────┘                                 │            │
│         │                                        │            │
│         │                               ┌────────▼────────┐  │
│         │                               │  PostgreSQL     │  │
│         │                               │  (private)      │  │
│         │                               │  port 5432      │  │
│         │                               │  VPC only       │  │
│         │                               └─────────────────┘  │
└─────────┼────────────────────────────────────────────────────┘
          │
     ┌────┴──── VPC / WireGuard / Gateway ────────┐
     │                                             │
┌────▼─────┐  ┌──────────┐  ┌──────────┐  ┌──────▼───┐
│  Agent   │  │  Agent   │  │  Agent   │  │  Agent   │
│  alpha   │  │  beta    │  │  gamma   │  │  delta   │
│  DO SGP  │  │  DO NYC  │  │ Tencent  │  │ BytePlus │
└──────────┘  └──────────┘  └──────────┘  └──────────┘

Agents connect to Redis (directly or via gateway).
Agents have ZERO knowledge of PostgreSQL.
Only the tracer binary has DATABASE_URL.
```

### Systemd service for the tracer:

```ini
[Unit]
Description=OpenClaw Message Tracer
After=network.target postgresql.service redis.service

[Service]
Type=simple
User=openclaw
Environment=REDIS_URL=redis://127.0.0.1:6379
Environment=DATABASE_URL=postgres://tracer:password@127.0.0.1:5432/openclaw_trace
ExecStart=/usr/local/bin/openclaw-tracer
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

---

## Step 8: Useful Queries

Once messages are flowing into PostgreSQL:

```sql
-- ─── Full conversation chain by correlation_id ───
-- Traces a request from orchestrator → agent → P2P → reply
SELECT
    redis_stream,
    from_agent,
    to_target_type || COALESCE(':' || to_target_id, '') AS routed_to,
    msg_type,
    message_ts,
    payload->>'status' AS status
FROM message_trace
WHERE correlation_id = 'xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx'
ORDER BY message_ts;

-- ─── All P2P messages between alpha and beta (last 24h) ───
SELECT message_id, from_agent, msg_type, payload, message_ts
FROM message_trace
WHERE msg_type = 'p2p'
  AND (
      (from_agent = 'alpha' AND to_target_id = 'beta')
   OR (from_agent = 'beta'  AND to_target_id = 'alpha')
  )
  AND recorded_at > NOW() - INTERVAL '24 hours'
ORDER BY message_ts;

-- ─── Message volume per agent per hour (dashboard) ───
SELECT
    date_trunc('hour', message_ts) AS hour,
    from_agent,
    msg_type,
    COUNT(*) AS msg_count
FROM message_trace
WHERE recorded_at > NOW() - INTERVAL '7 days'
GROUP BY hour, from_agent, msg_type
ORDER BY hour DESC, msg_count DESC;

-- ─── Find the slowest request-reply round trips ───
SELECT
    req.message_id AS request_id,
    req.from_agent AS requester,
    req.to_target_id AS responder,
    req.msg_type AS request_type,
    EXTRACT(EPOCH FROM (resp.message_ts - req.message_ts)) * 1000
        AS round_trip_ms
FROM message_trace req
JOIN message_trace resp
    ON resp.correlation_id = req.message_id
    AND resp.msg_type = 'task_result'
WHERE req.msg_type = 'task'
  AND req.recorded_at > NOW() - INTERVAL '1 hour'
ORDER BY round_trip_ms DESC
LIMIT 20;

-- ─── What skills are being invoked most? ───
SELECT
    msg_type,
    COUNT(*) AS invocations,
    COUNT(DISTINCT from_agent) AS unique_callers
FROM message_trace
WHERE msg_type LIKE 'skill:%'
  AND recorded_at > NOW() - INTERVAL '30 days'
GROUP BY msg_type
ORDER BY invocations DESC;

-- ─── Unresolved dead letters ───
SELECT dlq_id, message_id, agent_id, delivery_count, reason, moved_at
FROM dlq_audit
WHERE resolved_at IS NULL
ORDER BY moved_at DESC;
```

---

## Step 9: Data Retention

The trace table will grow. Plan for it:

```sql
-- Option A: Partition by month (recommended for production)
-- Convert to partitioned table and create monthly partitions.
-- Drop partitions older than your retention window.
DROP TABLE message_trace_2025_01;  -- drop data older than 1 year

-- Option B: Simple time-based cleanup job (cron)
DELETE FROM message_trace WHERE recorded_at < NOW() - INTERVAL '90 days';

-- Option C: Move old data to cold storage
-- Export to Parquet/CSV, upload to S3, then delete from PostgreSQL.
```

---

## Summary: Separation of Concerns

| Binary | Talks to Redis | Talks to Gateway | Talks to PostgreSQL | Role |
|--------|:-:|:-:|:-:|------|
| `openclaw-agent` | ❌ | ✅ (API key) | ❌ | Send/receive messages via gateway |
| `openclaw-gateway` | ✅ | — | ❌ | Auth, rate limit, relay for agents |
| `openclaw-orchestrator` | ✅ | ❌ | ❌ | Route tasks, health checks, DLQ |
| **`openclaw-tracer`** | ✅ (read-only) | ❌ | ✅ (only writer) | Record everything permanently |

Agents never touch Redis directly — they authenticate with the gateway via API key and send/receive messages through its HTTP API. Only infrastructure components (gateway, orchestrator, tracer) have direct Redis access within the VPC. The tracer is a passive, independent observer. If you kill the tracer, messaging continues unaffected. If PostgreSQL goes down, Redis messaging is unaffected. The two systems are fully decoupled.
