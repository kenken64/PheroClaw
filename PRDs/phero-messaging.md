# OpenClaw Redis Messaging — Implementation Tutorial

A step-by-step guide to building an orchestrator-to-many-agents messaging system with P2P support and permanent RDBMS message tracing, using Redis Streams, PostgreSQL, and Rust.

---

## Architecture Overview

### The Problem With the Naive Design

The previous architecture assumed all messages are born inside Redis. But in reality:

```
❌ What we had — blind spots everywhere:

 Human (Telegram) ──webhook──▶ OpenClaw Core ──response──▶ Telegram API
                                    │
                               (invisible to Redis, CLI, tracer)

 OpenClaw A ──native A2A──▶ OpenClaw B
                                    │
                               (invisible to Redis, CLI, tracer)
```

The orchestrator CLI (`clawmacdo`) can't see Telegram conversations. The tracer can't record webchat sessions. P2P between OpenClaw instances bypasses Redis entirely. The Redis bus only sees what's explicitly written to it.

### The Solution: Agent as Reverse Proxy Sidecar

The `openclaw-agent` binary sits **in front of** each OpenClaw instance as a reverse proxy. ALL traffic — Telegram webhooks, webchat WebSocket, A2A from other agents, orchestrator commands — passes through the sidecar. The sidecar intercepts everything, publishes to Redis, and forwards to the OpenClaw core on localhost.

```
┌──────────────────────────────────────────────────────────────────────┐
│                         Redis (Shared Bus)                            │
│                                                                      │
│  openclaw:orch:broadcast           ← orchestrator fan-out            │
│  openclaw:agent:{id}:inbox         ← ALL inbound (tasks, P2P, etc.) │
│  openclaw:agent:{id}:outbox        ← ALL outbound (results, replies)│
│  openclaw:channel:{id}:{ch}:in     ← human→agent via channel        │
│  openclaw:channel:{id}:{ch}:out    ← agent→human via channel        │
│  openclaw:agent:{id}:heartbeat     ← liveness (String + TTL)        │
│  openclaw:agent:roster             ← Set of known agent IDs         │
│  openclaw:dlq                      ← dead letter queue              │
│                                                                      │
│  Consumer groups: cg-{agent}, cg-tracer, orchestrator                │
└──────────────────────────────────────────────────────────────────────┘
     │          │              │                       │
     ▼          ▼              ▼                       ▼
┌────────┐ ┌──────────┐ ┌───────────────────────┐ ┌───────────────────────┐
│Tracer  │ │Orch/CLI  │ │  Agent "alpha"        │ │  Agent "beta"         │
│(Rust)  │ │(Rust)    │ │  (sidecar + OpenClaw) │ │  (sidecar + OpenClaw) │
│        │ │          │ │                       │ │                       │
│-passive│ │-broadcast│ │ ┌───────────────────┐ │ │ ┌───────────────────┐ │
│ reader │ │-send_to  │ │ │ openclaw-agent    │ │ │ │ openclaw-agent    │ │
│-batch  │ │-DLQ sweep│ │ │ (reverse proxy)   │ │ │ │ (reverse proxy)   │ │
│ write  │ │-watch cmd│ │ │                   │ │ │ │                   │ │
│ to PG  │ │-clawmacdo│ │ │ Telegram webhook──┤ │ │ │ Telegram webhook──┤ │
└───┬────┘ └──────────┘ │ │ Webchat WS/HTTP──┤ │ │ │ Webchat WS/HTTP──┤ │
    │                   │ │ A2A endpoint─────┤ │ │ │ A2A endpoint─────┤ │
    ▼                   │ │ Redis inbox──────┤ │ │ │ Redis inbox──────┤ │
┌────────┐              │ │        │ (all go │ │ │ │        │         │ │
│Postgres│              │ │        ▼ to Redis│ │ │ │        ▼         │ │
│(trace) │              │ │ ┌─────────────┐  │ │ │ │ ┌─────────────┐  │ │
└────────┘              │ │ │OpenClaw Core│  │ │ │ │ │OpenClaw Core│  │ │
                        │ │ │(AI Agent)   │  │ │ │ │ │(AI Agent)   │  │ │
                        │ │ │localhost:8080│  │ │ │ │ │localhost:8080│  │ │
                        │ │ └─────────────┘  │ │ │ │ └─────────────┘  │ │
                        │ └───────────────────┘ │ │ └───────────────────┘ │
                        └───────────────────────┘ └───────────────────────┘
                                ▲          │
                                │   P2P    │  (sidecar-to-sidecar via
                                └──────────┘   Redis OR A2A endpoint)
```

**What each component sees:**

| Component | Redis | PostgreSQL | Telegram | Webchat | A2A |
|-----------|:-----:|:----------:|:--------:|:-------:|:---:|
| OpenClaw Core | ❌ | ❌ | ❌ | ❌ | ❌ |
| **Agent sidecar** | ✅ R/W | ❌ | ✅ webhook | ✅ proxy | ✅ endpoint |
| Orchestrator/CLI | ✅ R/W | ❌ | ❌ (reads via Redis) | ❌ (reads via Redis) | ❌ |
| Tracer | ✅ Read-only | ✅ Sole writer | ❌ | ❌ | ❌ |

**The OpenClaw core is completely isolated.** It only speaks its native HTTP API on localhost. The sidecar handles all external communication, publishes everything to Redis, and forwards to/from the core. This means:
- `clawmacdo watch --agent alpha --channel telegram` reads from Redis and sees Telegram conversations
- The tracer records every Telegram message, webchat session, and A2A exchange to PostgreSQL
- P2P between OpenClaw instances goes sidecar → Redis → sidecar, so it's fully traced

There are three Rust binaries and one shared library to build:

1. **`openclaw-agent`** — sidecar reverse proxy that wraps each OpenClaw instance (provisioned via clawmacdo)
2. **`openclaw-orchestrator`** — central coordinator + CLI (`clawmacdo` integration)
3. **`openclaw-tracer`** — dedicated consumer that records all messages to PostgreSQL

All three binaries share a common `openclaw-messaging` library crate.

**Separation of concerns:** The OpenClaw core has zero knowledge of Redis, PostgreSQL, or the messaging bus. The agent sidecar bridges all channels into Redis. The tracer passively records everything. The orchestrator/CLI observes and commands via Redis.

---

## Phase 1: Workspace Setup

### Step 1.1 — Create the Rust workspace

```bash
cargo init openclaw-messaging-ws --name openclaw-messaging-ws
cd openclaw-messaging-ws
```

Replace `Cargo.toml` with a workspace manifest:

```toml
[workspace]
members = [
    "crates/messaging",      # shared library
    "crates/orchestrator",   # orchestrator binary
    "crates/agent",          # agent binary
    "crates/tracer",         # message recorder (PostgreSQL)
]
resolver = "2"

[workspace.dependencies]
tokio = { version = "1", features = ["full"] }
fred = { version = "10", features = ["subscriber-client"] }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
uuid = { version = "1", features = ["v4", "serde"] }
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter", "json"] }
clap = { version = "4", features = ["derive"] }
anyhow = "1"
chrono = { version = "0.4", features = ["serde"] }
sqlx = { version = "0.8", features = ["runtime-tokio", "postgres", "chrono", "uuid", "json"] }
```

### Step 1.2 — Scaffold the four crates

```bash
mkdir -p crates/messaging/src
mkdir -p crates/orchestrator/src
mkdir -p crates/agent/src
mkdir -p crates/tracer/src
mkdir -p crates/tracer/migrations
```

**`crates/messaging/Cargo.toml`**:
```toml
[package]
name = "openclaw-messaging"
version = "0.1.0"
edition = "2021"

[dependencies]
tokio.workspace = true
fred.workspace = true
serde.workspace = true
serde_json.workspace = true
uuid.workspace = true
tracing.workspace = true
anyhow.workspace = true
chrono.workspace = true
```

**`crates/orchestrator/Cargo.toml`**:
```toml
[package]
name = "openclaw-orchestrator"
version = "0.1.0"
edition = "2021"

[[bin]]
name = "orchestrator"
path = "src/main.rs"

[dependencies]
openclaw-messaging = { path = "../messaging" }
tokio.workspace = true
fred.workspace = true
serde_json.workspace = true
tracing.workspace = true
tracing-subscriber.workspace = true
clap.workspace = true
anyhow.workspace = true
uuid.workspace = true
```

**`crates/agent/Cargo.toml`**:
```toml
[package]
name = "openclaw-agent"
version = "0.1.0"
edition = "2021"

[[bin]]
name = "agent"
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

# Sidecar HTTP server (receives webhooks, webchat, A2A)
axum = { version = "0.8", features = ["json"] }
tower-http = { version = "0.6", features = ["trace"] }
reqwest = { version = "0.12", features = ["json"] }
```

**`crates/tracer/Cargo.toml`** (note: `sqlx` only appears here — agents and orchestrator have zero DB dependencies):
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
sqlx.workspace = true
```

---

## Phase 2: Shared Messaging Library (`openclaw-messaging`)

This crate contains all types, Redis key helpers, and the core send/receive logic that both binaries share.

### Step 2.1 — Define the message envelope

**`crates/messaging/src/types.rs`**:

```rust
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Unique identifier for an agent or the orchestrator.
pub type AgentId = String;

/// Every message on the wire uses this envelope.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ClawMessage {
    /// Globally unique message ID.
    pub id: Uuid,
    /// Sender — either "orchestrator", an agent UUID, or "telegram:{chat_id}".
    pub from: AgentId,
    /// Routing target.
    pub to: MessageTarget,
    /// What kind of message this is.
    pub msg_type: MsgType,
    /// Arbitrary JSON payload — keeps the envelope extensible.
    pub payload: serde_json::Value,
    /// For request-reply: the original request's ID.
    pub correlation_id: Option<Uuid>,
    /// Which channel this message entered the system through.
    pub channel: MessageChannel,
    /// External session identifier (Telegram chat_id, webchat session, etc.).
    pub session_id: Option<String>,
    /// Unix milliseconds timestamp.
    pub ts: i64,
    /// Retry counter — incremented on each DLQ re-queue.
    pub retry_count: u32,
}

impl ClawMessage {
    pub fn new(
        from: AgentId,
        to: MessageTarget,
        msg_type: MsgType,
        payload: serde_json::Value,
    ) -> Self {
        Self {
            id: Uuid::new_v4(),
            from,
            to,
            msg_type,
            payload,
            correlation_id: None,
            channel: MessageChannel::Internal,
            session_id: None,
            ts: chrono::Utc::now().timestamp_millis(),
            retry_count: 0,
        }
    }

    /// Create a message originating from a human channel.
    pub fn from_channel(
        from: AgentId,
        to: MessageTarget,
        channel: MessageChannel,
        session_id: String,
        payload: serde_json::Value,
    ) -> Self {
        Self {
            id: Uuid::new_v4(),
            from,
            to,
            msg_type: MsgType::ChannelMessage,
            payload,
            correlation_id: None,
            channel,
            session_id: Some(session_id),
            ts: chrono::Utc::now().timestamp_millis(),
            retry_count: 0,
        }
    }

    /// Create a reply to an existing message, preserving correlation and channel.
    pub fn reply(
        &self,
        from: AgentId,
        msg_type: MsgType,
        payload: serde_json::Value,
    ) -> Self {
        Self {
            id: Uuid::new_v4(),
            from,
            to: MessageTarget::Agent(self.from.clone()),
            msg_type,
            payload,
            correlation_id: self.correlation_id.or(Some(self.id)),
            channel: self.channel.clone(),
            session_id: self.session_id.clone(),
            ts: chrono::Utc::now().timestamp_millis(),
            retry_count: 0,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "snake_case")]
pub enum MessageTarget {
    /// Fan-out to every agent.
    Broadcast,
    /// Deliver to a specific agent's inbox.
    Agent(String),
    /// Return to the orchestrator.
    Orchestrator,
}

/// Which external channel a message entered or will exit through.
/// The sidecar uses this to know how to deliver responses.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum MessageChannel {
    /// Internal Redis-only message (orchestrator commands, inter-agent).
    Internal,
    /// Human talking via Telegram. session_id = chat_id.
    Telegram,
    /// Human talking via OpenClaw webchat. session_id = webchat session token.
    Webchat,
    /// Human talking via WhatsApp. session_id = phone number.
    Whatsapp,
    /// Human talking via Microsoft Teams. session_id = conversation_id.
    Teams,
    /// Agent-to-agent via the A2A protocol (not Redis-native).
    A2A,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "snake_case")]
pub enum MsgType {
    /// Orchestrator assigns a task to an agent.
    Task,
    /// Agent returns a task result.
    TaskResult,
    /// Agent-to-agent direct message.
    P2P,
    /// Liveness signal.
    Heartbeat,
    /// OpenClaw skill invocation — carries the skill name.
    Skill(String),
    /// System commands: reload_skills, shutdown, etc.
    System(String),
    /// Human ↔ agent conversation message (from any channel).
    ChannelMessage,
    /// Agent's response back to a human channel.
    ChannelResponse,
}
```

**Why `channel` and `session_id` on every message:**
- The sidecar tags every inbound message with the channel it arrived on and the external session ID (Telegram chat_id, webchat session token, etc.).
- When the OpenClaw core responds, the sidecar sees the `channel` and `session_id` on the outbound message and knows *how* and *where* to deliver it — send via Telegram API, push to WebSocket, etc.
- The tracer records the channel metadata, so you can query "show me all Telegram conversations for agent alpha last week".
- The CLI can filter by channel: `clawmacdo watch --agent alpha --channel telegram`.

### Step 2.2 — Redis key helpers

**`crates/messaging/src/keys.rs`**:

```rust
/// Centralise all Redis key patterns in one place.
/// Changing a key format here updates all binaries.

// ─── Internal messaging streams ───
pub const BROADCAST_STREAM: &str = "openclaw:orch:broadcast";
pub const ROSTER_KEY: &str = "openclaw:agent:roster";
pub const DLQ_STREAM: &str = "openclaw:dlq";

pub fn agent_inbox(agent_id: &str) -> String {
    format!("openclaw:agent:{agent_id}:inbox")
}

pub fn agent_outbox(agent_id: &str) -> String {
    format!("openclaw:agent:{agent_id}:outbox")
}

pub fn agent_heartbeat(agent_id: &str) -> String {
    format!("openclaw:agent:{agent_id}:heartbeat")
}

pub fn consumer_group(agent_id: &str) -> String {
    format!("cg-{agent_id}")
}

// ─── Agent discovery metadata ───
// Each agent registers a Hash with its endpoint, cloud, region, etc.
// The orchestrator reads these to discover how to reach each agent.

pub fn agent_meta(agent_id: &str) -> String {
    format!("openclaw:agent:{agent_id}:meta")
}

// ─── Human channel streams ───
// These capture conversations between humans and agents
// via external channels (Telegram, webchat, WhatsApp, Teams).
// The sidecar writes to these AFTER forwarding to the OpenClaw core.

/// Human → Agent messages arriving via an external channel.
pub fn channel_inbound(agent_id: &str, channel: &str) -> String {
    format!("openclaw:channel:{agent_id}:{channel}:in")
}

/// Agent → Human responses going out via an external channel.
pub fn channel_outbound(agent_id: &str, channel: &str) -> String {
    format!("openclaw:channel:{agent_id}:{channel}:out")
}

/// List all channel stream keys for a given agent.
/// Used by the tracer for stream discovery.
pub fn all_channel_streams(agent_id: &str) -> Vec<String> {
    let channels = ["telegram", "webchat", "whatsapp", "teams"];
    channels
        .iter()
        .flat_map(|ch| {
            vec![
                channel_inbound(agent_id, ch),
                channel_outbound(agent_id, ch),
            ]
        })
        .collect()
}
```

### Step 2.3 — Core send/receive operations

**`crates/messaging/src/transport.rs`**:

```rust
use anyhow::Result;
use fred::prelude::*;
use tracing::{debug, error, warn};

use crate::keys;
use crate::types::ClawMessage;

/// Maximum stream length before trimming (approximate).
const MAX_STREAM_LEN: u64 = 10_000;

/// Publish a message to the broadcast stream (orchestrator → all agents).
pub async fn broadcast(redis: &RedisClient, msg: &ClawMessage) -> Result<()> {
    let payload = serde_json::to_string(msg)?;
    redis
        .xadd::<String, _, _, _, _>(
            keys::BROADCAST_STREAM,
            false,                          // not NOMKSTREAM
            ("MAXLEN", "~", MAX_STREAM_LEN.to_string().as_str()),
            "*",                            // auto-generate ID
            vec![("msg", payload.as_str())],
        )
        .await?;
    debug!(msg_id = %msg.id, "broadcast sent");
    Ok(())
}

/// Send a message directly to a specific agent's inbox.
pub async fn send_to_agent(
    redis: &RedisClient,
    target_id: &str,
    msg: &ClawMessage,
) -> Result<()> {
    let key = keys::agent_inbox(target_id);
    let payload = serde_json::to_string(msg)?;
    redis
        .xadd::<String, _, _, _, _>(
            &key,
            false,
            ("MAXLEN", "~", MAX_STREAM_LEN.to_string().as_str()),
            "*",
            vec![("msg", payload.as_str())],
        )
        .await?;
    debug!(msg_id = %msg.id, target = target_id, "sent to agent");
    Ok(())
}

/// Send a message to the orchestrator via the agent's outbox.
pub async fn send_to_orchestrator(
    redis: &RedisClient,
    agent_id: &str,
    msg: &ClawMessage,
) -> Result<()> {
    let key = keys::agent_outbox(agent_id);
    let payload = serde_json::to_string(msg)?;
    redis
        .xadd::<String, _, _, _, _>(
            &key,
            false,
            ("MAXLEN", "~", MAX_STREAM_LEN.to_string().as_str()),
            "*",
            vec![("msg", payload.as_str())],
        )
        .await?;
    debug!(msg_id = %msg.id, "sent to orchestrator outbox");
    Ok(())
}

/// Create a consumer group on a stream (idempotent).
/// Uses MKSTREAM so the stream is created if it doesn't exist.
pub async fn ensure_consumer_group(
    redis: &RedisClient,
    stream: &str,
    group: &str,
    start_from: &str, // "$" for latest, "0" to replay all history
) -> Result<()> {
    match redis
        .xgroup_create::<(), _, _, _>(stream, group, start_from, true)
        .await
    {
        Ok(_) => debug!(stream, group, "consumer group created"),
        Err(e) if e.to_string().contains("BUSYGROUP") => {
            debug!(stream, group, "consumer group already exists");
        }
        Err(e) => return Err(e.into()),
    }
    Ok(())
}

/// Register an agent in the global roster and set its heartbeat.
pub async fn register_agent(redis: &RedisClient, agent_id: &str) -> Result<()> {
    redis.sadd::<(), _, _>(keys::ROSTER_KEY, agent_id).await?;
    refresh_heartbeat(redis, agent_id).await?;
    debug!(agent_id, "agent registered in roster");
    Ok(())
}

/// Register an agent with full discovery metadata.
/// Called during installation or first startup with cloud-detected info.
pub async fn register_agent_with_meta(
    redis: &RedisClient,
    agent_id: &str,
    meta: &AgentMeta,
) -> Result<()> {
    // Add to roster Set
    redis.sadd::<(), _, _>(keys::ROSTER_KEY, agent_id).await?;

    // Write metadata Hash
    let key = keys::agent_meta(agent_id);
    redis.hset::<(), _, _>(&key, vec![
        ("agent_id",    agent_id.to_string()),
        ("endpoint",    meta.endpoint.clone()),
        ("cloud",       meta.cloud.clone()),
        ("region",      meta.region.clone()),
        ("instance_id", meta.instance_id.clone()),
        ("private_ip",  meta.private_ip.clone().unwrap_or_default()),
        ("public_ip",   meta.public_ip.clone().unwrap_or_default()),
        ("hostname",    meta.hostname.clone()),
        ("version",     meta.version.clone()),
        ("registered_at", chrono::Utc::now().to_rfc3339()),
    ]).await?;

    refresh_heartbeat(redis, agent_id).await?;
    debug!(agent_id, endpoint = %meta.endpoint, "agent registered with metadata");
    Ok(())
}

/// Agent discovery metadata — written during registration,
/// read by the orchestrator and CLI for fleet visibility.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct AgentMeta {
    pub endpoint: String,    // http://10.0.0.5:9090
    pub cloud: String,       // "do", "aws", "tencent", "byteplus", "gcp", "bare"
    pub region: String,      // "sgp1", "ap-southeast-1", etc.
    pub instance_id: String, // cloud instance ID or hostname
    pub private_ip: Option<String>,
    pub public_ip: Option<String>,
    pub hostname: String,
    pub version: String,
}

/// Read an agent's discovery metadata.
pub async fn get_agent_meta(
    redis: &RedisClient,
    agent_id: &str,
) -> Result<Option<AgentMeta>> {
    let key = keys::agent_meta(agent_id);
    let map: std::collections::HashMap<String, String> =
        redis.hgetall(&key).await?;

    if map.is_empty() {
        return Ok(None);
    }

    Ok(Some(AgentMeta {
        endpoint:    map.get("endpoint").cloned().unwrap_or_default(),
        cloud:       map.get("cloud").cloned().unwrap_or_default(),
        region:      map.get("region").cloned().unwrap_or_default(),
        instance_id: map.get("instance_id").cloned().unwrap_or_default(),
        private_ip:  map.get("private_ip").cloned().filter(|s| !s.is_empty()),
        public_ip:   map.get("public_ip").cloned().filter(|s| !s.is_empty()),
        hostname:    map.get("hostname").cloned().unwrap_or_default(),
        version:     map.get("version").cloned().unwrap_or_default(),
    }))
}

/// Discover all agents with their metadata (used by orchestrator fleet view).
pub async fn discover_all_agents(
    redis: &RedisClient,
) -> Result<Vec<(String, AgentMeta)>> {
    let roster = get_roster(redis).await?;
    let mut agents = Vec::with_capacity(roster.len());

    for agent_id in roster {
        if let Some(meta) = get_agent_meta(redis, &agent_id).await? {
            agents.push((agent_id, meta));
        }
    }

    Ok(agents)
}

/// Refresh an agent's heartbeat (SET with 60s TTL, called every 30s).
pub async fn refresh_heartbeat(redis: &RedisClient, agent_id: &str) -> Result<()> {
    let key = keys::agent_heartbeat(agent_id);
    redis
        .set::<(), _, _>(
            &key,
            "alive",
            Some(Expiration::EX(60)),
            None,
            false,
        )
        .await?;
    Ok(())
}

/// Remove an agent from the roster (graceful shutdown).
pub async fn deregister_agent(redis: &RedisClient, agent_id: &str) -> Result<()> {
    redis.srem::<(), _, _>(keys::ROSTER_KEY, agent_id).await?;
    let hb_key = keys::agent_heartbeat(agent_id);
    redis.del::<(), _>(&hb_key).await?;
    let meta_key = keys::agent_meta(agent_id);
    redis.del::<(), _>(&meta_key).await?;
    debug!(agent_id, "agent deregistered (roster + heartbeat + meta)");
    Ok(())
}

/// Get all known agent IDs from the roster.
pub async fn get_roster(redis: &RedisClient) -> Result<Vec<String>> {
    let members: Vec<String> = redis.smembers(keys::ROSTER_KEY).await?;
    Ok(members)
}

/// Decode a ClawMessage from a raw stream entry field.
pub fn decode_message(raw: &str) -> Result<ClawMessage> {
    let msg: ClawMessage = serde_json::from_str(raw)?;
    Ok(msg)
}
```

### Step 2.4 — Wire up the library module

**`crates/messaging/src/lib.rs`**:

```rust
pub mod keys;
pub mod transport;
pub mod types;

pub use keys::*;
pub use transport::*;
pub use types::*;
```

---

## Phase 3: The Agent Sidecar Binary (`openclaw-agent`)

The agent binary is a **reverse proxy sidecar** that wraps each OpenClaw instance. It intercepts ALL communication channels — Telegram webhooks, webchat, A2A from other agents, and Redis orchestrator commands — publishes everything to Redis, and forwards to/from the OpenClaw core on localhost.

```
External traffic                    Internal
───────────────                    ─────────
                  ┌──────────────────────────────────────┐
Telegram webhook ─▶  POST /webhook/telegram              │
                  │       │                              │
Webchat HTTP ────▶  POST /webchat/message                │
                  │       │                              │
A2A protocol ────▶  POST /a2a/messages                   │
                  │       │                              │
                  │       ▼                              │
                  │  ┌──────────────────┐                │
                  │  │ Channel Router   │                │
                  │  │                  │                │
                  │  │ 1. Wrap in       │   ┌──────────┐ │
                  │  │    ClawMessage   │──▶│  Redis    │ │
                  │  │ 2. Publish to    │   │  Streams  │ │
                  │  │    Redis stream  │   └──────────┘ │
                  │  │ 3. Forward to    │                │
                  │  │    OpenClaw core │                │
                  │  └───────┬──────────┘                │
                  │          ▼                           │
                  │  ┌──────────────────┐                │
                  │  │  OpenClaw Core   │                │
                  │  │  localhost:8080  │                │
                  │  └───────┬──────────┘                │
                  │          ▼                           │
                  │  ┌──────────────────┐                │
                  │  │ Outbound         │   ┌──────────┐ │
                  │  │ Dispatcher       │──▶│  Redis    │ │
                  │  │                  │   │  Streams  │ │
                  │  │ 1. Capture resp  │   └──────────┘ │
                  │  │ 2. Publish to    │                │
                  │  │    Redis stream  │                │
                  │  │ 3. Route back    │                │
                  │  │    via channel   │──▶ Telegram API│
                  │  └──────────────────┘   / WebSocket │
                  └──────────────────────────────────────┘
```

### Step 3.1 — CLI and bootstrap

**`crates/agent/src/main.rs`**:

```rust
mod channels;       // Telegram, webchat, A2A handlers
mod dispatcher;     // Outbound response routing
mod handler;        // Internal message dispatch
mod heartbeat;
mod inbox;

use anyhow::Result;
use axum::{routing::{get, post}, Router};
use clap::Parser;
use fred::prelude::*;
use openclaw_messaging as msg;
use std::sync::Arc;
use tracing::info;

#[derive(Parser, Debug)]
#[command(name = "openclaw-agent", about = "OpenClaw Agent Sidecar")]
struct Cli {
    /// Unique agent identifier (e.g. "alpha", "agent-us-east-1").
    #[arg(short, long, env = "OPENCLAW_AGENT_ID")]
    agent_id: String,

    /// Redis connection URL.
    #[arg(short, long, env = "REDIS_URL", default_value = "redis://127.0.0.1:6379")]
    redis_url: String,

    /// OpenClaw core URL (the AI agent running on localhost).
    #[arg(long, env = "OPENCLAW_CORE_URL", default_value = "http://127.0.0.1:8080")]
    core_url: String,

    /// Sidecar HTTP listen address (receives Telegram webhooks, webchat, A2A).
    #[arg(long, env = "SIDECAR_LISTEN", default_value = "0.0.0.0:9090")]
    listen: String,

    /// Telegram bot token (if this agent handles Telegram).
    #[arg(long, env = "TELEGRAM_BOT_TOKEN")]
    telegram_token: Option<String>,

    /// Replay all historical broadcast messages on startup.
    #[arg(long, default_value_t = false)]
    replay_broadcast: bool,
}

/// Shared state accessible by all HTTP handlers and background tasks.
#[derive(Clone)]
pub struct AgentState {
    pub agent_id: String,
    pub redis: Arc<RedisClient>,
    pub core_url: String,
    pub http_client: reqwest::Client,
    pub telegram_token: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter("openclaw=debug,info")
        .json()
        .init();

    let cli = Cli::parse();
    info!(
        agent_id = %cli.agent_id,
        core_url = %cli.core_url,
        listen = %cli.listen,
        "starting OpenClaw agent sidecar"
    );

    // --- Connect to Redis ---
    let config = RedisConfig::from_url(&cli.redis_url)?;
    let redis = RedisClient::new(config, None, None, None);
    redis.connect();
    redis.wait_for_connect().await?;
    info!("connected to Redis");

    let redis = Arc::new(redis);

    // --- Build shared state ---
    let state = AgentState {
        agent_id: cli.agent_id.clone(),
        redis: Arc::clone(&redis),
        core_url: cli.core_url.clone(),
        http_client: reqwest::Client::new(),
        telegram_token: cli.telegram_token.clone(),
    };

    // --- Step A: Register in roster ---
    msg::register_agent(&redis, &cli.agent_id).await?;

    // --- Step B: Create consumer groups ---
    let broadcast_start = if cli.replay_broadcast { "0" } else { "$" };
    msg::ensure_consumer_group(
        &redis,
        msg::BROADCAST_STREAM,
        &msg::consumer_group(&cli.agent_id),
        broadcast_start,
    )
    .await?;

    msg::ensure_consumer_group(
        &redis,
        &msg::agent_inbox(&cli.agent_id),
        &msg::consumer_group(&cli.agent_id),
        "0",
    )
    .await?;

    // --- Step C: Spawn background loops ---
    let heartbeat_handle = tokio::spawn(heartbeat::run(
        Arc::clone(&redis),
        cli.agent_id.clone(),
    ));

    let inbox_handle = tokio::spawn(inbox::run(state.clone()));

    // Outbound dispatcher — reads agent outbox, routes responses
    // back to the correct channel (Telegram API, WebSocket, etc.)
    let dispatcher_handle = tokio::spawn(dispatcher::run(state.clone()));

    // --- Step D: Start the sidecar HTTP server ---
    // This receives Telegram webhooks, webchat messages, and A2A requests.
    let app = Router::new()
        // Telegram webhook endpoint
        .route("/webhook/telegram", post(channels::telegram::webhook))
        // Webchat message endpoint
        .route("/webchat/message", post(channels::webchat::message))
        // A2A agent-to-agent protocol endpoint
        .route("/a2a/messages", post(channels::a2a::receive))
        // Health check
        .route("/health", get(|| async { "ok" }))
        .with_state(state.clone());

    let listener = tokio::net::TcpListener::bind(&cli.listen).await?;
    info!(listen = %cli.listen, "sidecar HTTP server started");

    // --- Step E: Run until shutdown ---
    tokio::select! {
        result = axum::serve(listener, app) => {
            if let Err(e) = result {
                tracing::error!(error = %e, "HTTP server error");
            }
        }
        _ = tokio::signal::ctrl_c() => {
            info!("shutting down...");
        }
    }

    // Graceful deregistration
    msg::deregister_agent(&redis, &cli.agent_id).await?;
    heartbeat_handle.abort();
    inbox_handle.abort();
    dispatcher_handle.abort();

    info!("agent stopped");
    Ok(())
}
```

### Step 3.2 — Heartbeat loop

**`crates/agent/src/heartbeat.rs`**:

```rust
use fred::prelude::*;
use openclaw_messaging as msg;
use std::sync::Arc;
use tracing::debug;

/// Refresh the agent's heartbeat key every 30 seconds.
/// The key has a 60s TTL, so missing two beats = detected as dead.
pub async fn run(redis: Arc<RedisClient>, agent_id: String) {
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(30));
    loop {
        interval.tick().await;
        if let Err(e) = msg::refresh_heartbeat(&redis, &agent_id).await {
            tracing::error!(error = %e, "heartbeat refresh failed");
        } else {
            debug!("heartbeat refreshed");
        }
    }
}
```

### Step 3.3 — Inbox consumer loop (broadcast + targeted + P2P)

**`crates/agent/src/inbox.rs`**:

```rust
use fred::prelude::*;
use openclaw_messaging as msg;
use tracing::{debug, error, info, warn};

use crate::handler;
use crate::AgentState;

/// Unified consumer loop.
/// Reads from BOTH the broadcast stream and the agent's private inbox
/// using XREADGROUP with blocking.
pub async fn run(state: AgentState) {
    let inbox_key = msg::agent_inbox(&state.agent_id);
    let group = msg::consumer_group(&state.agent_id);
    let consumer = state.agent_id.clone();

    info!(
        inbox = %inbox_key,
        broadcast = msg::BROADCAST_STREAM,
        "starting inbox consumer loop"
    );

    loop {
        // XREADGROUP GROUP <group> <consumer>
        //   COUNT 10 BLOCK 5000
        //   STREAMS inbox_key broadcast_key > >
        //
        // ">" means: give me new messages not yet delivered to this consumer.
        let result: Result<Vec<(String, Vec<(String, Vec<(String, String)>)>)>, _> =
            state.redis
            .xreadgroup(
                &group,
                &consumer,
                Some(10),       // COUNT — batch size
                Some(5_000),    // BLOCK — 5 second timeout
                false,          // NOACK = false → we'll XACK manually
                &[
                    (&inbox_key, ">"),
                    (msg::BROADCAST_STREAM, ">"),
                ],
            )
            .await;

        let streams = match result {
            Ok(s) => s,
            Err(e) => {
                error!(error = %e, "XREADGROUP failed");
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                continue;
            }
        };

        for (stream_name, entries) in streams {
            for (entry_id, fields) in entries {
                let raw = match fields.iter().find(|(k, _)| k == "msg") {
                    Some((_, v)) => v,
                    None => {
                        warn!(entry_id, "stream entry missing 'msg' field");
                        continue;
                    }
                };

                match msg::decode_message(raw) {
                    Ok(claw_msg) => {
                        debug!(
                            msg_id = %claw_msg.id,
                            from = %claw_msg.from,
                            msg_type = ?claw_msg.msg_type,
                            channel = ?claw_msg.channel,
                            stream = %stream_name,
                            "received message"
                        );

                        // Dispatch to handler (now receives full AgentState
                        // so it can forward to the OpenClaw core)
                        if let Err(e) = handler::handle(&state, &claw_msg).await
                        {
                            error!(
                                msg_id = %claw_msg.id,
                                error = %e,
                                "handler failed — message NOT acked"
                            );
                            continue;
                        }

                        // ACK only after successful processing
                        if let Err(e) = state.redis
                            .xack::<i64, _, _, _>(&stream_name, &group, &entry_id)
                            .await
                        {
                            error!(entry_id, error = %e, "XACK failed");
                        }
                    }
                    Err(e) => {
                        error!(error = %e, raw = %raw, "failed to decode message");
                        let _ = state.redis
                            .xack::<i64, _, _, _>(&stream_name, &group, &entry_id)
                            .await;
                    }
                }
            }
        }
    }
}
```

### Step 3.4 — Message handler (dispatch by type)

The handler now needs to deal with messages arriving via the inbox that originated from channels (e.g., an orchestrator forwarding a Telegram user's request to a different agent), plus the new `ChannelMessage` and `ChannelResponse` types.

**`crates/agent/src/handler.rs`**:

```rust
use anyhow::Result;
use fred::prelude::*;
use openclaw_messaging as msg;
use msg::types::{ClawMessage, MsgType, MessageTarget, MessageChannel};
use tracing::{info, warn, error};

use crate::AgentState;

/// Dispatch incoming messages by type.
/// This is where you plug in your OpenClaw skill execution logic.
///
/// Messages arrive here from TWO paths:
/// 1. Redis inbox loop (orchestrator tasks, P2P, system commands)
/// 2. Internally forwarded from channel handlers (if they write to the inbox)
pub async fn handle(
    state: &AgentState,
    incoming: &ClawMessage,
) -> Result<()> {
    let redis = &state.redis;
    let agent_id = &state.agent_id;

    match &incoming.msg_type {
        // ─── Task from orchestrator ───
        MsgType::Task => {
            info!(task_id = %incoming.id, "executing task");

            // Forward to OpenClaw core for AI processing
            let core_result = forward_to_core(state, incoming).await;

            let result_payload = match core_result {
                Ok(response) => serde_json::json!({
                    "status": "completed",
                    "task_id": incoming.id.to_string(),
                    "output": response,
                }),
                Err(e) => serde_json::json!({
                    "status": "failed",
                    "task_id": incoming.id.to_string(),
                    "error": e.to_string(),
                }),
            };

            // Send result back to orchestrator via outbox
            let reply = incoming.reply(
                agent_id.to_string(),
                MsgType::TaskResult,
                result_payload,
            );
            msg::send_to_orchestrator(redis, agent_id, &reply).await?;
        }

        // ─── P2P message from another agent ───
        MsgType::P2P => {
            info!(
                from = %incoming.from,
                channel = ?incoming.channel,
                "received P2P message"
            );

            // If this arrived via A2A, the channel handler already wrote
            // it to the inbox. Process it here.
            // TODO: process P2P payload — e.g. delegate to core, run skill, etc.

            // If this is a request (has correlation_id), send a reply:
            if incoming.correlation_id.is_some() {
                let reply = incoming.reply(
                    agent_id.to_string(),
                    MsgType::P2P,
                    serde_json::json!({ "ack": true }),
                );
                msg::send_to_agent(redis, &incoming.from, &reply).await?;
            }
        }

        // ─── Skill invocation ───
        MsgType::Skill(skill_name) => {
            info!(skill = %skill_name, "invoking OpenClaw skill");

            // Forward to core — the OpenClaw instance has the skill loaded
            let core_result = state.http_client
                .post(format!("{}/v1/skills/{}", state.core_url, skill_name))
                .json(&incoming.payload)
                .send()
                .await;

            let result = match core_result {
                Ok(resp) if resp.status().is_success() => {
                    resp.json::<serde_json::Value>().await.unwrap_or_default()
                }
                Ok(resp) => serde_json::json!({
                    "error": format!("skill returned {}", resp.status()),
                }),
                Err(e) => serde_json::json!({
                    "error": e.to_string(),
                }),
            };

            let reply = incoming.reply(
                agent_id.to_string(),
                MsgType::TaskResult,
                result,
            );

            // Route reply based on who sent it
            match &incoming.to {
                MessageTarget::Agent(_) => {
                    msg::send_to_agent(redis, &incoming.from, &reply).await?;
                }
                _ => {
                    msg::send_to_orchestrator(redis, agent_id, &reply).await?;
                }
            }
        }

        // ─── Channel message arriving via Redis ───
        // This happens when the orchestrator re-routes a human's message
        // from one agent to another, or when a channel message is
        // forwarded via the inbox instead of handled inline.
        MsgType::ChannelMessage => {
            info!(
                channel = ?incoming.channel,
                session = ?incoming.session_id,
                "channel message received via inbox"
            );

            // Forward to core
            let text = incoming.payload["text"].as_str().unwrap_or("");
            let session = incoming.session_id.as_deref().unwrap_or("unknown");

            let core_result = state.http_client
                .post(format!("{}/v1/chat", state.core_url))
                .json(&serde_json::json!({
                    "message": text,
                    "session_id": session,
                    "channel": format!("{:?}", incoming.channel),
                }))
                .send()
                .await;

            if let Ok(resp) = core_result {
                if let Ok(body) = resp.json::<serde_json::Value>().await {
                    let reply_text = body["response"].as_str().unwrap_or("");

                    // Build response and write to outbox
                    // The outbound dispatcher will route it to the correct channel
                    let mut reply = incoming.reply(
                        agent_id.to_string(),
                        MsgType::ChannelResponse,
                        serde_json::json!({
                            "text": reply_text,
                            "chat_id": incoming.session_id,
                        }),
                    );
                    reply.channel = incoming.channel.clone();
                    reply.session_id = incoming.session_id.clone();

                    msg::send_to_orchestrator(redis, agent_id, &reply).await?;
                }
            }
        }

        // ─── Channel response (shouldn't normally arrive in inbox) ───
        MsgType::ChannelResponse => {
            warn!("ChannelResponse arrived in inbox — expected in outbox");
        }

        // ─── System commands (broadcast from orchestrator) ───
        MsgType::System(cmd) => {
            info!(command = %cmd, "system command received");
            match cmd.as_str() {
                "reload_skills" => {
                    info!("reloading skills...");
                    // Tell the core to reload
                    let _ = state.http_client
                        .post(format!("{}/v1/admin/reload-skills", state.core_url))
                        .send()
                        .await;
                }
                "shutdown" => {
                    info!("graceful shutdown requested");
                    msg::deregister_agent(redis, agent_id).await?;
                    std::process::exit(0);
                }
                _ => warn!(command = %cmd, "unknown system command"),
            }
        }

        MsgType::Heartbeat => { /* handled by heartbeat loop */ }

        MsgType::TaskResult => {
            warn!("agent received a TaskResult — unexpected routing");
        }
    }

    Ok(())
}

/// Forward a message to the OpenClaw core and return its response.
async fn forward_to_core(
    state: &AgentState,
    msg: &ClawMessage,
) -> Result<serde_json::Value> {
    let resp = state.http_client
        .post(format!("{}/v1/tasks", state.core_url))
        .json(&serde_json::json!({
            "task_id": msg.id.to_string(),
            "payload": msg.payload,
            "from": msg.from,
            "channel": format!("{:?}", msg.channel),
            "session_id": msg.session_id,
        }))
        .send()
        .await?
        .error_for_status()?;

    let body = resp.json::<serde_json::Value>().await?;
    Ok(body)
}
```

### Step 3.5 — Channel handlers (Telegram, webchat, A2A)

These HTTP handlers receive external traffic, wrap it in a `ClawMessage` with channel metadata, publish to Redis, and forward to the OpenClaw core.

**`crates/agent/src/channels/telegram.rs`**:

```rust
use axum::{extract::State, http::StatusCode, Json};
use openclaw_messaging as msg;
use msg::types::*;
use tracing::{info, error};

use crate::AgentState;

/// Telegram webhook payload (simplified — extend for full Telegram API).
#[derive(serde::Deserialize, Debug)]
pub struct TelegramUpdate {
    pub update_id: i64,
    pub message: Option<TelegramMessage>,
}

#[derive(serde::Deserialize, Debug)]
pub struct TelegramMessage {
    pub message_id: i64,
    pub chat: TelegramChat,
    pub from: Option<TelegramUser>,
    pub text: Option<String>,
}

#[derive(serde::Deserialize, Debug)]
pub struct TelegramChat {
    pub id: i64,
    #[serde(rename = "type")]
    pub chat_type: String,
}

#[derive(serde::Deserialize, Debug)]
pub struct TelegramUser {
    pub id: i64,
    pub first_name: String,
    pub username: Option<String>,
}

/// POST /webhook/telegram
/// Telegram sends updates here. The sidecar:
/// 1. Wraps the message in a ClawMessage with channel=Telegram
/// 2. Publishes to the channel inbound stream (for tracing/CLI)
/// 3. Publishes to the agent's inbox (for the inbox loop to process)
/// 4. Forwards to the OpenClaw core for AI processing
pub async fn webhook(
    State(state): State<AgentState>,
    Json(update): Json<TelegramUpdate>,
) -> Result<StatusCode, StatusCode> {
    let tg_msg = match update.message {
        Some(m) => m,
        None => return Ok(StatusCode::OK), // ignore non-message updates
    };

    let text = tg_msg.text.unwrap_or_default();
    let chat_id = tg_msg.chat.id.to_string();
    let sender_name = tg_msg
        .from
        .as_ref()
        .map(|u| u.first_name.clone())
        .unwrap_or_else(|| "unknown".into());

    info!(
        chat_id = %chat_id,
        sender = %sender_name,
        text = %text,
        "Telegram message received"
    );

    // Build the ClawMessage envelope
    let claw_msg = ClawMessage::from_channel(
        format!("telegram:{chat_id}"),         // from = human identifier
        MessageTarget::Agent(state.agent_id.clone()),
        MessageChannel::Telegram,
        chat_id.clone(),                        // session_id = chat_id
        serde_json::json!({
            "text": text,
            "sender_name": sender_name,
            "message_id": tg_msg.message_id,
            "chat_type": tg_msg.chat.chat_type,
        }),
    );

    // 1. Publish to channel inbound stream (tracer + CLI can see this)
    let channel_key = msg::channel_inbound(&state.agent_id, "telegram");
    let payload_str = serde_json::to_string(&claw_msg).unwrap();
    let _ = state.redis.xadd::<String, _, _, _, _>(
        &channel_key, false, ("MAXLEN", "~", "10000"), "*",
        vec![("msg", payload_str.as_str())],
    ).await;

    // 2. Forward to OpenClaw core for AI processing
    let core_response = state.http_client
        .post(format!("{}/v1/chat", state.core_url))
        .json(&serde_json::json!({
            "message": text,
            "session_id": chat_id,
            "channel": "telegram",
            "metadata": {
                "sender_name": sender_name,
            }
        }))
        .send()
        .await;

    match core_response {
        Ok(resp) if resp.status().is_success() => {
            // Parse OpenClaw's response
            let body: serde_json::Value = resp.json().await.unwrap_or_default();
            let reply_text = body["response"]
                .as_str()
                .unwrap_or("I couldn't process that.");

            // 3. Build outbound ClawMessage
            let out_msg = ClawMessage::from_channel(
                state.agent_id.clone(),
                MessageTarget::Agent(format!("telegram:{chat_id}")),
                MessageChannel::Telegram,
                chat_id.clone(),
                serde_json::json!({
                    "text": reply_text,
                    "chat_id": chat_id,
                }),
            );
            // Change msg_type to ChannelResponse
            let mut out_msg = out_msg;
            out_msg.msg_type = MsgType::ChannelResponse;
            out_msg.correlation_id = Some(claw_msg.id);

            // 4. Publish to channel outbound stream
            let out_key = msg::channel_outbound(&state.agent_id, "telegram");
            let out_str = serde_json::to_string(&out_msg).unwrap();
            let _ = state.redis.xadd::<String, _, _, _, _>(
                &out_key, false, ("MAXLEN", "~", "10000"), "*",
                vec![("msg", out_str.as_str())],
            ).await;

            // 5. Send reply via Telegram API
            if let Some(ref token) = state.telegram_token {
                let _ = state.http_client
                    .post(format!(
                        "https://api.telegram.org/bot{token}/sendMessage"
                    ))
                    .json(&serde_json::json!({
                        "chat_id": chat_id,
                        "text": reply_text,
                    }))
                    .send()
                    .await;
            }
        }
        Ok(resp) => {
            error!(status = %resp.status(), "OpenClaw core returned error");
        }
        Err(e) => {
            error!(error = %e, "failed to reach OpenClaw core");
        }
    }

    Ok(StatusCode::OK)
}
```

**`crates/agent/src/channels/webchat.rs`**:

```rust
use axum::{extract::State, http::StatusCode, Json};
use openclaw_messaging as msg;
use msg::types::*;
use tracing::{info, error};

use crate::AgentState;

#[derive(serde::Deserialize)]
pub struct WebchatRequest {
    pub session_id: String,
    pub message: String,
    pub user_name: Option<String>,
}

#[derive(serde::Serialize)]
pub struct WebchatResponse {
    pub response: String,
    pub message_id: String,
}

/// POST /webchat/message
/// OpenClaw's webchat UI sends messages here. Same flow as Telegram:
/// intercept → Redis → core → Redis → respond.
pub async fn message(
    State(state): State<AgentState>,
    Json(req): Json<WebchatRequest>,
) -> Result<Json<WebchatResponse>, StatusCode> {
    info!(
        session_id = %req.session_id,
        text = %req.message,
        "webchat message received"
    );

    // Build inbound ClawMessage
    let claw_msg = ClawMessage::from_channel(
        format!("webchat:{}", req.session_id),
        MessageTarget::Agent(state.agent_id.clone()),
        MessageChannel::Webchat,
        req.session_id.clone(),
        serde_json::json!({
            "text": req.message,
            "user_name": req.user_name,
        }),
    );

    // Publish to channel inbound stream
    let channel_key = msg::channel_inbound(&state.agent_id, "webchat");
    let payload_str = serde_json::to_string(&claw_msg).unwrap();
    let _ = state.redis.xadd::<String, _, _, _, _>(
        &channel_key, false, ("MAXLEN", "~", "10000"), "*",
        vec![("msg", payload_str.as_str())],
    ).await;

    // Forward to OpenClaw core
    let core_resp = state.http_client
        .post(format!("{}/v1/chat", state.core_url))
        .json(&serde_json::json!({
            "message": req.message,
            "session_id": req.session_id,
            "channel": "webchat",
        }))
        .send()
        .await
        .map_err(|e| {
            error!(error = %e, "core unreachable");
            StatusCode::BAD_GATEWAY
        })?;

    let body: serde_json::Value = core_resp.json().await.unwrap_or_default();
    let reply_text = body["response"]
        .as_str()
        .unwrap_or("I couldn't process that.")
        .to_string();

    // Build outbound ClawMessage
    let mut out_msg = claw_msg.reply(
        state.agent_id.clone(),
        MsgType::ChannelResponse,
        serde_json::json!({
            "text": reply_text,
            "session_id": req.session_id,
        }),
    );
    out_msg.channel = MessageChannel::Webchat;
    out_msg.session_id = Some(req.session_id);

    // Publish to channel outbound stream
    let out_key = msg::channel_outbound(&state.agent_id, "webchat");
    let out_str = serde_json::to_string(&out_msg).unwrap();
    let _ = state.redis.xadd::<String, _, _, _, _>(
        &out_key, false, ("MAXLEN", "~", "10000"), "*",
        vec![("msg", out_str.as_str())],
    ).await;

    // Return directly to webchat (synchronous response)
    Ok(Json(WebchatResponse {
        response: reply_text,
        message_id: out_msg.id.to_string(),
    }))
}
```

**`crates/agent/src/channels/a2a.rs`**:

```rust
use axum::{extract::State, http::StatusCode, Json};
use openclaw_messaging as msg;
use msg::types::*;
use tracing::{info, error};

use crate::AgentState;

/// POST /a2a/messages
/// Another OpenClaw agent (via its sidecar) sends a message here
/// using the A2A protocol. The sidecar:
/// 1. Tags the message with channel=A2A
/// 2. Publishes to Redis (inbox stream, so the inbox loop picks it up)
/// 3. Forwards to the OpenClaw core if it's a task/skill request
pub async fn receive(
    State(state): State<AgentState>,
    Json(incoming): Json<ClawMessage>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    info!(
        from = %incoming.from,
        msg_type = ?incoming.msg_type,
        "A2A message received"
    );

    // Re-tag the channel as A2A (the sender might have set it differently)
    let mut claw_msg = incoming;
    claw_msg.channel = MessageChannel::A2A;

    // Write to the agent's inbox stream — the inbox consumer loop
    // will pick this up and dispatch via handler.rs
    msg::send_to_agent(&state.redis, &state.agent_id, &claw_msg)
        .await
        .map_err(|e| {
            error!(error = %e, "failed to write A2A message to inbox");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    Ok(Json(serde_json::json!({
        "status": "accepted",
        "message_id": claw_msg.id.to_string(),
    })))
}

/// Send an A2A message to another agent's sidecar HTTP endpoint.
/// This is the outbound A2A path — used when the sidecar decides
/// to route a P2P message via HTTP instead of Redis (e.g., cross-cloud
/// where direct Redis access isn't available).
pub async fn send_a2a(
    http_client: &reqwest::Client,
    target_url: &str,
    msg: &ClawMessage,
) -> anyhow::Result<()> {
    http_client
        .post(format!("{target_url}/a2a/messages"))
        .json(msg)
        .send()
        .await?
        .error_for_status()?;
    Ok(())
}
```

**`crates/agent/src/channels/mod.rs`**:

```rust
pub mod telegram;
pub mod webchat;
pub mod a2a;
```

### Step 3.6 — Outbound dispatcher

The outbound dispatcher watches the agent's outbox stream for responses that need to be routed back to external channels. When the OpenClaw core generates a response (e.g., via a task that was originally triggered by Telegram), the dispatcher reads the `channel` and `session_id` fields to know where to send it.

**`crates/agent/src/dispatcher.rs`**:

```rust
use fred::prelude::*;
use openclaw_messaging as msg;
use msg::types::*;
use tracing::{debug, error, info, warn};

use crate::AgentState;

/// Watch the agent's outbox for messages that need external delivery.
/// Internal messages (channel=Internal) are ignored — the orchestrator
/// reads those via its own outbox consumer.
pub async fn run(state: AgentState) {
    let outbox_key = msg::agent_outbox(&state.agent_id);
    let group = format!("dispatcher-{}", state.agent_id);
    let consumer = "dispatcher-0";

    // Ensure consumer group
    let _ = msg::ensure_consumer_group(
        &state.redis, &outbox_key, &group, "$",
    ).await;

    info!("outbound dispatcher started");

    loop {
        let result: Result<Vec<(String, Vec<(String, Vec<(String, String)>)>)>, _> =
            state.redis.xreadgroup(
                &group, consumer,
                Some(10), Some(5_000), false,
                &[(&outbox_key, ">")],
            ).await;

        let streams = match result {
            Ok(s) => s,
            Err(e) => {
                debug!(error = %e, "dispatcher XREADGROUP error");
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                continue;
            }
        };

        for (_stream, entries) in streams {
            for (entry_id, fields) in entries {
                if let Some((_, raw)) = fields.iter().find(|(k, _)| k == "msg") {
                    if let Ok(claw_msg) = msg::decode_message(raw) {
                        // Route based on channel
                        match &claw_msg.channel {
                            MessageChannel::Telegram => {
                                dispatch_telegram(&state, &claw_msg).await;
                            }
                            MessageChannel::Webchat => {
                                // Webchat responses are returned synchronously
                                // in the HTTP handler, so nothing to do here.
                                // But we still record the outbound in Redis.
                                debug!("webchat response already delivered inline");
                            }
                            MessageChannel::A2A => {
                                // A2A responses go back via the sender's
                                // inbox (already handled by handler.rs)
                                debug!("A2A response routed via Redis inbox");
                            }
                            MessageChannel::Internal => {
                                // Orchestrator reads these — not our job
                            }
                            _ => {
                                warn!(channel = ?claw_msg.channel, "unknown outbound channel");
                            }
                        }
                    }
                }

                // ACK regardless
                let _ = state.redis
                    .xack::<i64, _, _, _>(&outbox_key, &group, &entry_id)
                    .await;
            }
        }
    }
}

/// Send a response back to Telegram.
async fn dispatch_telegram(state: &AgentState, msg: &ClawMessage) {
    let chat_id = match &msg.session_id {
        Some(id) => id,
        None => {
            error!(msg_id = %msg.id, "Telegram response missing session_id (chat_id)");
            return;
        }
    };

    let text = msg.payload["text"]
        .as_str()
        .unwrap_or("I couldn't generate a response.");

    let token = match &state.telegram_token {
        Some(t) => t,
        None => {
            error!("TELEGRAM_BOT_TOKEN not set — can't send response");
            return;
        }
    };

    match state.http_client
        .post(format!("https://api.telegram.org/bot{token}/sendMessage"))
        .json(&serde_json::json!({
            "chat_id": chat_id,
            "text": text,
        }))
        .send()
        .await
    {
        Ok(resp) if resp.status().is_success() => {
            info!(chat_id, "Telegram response sent");
        }
        Ok(resp) => {
            error!(status = %resp.status(), "Telegram API error");
        }
        Err(e) => {
            error!(error = %e, "failed to send Telegram response");
        }
    }
}
```

---

## Phase 4: The Orchestrator Binary (`openclaw-orchestrator`)

The orchestrator manages the agent fleet: broadcasting commands, sending targeted tasks, consuming results from agent outboxes, running health checks, and operating the dead letter queue. It also provides **CLI observation commands** via `clawmacdo` integration — letting you watch Telegram conversations, webchat sessions, and P2P exchanges in real-time.

### Step 4.0 — CLI watch commands (`clawmacdo` integration)

The orchestrator can tail any Redis stream in real-time. This is how a human operator observes what's happening across all agents and channels.

**`crates/orchestrator/src/watch.rs`**:

```rust
use fred::prelude::*;
use openclaw_messaging as msg;
use msg::types::*;
use tracing::info;

/// Live-tail messages for a specific agent, optionally filtered by channel.
/// Used by: clawmacdo watch --agent alpha --channel telegram
pub async fn watch_agent(
    redis: &RedisClient,
    agent_id: &str,
    channel_filter: Option<&str>,
) {
    // Build the list of streams to watch
    let mut streams: Vec<String> = vec![
        msg::agent_inbox(agent_id),
        msg::agent_outbox(agent_id),
    ];

    // Add channel-specific streams if filtering
    if let Some(ch) = channel_filter {
        streams.push(msg::channel_inbound(agent_id, ch));
        streams.push(msg::channel_outbound(agent_id, ch));
    } else {
        // Watch all channel streams
        for ch in &["telegram", "webchat", "whatsapp", "teams"] {
            streams.push(msg::channel_inbound(agent_id, ch));
            streams.push(msg::channel_outbound(agent_id, ch));
        }
    }

    // Use a dedicated consumer group for CLI watching
    let group = "cg-cli-watch";
    for stream in &streams {
        let _ = msg::ensure_consumer_group(redis, stream, group, "$").await;
    }

    let stream_refs: Vec<(&str, &str)> = streams
        .iter()
        .map(|s| (s.as_str(), ">"))
        .collect();

    info!(agent_id, "watching streams — press Ctrl+C to stop");

    loop {
        let result: Result<Vec<(String, Vec<(String, Vec<(String, String)>)>)>, _> =
            redis.xreadgroup(
                group, "cli-0",
                Some(10), Some(5_000), false,
                &stream_refs,
            ).await;

        if let Ok(result_streams) = result {
            for (stream_name, entries) in result_streams {
                for (entry_id, fields) in entries {
                    if let Some((_, raw)) = fields.iter().find(|(k, _)| k == "msg") {
                        if let Ok(claw_msg) = msg::decode_message(raw) {
                            // Apply channel filter
                            if let Some(ch) = channel_filter {
                                let ch_enum = match ch {
                                    "telegram" => MessageChannel::Telegram,
                                    "webchat" => MessageChannel::Webchat,
                                    "whatsapp" => MessageChannel::Whatsapp,
                                    "teams" => MessageChannel::Teams,
                                    "a2a" => MessageChannel::A2A,
                                    _ => MessageChannel::Internal,
                                };
                                if claw_msg.channel != ch_enum {
                                    let _ = redis.xack::<i64, _, _, _>(
                                        &stream_name, group, &entry_id
                                    ).await;
                                    continue;
                                }
                            }

                            // Pretty-print the message
                            print_message(&stream_name, &claw_msg);
                        }
                    }
                    let _ = redis.xack::<i64, _, _, _>(
                        &stream_name, group, &entry_id
                    ).await;
                }
            }
        }
    }
}

fn print_message(stream: &str, msg: &ClawMessage) {
    let direction = if stream.contains(":in") || stream.contains(":inbox") {
        "◀─ IN "
    } else {
        "──▶ OUT"
    };

    let channel_tag = match &msg.channel {
        MessageChannel::Telegram => "[TG]",
        MessageChannel::Webchat => "[WEB]",
        MessageChannel::Whatsapp => "[WA]",
        MessageChannel::Teams => "[TEAMS]",
        MessageChannel::A2A => "[A2A]",
        MessageChannel::Internal => "[INT]",
    };

    let text = msg.payload["text"].as_str().unwrap_or("(no text)");
    let session = msg.session_id.as_deref().unwrap_or("-");

    println!(
        "{} {} {} | from={} session={} | {}",
        channel_tag, direction, msg.msg_type_str(), msg.from, session, text
    );
}
```

**Usage from `clawmacdo`:**
```bash
# Watch all traffic for agent alpha
clawmacdo watch --agent alpha

# Watch only Telegram conversations
clawmacdo watch --agent alpha --channel telegram

# Watch P2P between two agents
clawmacdo watch --agent alpha --channel a2a

# Watch all agents' Telegram traffic
clawmacdo watch --channel telegram
```

### Step 4.1 — CLI and bootstrap

**`crates/orchestrator/src/main.rs`**:

```rust
mod dlq;
mod health;
mod outbox_consumer;
mod task_router;

use anyhow::Result;
use clap::Parser;
use fred::prelude::*;
use openclaw_messaging as msg;
use std::sync::Arc;
use tracing::info;

#[derive(Parser, Debug)]
#[command(name = "openclaw-orchestrator", about = "OpenClaw Orchestrator")]
struct Cli {
    /// Redis connection URL.
    #[arg(short, long, env = "REDIS_URL", default_value = "redis://127.0.0.1:6379")]
    redis_url: String,

    /// DLQ max retries before giving up on a message.
    #[arg(long, default_value_t = 3)]
    max_retries: u32,

    /// Seconds before a pending message is considered stuck.
    #[arg(long, default_value_t = 120)]
    pending_timeout_secs: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter("openclaw=debug,info")
        .json()
        .init();

    let cli = Cli::parse();
    info!("starting OpenClaw orchestrator");

    let config = RedisConfig::from_url(&cli.redis_url)?;
    let redis = RedisClient::new(config, None, None, None);
    redis.connect();
    redis.wait_for_connect().await?;
    info!("connected to Redis");

    let redis = Arc::new(redis);

    // Spawn background loops
    let health_handle = tokio::spawn(health::run(Arc::clone(&redis)));

    let dlq_handle = tokio::spawn(dlq::run(
        Arc::clone(&redis),
        cli.max_retries,
        cli.pending_timeout_secs,
    ));

    let outbox_handle = tokio::spawn(outbox_consumer::run(Arc::clone(&redis)));

    // The task_router module exposes functions you call from your
    // own control plane (HTTP API, CLI commands, Telegram bot, etc.)
    // For now, we just keep the orchestrator alive.
    info!("orchestrator is running — press Ctrl+C to stop");
    tokio::signal::ctrl_c().await?;

    health_handle.abort();
    dlq_handle.abort();
    outbox_handle.abort();

    info!("orchestrator stopped");
    Ok(())
}
```

### Step 4.2 — Task router (orchestrator → agents)

**`crates/orchestrator/src/task_router.rs`**:

```rust
use anyhow::Result;
use fred::prelude::*;
use openclaw_messaging as msg;
use msg::types::{ClawMessage, MessageTarget, MsgType};
use tracing::info;

/// Send a task to a specific agent.
pub async fn assign_task(
    redis: &RedisClient,
    target_agent: &str,
    payload: serde_json::Value,
) -> Result<ClawMessage> {
    let msg = ClawMessage::new(
        "orchestrator".into(),
        MessageTarget::Agent(target_agent.into()),
        MsgType::Task,
        payload,
    );
    msg::send_to_agent(redis, target_agent, &msg).await?;
    info!(
        msg_id = %msg.id,
        target = target_agent,
        "task assigned"
    );
    Ok(msg)
}

/// Broadcast a system command to all agents.
pub async fn broadcast_system(
    redis: &RedisClient,
    command: &str,
    payload: serde_json::Value,
) -> Result<()> {
    let msg = ClawMessage::new(
        "orchestrator".into(),
        MessageTarget::Broadcast,
        MsgType::System(command.into()),
        payload,
    );
    msg::broadcast(redis, &msg).await?;
    info!(command, "system command broadcast");
    Ok(())
}

/// Invoke a skill on a specific agent.
pub async fn invoke_skill(
    redis: &RedisClient,
    target_agent: &str,
    skill_name: &str,
    payload: serde_json::Value,
) -> Result<ClawMessage> {
    let msg = ClawMessage::new(
        "orchestrator".into(),
        MessageTarget::Agent(target_agent.into()),
        MsgType::Skill(skill_name.into()),
        payload,
    );
    msg::send_to_agent(redis, target_agent, &msg).await?;
    info!(
        msg_id = %msg.id,
        target = target_agent,
        skill = skill_name,
        "skill invocation sent"
    );
    Ok(msg)
}
```

### Step 4.3 — Outbox consumer (collecting agent results)

**`crates/orchestrator/src/outbox_consumer.rs`**:

```rust
use fred::prelude::*;
use openclaw_messaging as msg;
use std::sync::Arc;
use tracing::{debug, error, info, warn};

/// Periodically scan all agent outboxes for results.
/// Uses the roster to discover which agents exist.
pub async fn run(redis: Arc<RedisClient>) {
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(2));

    loop {
        interval.tick().await;

        let roster = match msg::get_roster(&redis).await {
            Ok(r) => r,
            Err(e) => {
                error!(error = %e, "failed to read roster");
                continue;
            }
        };

        for agent_id in &roster {
            let outbox_key = msg::agent_outbox(agent_id);
            let group = "orchestrator";
            let consumer = "orch-main";

            // Ensure consumer group exists (idempotent)
            let _ = msg::ensure_consumer_group(&redis, &outbox_key, group, "0").await;

            // Read up to 50 messages per agent per tick
            let result: Result<Vec<(String, Vec<(String, Vec<(String, String)>)>)>, _> =
                redis
                    .xreadgroup(
                        group,
                        consumer,
                        Some(50),
                        Some(0), // don't block — non-blocking poll
                        false,
                        &[(&outbox_key, ">")],
                    )
                    .await;

            let streams = match result {
                Ok(s) => s,
                Err(_) => continue,
            };

            for (_stream, entries) in streams {
                for (entry_id, fields) in entries {
                    if let Some((_, raw)) = fields.iter().find(|(k, _)| k == "msg") {
                        match msg::decode_message(raw) {
                            Ok(claw_msg) => {
                                info!(
                                    msg_id = %claw_msg.id,
                                    from = %claw_msg.from,
                                    msg_type = ?claw_msg.msg_type,
                                    "result received from agent"
                                );

                                // TODO: route the result to your application logic
                                // e.g. update a task database, notify via Telegram, etc.

                                // ACK
                                let _ = redis
                                    .xack::<i64, _, _, _>(&outbox_key, group, &entry_id)
                                    .await;
                            }
                            Err(e) => {
                                warn!(error = %e, "bad message in outbox");
                                let _ = redis
                                    .xack::<i64, _, _, _>(&outbox_key, group, &entry_id)
                                    .await;
                            }
                        }
                    }
                }
            }
        }
    }
}
```

### Step 4.4 — Health checker (detect dead agents)

**`crates/orchestrator/src/health.rs`**:

```rust
use fred::prelude::*;
use openclaw_messaging as msg;
use std::sync::Arc;
use tracing::{info, warn};

/// Every 30 seconds, check the roster against heartbeat keys.
/// Agents whose heartbeat has expired (TTL gone) are flagged as dead.
/// Uses agent metadata for rich logging (cloud, region, endpoint).
pub async fn run(redis: Arc<RedisClient>) {
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(30));

    loop {
        interval.tick().await;

        // Use discover_all_agents to get metadata alongside roster
        let agents = match msg::discover_all_agents(&redis).await {
            Ok(a) => a,
            Err(_) => continue,
        };

        let mut alive = 0;
        let mut dead_agents: Vec<(String, Option<msg::AgentMeta>)> = Vec::new();

        for (agent_id, meta) in &agents {
            let hb_key = msg::agent_heartbeat(agent_id);
            let exists: bool = redis.exists(&hb_key).await.unwrap_or(false);

            if exists {
                alive += 1;
            } else {
                warn!(
                    agent_id,
                    cloud = %meta.cloud,
                    region = %meta.region,
                    endpoint = %meta.endpoint,
                    "agent heartbeat expired — presumed dead"
                );
                dead_agents.push((agent_id.clone(), Some(meta.clone())));
            }
        }

        // Also check roster members without metadata (legacy installs)
        let roster = msg::get_roster(&redis).await.unwrap_or_default();
        for agent_id in &roster {
            if !agents.iter().any(|(id, _)| id == agent_id) {
                let hb_key = msg::agent_heartbeat(agent_id);
                let exists: bool = redis.exists(&hb_key).await.unwrap_or(false);
                if exists {
                    alive += 1;
                } else {
                    warn!(agent_id, "agent without metadata — heartbeat expired");
                    dead_agents.push((agent_id.clone(), None));
                }
            }
        }

        // Remove dead agents from roster (keep metadata for forensics)
        for (dead_id, meta) in &dead_agents {
            let _ = redis
                .srem::<(), _, _>(msg::ROSTER_KEY, dead_id.as_str())
                .await;

            // Don't delete the meta hash — keep it for forensic queries.
            // Mark it as dead instead.
            let meta_key = msg::agent_meta(dead_id);
            let _ = redis.hset::<(), _, _>(
                &meta_key,
                vec![("status", "dead"), ("dead_at", &chrono::Utc::now().to_rfc3339())],
            ).await;

            // TODO: re-queue pending tasks, notify via Telegram
            info!(agent_id = %dead_id, "removed dead agent from roster");
        }

        info!(
            alive_count = alive,
            dead_count = dead_agents.len(),
            "health check complete"
        );
    }
}
```

### Step 4.5 — Dead letter queue (DLQ) sweep

**`crates/orchestrator/src/dlq.rs`**:

```rust
use fred::prelude::*;
use openclaw_messaging as msg;
use std::sync::Arc;
use tracing::{error, info, warn};

/// Periodically scan consumer groups for stuck messages (not ACKed
/// within `pending_timeout_secs`). Re-queue or move to the DLQ.
pub async fn run(
    redis: Arc<RedisClient>,
    max_retries: u32,
    pending_timeout_secs: u64,
) {
    let pending_timeout_ms = pending_timeout_secs * 1000;
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));

    loop {
        interval.tick().await;

        let roster = match msg::get_roster(&redis).await {
            Ok(r) => r,
            Err(_) => continue,
        };

        // Check broadcast stream PEL for each agent's consumer group
        for agent_id in &roster {
            let group = msg::consumer_group(agent_id);

            // Scan pending entries for the broadcast stream
            scan_pel(
                &redis,
                msg::BROADCAST_STREAM,
                &group,
                agent_id,
                pending_timeout_ms,
                max_retries,
            )
            .await;

            // Scan pending entries for the agent's inbox
            let inbox = msg::agent_inbox(agent_id);
            scan_pel(
                &redis,
                &inbox,
                &group,
                agent_id,
                pending_timeout_ms,
                max_retries,
            )
            .await;
        }
    }
}

async fn scan_pel(
    redis: &RedisClient,
    stream: &str,
    group: &str,
    agent_id: &str,
    timeout_ms: u64,
    max_retries: u32,
) {
    // XPENDING <stream> <group> - + 50
    // Returns entries that have been delivered but not ACKed
    let pending: Result<Vec<(String, String, u64, u64)>, _> = redis
        .xpending(stream, group, Some("-"), Some("+"), Some(50), None::<String>)
        .await;

    let entries = match pending {
        Ok(e) => e,
        Err(_) => return,
    };

    for (entry_id, _consumer, idle_ms, delivery_count) in entries {
        if idle_ms < timeout_ms {
            continue; // not stuck yet
        }

        if delivery_count as u32 >= max_retries {
            // Exceeded max retries — move to DLQ
            warn!(
                stream,
                entry_id,
                delivery_count,
                "message exceeded max retries — moving to DLQ"
            );

            // Read the message data before ACKing
            let range: Vec<(String, Vec<(String, String)>)> = redis
                .xrange(stream, &entry_id, &entry_id, Some(1))
                .await
                .unwrap_or_default();

            if let Some((_, fields)) = range.first() {
                if let Some((_, raw)) = fields.iter().find(|(k, _)| k == "msg") {
                    // Write to DLQ stream with metadata
                    let _ = redis
                        .xadd::<String, _, _, _, _>(
                            msg::DLQ_STREAM,
                            false,
                            ("MAXLEN", "~", "5000"),
                            "*",
                            vec![
                                ("msg", raw.as_str()),
                                ("original_stream", stream),
                                ("agent_id", agent_id),
                                ("delivery_count", &delivery_count.to_string()),
                            ],
                        )
                        .await;
                }
            }

            // ACK to remove from PEL
            let _ = redis.xack::<i64, _, _, _>(stream, group, &entry_id).await;
        } else {
            // Re-claim for retry (XCLAIM to self for reprocessing)
            info!(
                stream,
                entry_id,
                delivery_count,
                "re-claiming stuck message"
            );
            let _: Result<Vec<(String, Vec<(String, String)>)>, _> = redis
                .xclaim(
                    stream,
                    group,
                    agent_id,       // claim to the same agent
                    timeout_ms,     // min idle time
                    &[&entry_id],
                    None,           // IDLE
                    None,           // TIME
                    None,           // RETRYCOUNT
                    false,          // FORCE
                    false,          // JUSTID
                )
                .await;
        }
    }
}
```

---

## Phase 5: P2P Request-Reply Pattern

This is a higher-level pattern built on top of the inbox loop. An agent sends a P2P message with a `correlation_id` and waits for a reply on its own inbox.

### Step 5.1 — Correlation registry

Add this to the agent binary. It maps pending `correlation_id`s to oneshot channels so the inbox loop can wake up the waiting task.

**`crates/agent/src/correlation.rs`**:

```rust
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{oneshot, Mutex};
use uuid::Uuid;
use openclaw_messaging::types::ClawMessage;

/// Shared registry of pending request-reply correlations.
#[derive(Clone, Default)]
pub struct CorrelationRegistry {
    pending: Arc<Mutex<HashMap<Uuid, oneshot::Sender<ClawMessage>>>>,
}

impl CorrelationRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a correlation and get back a receiver to await the reply.
    pub async fn register(&self, id: Uuid) -> oneshot::Receiver<ClawMessage> {
        let (tx, rx) = oneshot::channel();
        self.pending.lock().await.insert(id, tx);
        rx
    }

    /// Try to resolve a correlation. Returns true if it was matched.
    pub async fn try_resolve(&self, msg: &ClawMessage) -> bool {
        if let Some(corr_id) = msg.correlation_id {
            let mut map = self.pending.lock().await;
            if let Some(tx) = map.remove(&corr_id) {
                let _ = tx.send(msg.clone());
                return true;
            }
        }
        false
    }

    /// Remove a timed-out correlation.
    pub async fn cancel(&self, id: &Uuid) {
        self.pending.lock().await.remove(id);
    }
}
```

### Step 5.2 — P2P request-reply helper

**`crates/agent/src/p2p.rs`**:

```rust
use anyhow::{anyhow, Result};
use fred::prelude::*;
use openclaw_messaging as msg;
use msg::types::{ClawMessage, MessageTarget, MsgType};
use std::time::Duration;
use uuid::Uuid;

use crate::correlation::CorrelationRegistry;

/// Send a P2P request to another agent and wait for a reply.
/// Times out after `timeout` duration.
pub async fn request(
    redis: &RedisClient,
    from_agent: &str,
    to_agent: &str,
    payload: serde_json::Value,
    registry: &CorrelationRegistry,
    timeout: Duration,
) -> Result<ClawMessage> {
    let correlation = Uuid::new_v4();

    // Register before sending — avoid race condition
    let rx = registry.register(correlation).await;

    // Build and send the request
    let mut req = ClawMessage::new(
        from_agent.into(),
        MessageTarget::Agent(to_agent.into()),
        MsgType::P2P,
        payload,
    );
    req.correlation_id = Some(correlation);
    msg::send_to_agent(redis, to_agent, &req).await?;

    // Wait for reply with timeout
    match tokio::time::timeout(timeout, rx).await {
        Ok(Ok(reply)) => Ok(reply),
        Ok(Err(_)) => {
            registry.cancel(&correlation).await;
            Err(anyhow!("correlation channel closed"))
        }
        Err(_) => {
            registry.cancel(&correlation).await;
            Err(anyhow!("P2P request timed out after {:?}", timeout))
        }
    }
}
```

### Step 5.3 — Integration into the inbox loop

Modify the inbox loop to check the correlation registry before dispatching to the handler. In `inbox.rs`, pass the `CorrelationRegistry` as a parameter and add this check before the handler dispatch:

```rust
// Inside the message processing section of inbox::run:

// Check if this is a reply to a pending P2P request
if registry.try_resolve(&claw_msg).await {
    debug!(msg_id = %claw_msg.id, "matched P2P correlation");
    // ACK and continue — the waiting task got the reply
    let _ = redis.xack::<i64, _, _, _>(&stream_name, &group, &entry_id).await;
    continue;
}

// Otherwise, dispatch to handler as normal
handler::handle(&state, &claw_msg).await?;
```

---

## Phase 6: The Tracer Binary (`openclaw-tracer`)

The tracer is a **passive, read-only consumer** that creates its own consumer group (`cg-tracer`) on every stream in the system. It reads every message that flows through Redis and batches writes to PostgreSQL. Agents and orchestrator are completely unaware it exists.

**Why a separate binary instead of a hook in the transport layer?** If every agent had a direct PostgreSQL connection, you'd have DB credentials scattered across every OpenClaw instance on DigitalOcean, Tencent, BytePlus — a security and operational headache. The tracer keeps PostgreSQL credentials in exactly one place.

**How consumer group isolation works:** When the tracer creates `cg-tracer` on a stream, it gets its own independent read cursor — completely separate from `cg-alpha` or `cg-beta`. Every consumer group independently receives every message. The tracer sees everything without interfering with agent delivery.

```
openclaw:orch:broadcast stream:
  ├── cg-alpha   → Agent alpha reads (independent cursor)
  ├── cg-beta    → Agent beta reads  (independent cursor)
  └── cg-tracer  → Tracer reads      (independent cursor)
                    ↓
              Each group gets ALL messages.
              They don't compete or interfere.
```

### Step 6.1 — PostgreSQL schema

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
    msg_type        VARCHAR(64)  NOT NULL,   -- 'task', 'p2p', 'skill:xxx', 'channel_message', etc.
    payload         JSONB        NOT NULL,
    retry_count     INT          NOT NULL DEFAULT 0,

    -- Channel metadata (how the message entered/exited the system)
    channel         VARCHAR(32)  NOT NULL DEFAULT 'internal',  -- 'telegram', 'webchat', 'whatsapp', 'teams', 'a2a', 'internal'
    session_id      VARCHAR(256),            -- Telegram chat_id, webchat session, phone number, etc.

    -- Which stream and entry this came from
    redis_stream    VARCHAR(256) NOT NULL,
    redis_entry_id  VARCHAR(64)  NOT NULL,

    -- Timestamps
    message_ts      TIMESTAMPTZ  NOT NULL,   -- from ClawMessage.ts (agent's clock)
    recorded_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- Deduplicate: same stream entry can't be recorded twice
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

-- "Show all Telegram conversations for agent alpha"
CREATE INDEX idx_trace_channel
    ON message_trace (channel, from_agent, recorded_at DESC)
    WHERE channel != 'internal';

-- "Show full conversation thread by session"
CREATE INDEX idx_trace_session
    ON message_trace (session_id, recorded_at)
    WHERE session_id IS NOT NULL;

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

### Step 6.2 — Stream discovery

The tracer dynamically discovers which streams to read from by polling the roster. When new agents register, the tracer picks up their inbox/outbox streams automatically — no restart needed.

**`crates/tracer/src/discovery.rs`**:

```rust
use anyhow::Result;
use fred::prelude::*;
use openclaw_messaging as msg;
use std::collections::HashSet;
use tracing::{debug, info};

#[derive(Debug, Clone)]
pub struct TracedStreams {
    pub streams: Vec<String>,
}

/// Discover all streams by reading the roster and building
/// the full list of broadcast + inbox + outbox + DLQ streams.
pub async fn discover_streams(redis: &RedisClient) -> Result<TracedStreams> {
    let mut streams = Vec::new();

    // Always-present streams
    streams.push(msg::BROADCAST_STREAM.to_string());
    streams.push(msg::DLQ_STREAM.to_string());

    // Per-agent streams (inbox + outbox + ALL channel streams)
    let roster: Vec<String> = redis.smembers(msg::ROSTER_KEY).await.unwrap_or_default();
    for agent_id in &roster {
        streams.push(msg::agent_inbox(agent_id));
        streams.push(msg::agent_outbox(agent_id));
        // Channel streams (Telegram in/out, webchat in/out, etc.)
        streams.extend(msg::all_channel_streams(agent_id));
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

### Step 6.3 — Batch writer (PostgreSQL)

Non-blocking, batched writes. The consumer loop sends trace events into a bounded channel; the writer drains the channel and flushes to PostgreSQL in batches using `UNNEST` for high throughput.

**`crates/tracer/src/writer.rs`**:

```rust
use anyhow::Result;
use chrono::{DateTime, Utc};
use openclaw_messaging::types::*;
use sqlx::PgPool;
use tokio::sync::mpsc;
use tracing::{debug, error, info};
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
    pub channel: String,
    pub session_id: Option<String>,
    pub redis_stream: String,
    pub redis_entry_id: String,
    pub message_ts: DateTime<Utc>,
}

impl TraceEvent {
    /// Convert a ClawMessage + stream metadata into a TraceEvent.
    pub fn from_message(msg: &ClawMessage, stream: &str, entry_id: &str) -> Self {
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
            MsgType::ChannelMessage => "channel_message".to_string(),
            MsgType::ChannelResponse => "channel_response".to_string(),
        };

        let channel_str = match &msg.channel {
            MessageChannel::Internal => "internal".to_string(),
            MessageChannel::Telegram => "telegram".to_string(),
            MessageChannel::Webchat => "webchat".to_string(),
            MessageChannel::Whatsapp => "whatsapp".to_string(),
            MessageChannel::Teams => "teams".to_string(),
            MessageChannel::A2A => "a2a".to_string(),
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
            channel: channel_str,
            session_id: msg.session_id.clone(),
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

/// Bulk insert using UNNEST — much faster than individual INSERTs.
async fn flush_batch(pool: &PgPool, events: &[TraceEvent]) -> Result<()> {
    if events.is_empty() {
        return Ok(());
    }

    let mut message_ids: Vec<Uuid> = Vec::with_capacity(events.len());
    let mut correlation_ids: Vec<Option<Uuid>> = Vec::with_capacity(events.len());
    let mut from_agents: Vec<String> = Vec::with_capacity(events.len());
    let mut to_types: Vec<String> = Vec::with_capacity(events.len());
    let mut to_ids: Vec<Option<String>> = Vec::with_capacity(events.len());
    let mut msg_types: Vec<String> = Vec::with_capacity(events.len());
    let mut payloads: Vec<serde_json::Value> = Vec::with_capacity(events.len());
    let mut retry_counts: Vec<i32> = Vec::with_capacity(events.len());
    let mut channels: Vec<String> = Vec::with_capacity(events.len());
    let mut session_ids: Vec<Option<String>> = Vec::with_capacity(events.len());
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
        channels.push(e.channel.clone());
        session_ids.push(e.session_id.clone());
        streams.push(e.redis_stream.clone());
        entry_ids.push(e.redis_entry_id.clone());
        timestamps.push(e.message_ts);
    }

    sqlx::query(r#"
        INSERT INTO message_trace
            (message_id, correlation_id, from_agent, to_target_type,
             to_target_id, msg_type, payload, retry_count,
             channel, session_id, redis_stream, redis_entry_id, message_ts)
        SELECT * FROM UNNEST(
            $1::UUID[], $2::UUID[], $3::VARCHAR[], $4::VARCHAR[],
            $5::VARCHAR[], $6::VARCHAR[], $7::JSONB[], $8::INT[],
            $9::VARCHAR[], $10::VARCHAR[], $11::VARCHAR[], $12::VARCHAR[],
            $13::TIMESTAMPTZ[]
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
    .bind(&channels)
    .bind(&session_ids)
    .bind(&streams)
    .bind(&entry_ids)
    .bind(&timestamps)
    .execute(pool)
    .await?;

    Ok(())
}
```

### Step 6.4 — Stream consumer (Redis side)

The consumer reads from all discovered streams using its own consumer group. It has its own independent cursor — agents don't know it exists.

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
    msg::ensure_consumer_group(redis, stream, TRACER_GROUP, "0").await
}

/// Consume from a set of streams and send trace events to the writer.
pub async fn consume_streams(
    redis: &RedisClient,
    streams: &[String],
    tx: &mpsc::Sender<TraceEvent>,
) {
    if streams.is_empty() {
        return;
    }

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
                false,
                &stream_refs,
            )
            .await;

        let result_streams = match result {
            Ok(s) => s,
            Err(e) => {
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
                        let _ = redis
                            .xack::<i64, _, _, _>(&stream_name, TRACER_GROUP, &entry_id)
                            .await;
                        continue;
                    }
                };

                match msg::decode_message(raw) {
                    Ok(claw_msg) => {
                        // Skip heartbeats — too noisy for the trace DB
                        if matches!(claw_msg.msg_type, msg::types::MsgType::Heartbeat) {
                            let _ = redis
                                .xack::<i64, _, _, _>(
                                    &stream_name, TRACER_GROUP, &entry_id
                                )
                                .await;
                            continue;
                        }

                        let event = TraceEvent::from_message(
                            &claw_msg, &stream_name, &entry_id,
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
                                    stream = %stream_name, entry_id,
                                    "writer buffer full — NOT acking, will retry"
                                );
                                // Don't ACK — stays in PEL, re-delivered next iteration
                            }
                            Err(mpsc::error::TrySendError::Closed(_)) => {
                                error!("writer channel closed — tracer shutting down");
                                return;
                            }
                        }
                    }
                    Err(e) => {
                        warn!(error = %e, stream = %stream_name, entry_id,
                            "failed to decode message — acking to skip");
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

**Backpressure without data loss:** If the writer's bounded channel fills up (PostgreSQL slow), the consumer simply doesn't ACK those messages in `cg-tracer`. Redis keeps them in the PEL (Pending Entries List). Next `XREADGROUP` iteration re-delivers them. Agents are completely unaffected.

### Step 6.5 — Tracer main

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

    /// How often to re-scan the roster for new agent streams (seconds).
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

    // ─── Redis (read-only consumer) ───
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
    let discovery_redis = Arc::clone(&redis);
    let consumer_redis = Arc::clone(&redis);

    let mut current_streams: Vec<String> = Vec::new();
    let mut consumer_handle: Option<tokio::task::JoinHandle<()>> = None;

    let mut interval = tokio::time::interval(
        std::time::Duration::from_secs(cli.discovery_interval_secs),
    );

    info!("entering discovery loop");

    loop {
        tokio::select! {
            _ = interval.tick() => {
                let discovered = match discovery::discover_streams(
                    &discovery_redis
                ).await {
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

### Step 6.6 — Useful trace queries

Once messages are flowing into PostgreSQL, you can query across all channels:

```sql
-- ─── Full Telegram conversation for a specific chat (by session_id) ───
-- This is what `clawmacdo watch --agent alpha --channel telegram` shows in real-time,
-- but here you can query historically.
SELECT
    CASE WHEN msg_type = 'channel_message' THEN '👤 Human'
         WHEN msg_type = 'channel_response' THEN '🤖 Agent'
         ELSE msg_type END AS speaker,
    payload->>'text' AS text,
    message_ts
FROM message_trace
WHERE session_id = '123456789'       -- Telegram chat_id
  AND channel = 'telegram'
ORDER BY message_ts;

-- ─── All Telegram conversations for agent alpha (last 24h) ───
SELECT DISTINCT session_id AS chat_id,
       MIN(message_ts) AS started,
       MAX(message_ts) AS last_activity,
       COUNT(*) AS message_count
FROM message_trace
WHERE channel = 'telegram'
  AND (from_agent = 'alpha' OR to_target_id = 'alpha')
  AND recorded_at > NOW() - INTERVAL '24 hours'
GROUP BY session_id
ORDER BY last_activity DESC;

-- ─── Full request-reply chain by correlation_id ───
-- Traces a request from human → Telegram → agent → P2P → reply → Telegram
SELECT redis_stream, from_agent,
       to_target_type || COALESCE(':' || to_target_id, '') AS routed_to,
       channel, session_id, msg_type, message_ts,
       payload->>'text' AS text
FROM message_trace
WHERE correlation_id = 'xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx'
ORDER BY message_ts;

-- ─── All P2P messages between alpha and beta (last 24h) ───
SELECT message_id, from_agent, channel, msg_type, payload, message_ts
FROM message_trace
WHERE msg_type = 'p2p'
  AND (
      (from_agent = 'alpha' AND to_target_id = 'beta')
   OR (from_agent = 'beta'  AND to_target_id = 'alpha')
  )
  AND recorded_at > NOW() - INTERVAL '24 hours'
ORDER BY message_ts;

-- ─── Message volume per channel per hour (dashboard) ───
SELECT date_trunc('hour', message_ts) AS hour,
       channel, msg_type, COUNT(*) AS msg_count
FROM message_trace
WHERE recorded_at > NOW() - INTERVAL '7 days'
GROUP BY hour, channel, msg_type
ORDER BY hour DESC, msg_count DESC;

-- ─── Channel breakdown per agent (which agents handle the most Telegram?) ───
SELECT from_agent, channel, COUNT(*) AS messages
FROM message_trace
WHERE msg_type IN ('channel_message', 'channel_response')
  AND recorded_at > NOW() - INTERVAL '30 days'
GROUP BY from_agent, channel
ORDER BY messages DESC;

-- ─── Slowest request-reply round trips ───
SELECT req.message_id, req.from_agent AS requester,
       req.to_target_id AS responder, req.channel,
       EXTRACT(EPOCH FROM (resp.message_ts - req.message_ts)) * 1000
           AS round_trip_ms
FROM message_trace req
JOIN message_trace resp
    ON resp.correlation_id = req.message_id
    AND resp.msg_type IN ('task_result', 'channel_response')
WHERE req.msg_type IN ('task', 'channel_message')
  AND req.recorded_at > NOW() - INTERVAL '1 hour'
ORDER BY round_trip_ms DESC
LIMIT 20;

-- ─── Most invoked skills (last 30 days) ───
SELECT msg_type, channel, COUNT(*) AS invocations,
       COUNT(DISTINCT from_agent) AS unique_callers
FROM message_trace
WHERE msg_type LIKE 'skill:%'
  AND recorded_at > NOW() - INTERVAL '30 days'
GROUP BY msg_type, channel
ORDER BY invocations DESC;

-- ─── Unresolved dead letters ───
SELECT dlq_id, message_id, agent_id, delivery_count, reason, moved_at
FROM dlq_audit
WHERE resolved_at IS NULL
ORDER BY moved_at DESC;
```

### Step 6.7 — Data retention

```sql
-- Option A: Time-based cleanup (cron job)
DELETE FROM message_trace WHERE recorded_at < NOW() - INTERVAL '90 days';

-- Option B: Partition by month (recommended for production)
-- Convert to partitioned table, drop old partitions:
-- DROP TABLE message_trace_2025_01;

-- Option C: Archive to cold storage
-- Export to Parquet/CSV → S3 → delete from PostgreSQL
```

---

## Phase 7: Testing and Local Development

### Step 7.1 — Local Redis + PostgreSQL with Docker

```bash
docker run -d --name openclaw-redis \
  -p 6379:6379 \
  redis:7-alpine \
  redis-server --appendonly yes

docker run -d --name openclaw-postgres \
  -p 5432:5432 \
  -e POSTGRES_USER=openclaw \
  -e POSTGRES_PASSWORD=openclaw \
  -e POSTGRES_DB=openclaw_trace \
  postgres:16-alpine
```

### Step 7.2 — Run the orchestrator

```bash
cd crates/orchestrator
cargo run -- --redis-url redis://127.0.0.1:6379
```

### Step 7.3 — Run two agents in separate terminals

```bash
# Terminal 1
cargo run -p openclaw-agent -- --agent-id alpha --redis-url redis://127.0.0.1:6379

# Terminal 2
cargo run -p openclaw-agent -- --agent-id beta --redis-url redis://127.0.0.1:6379
```

### Step 7.4 — Run the tracer

```bash
# Terminal 3
cargo run -p openclaw-tracer -- \
  --redis-url redis://127.0.0.1:6379 \
  --database-url postgres://openclaw:openclaw@127.0.0.1:5432/openclaw_trace
```

### Step 7.5 — Verify with `redis-cli`

```bash
# Check the roster
redis-cli SMEMBERS openclaw:agent:roster
# → 1) "alpha"
# → 2) "beta"

# Check heartbeats
redis-cli TTL openclaw:agent:alpha:heartbeat
# → 58 (or similar, counting down from 60)

# Manually inject a broadcast message
redis-cli XADD openclaw:orch:broadcast '*' msg '{"id":"00000000-0000-0000-0000-000000000001","from":"orchestrator","to":"broadcast","msg_type":{"system":"reload_skills"},"payload":{},"correlation_id":null,"ts":1700000000000,"retry_count":0}'

# Watch agent logs for the message pickup

# Verify the tracer's consumer group exists
redis-cli XINFO GROUPS openclaw:orch:broadcast
# Should show cg-alpha, cg-beta, AND cg-tracer

# Check PostgreSQL for recorded messages
psql postgres://openclaw:openclaw@127.0.0.1:5432/openclaw_trace \
  -c "SELECT message_id, from_agent, msg_type, recorded_at FROM message_trace ORDER BY recorded_at DESC LIMIT 5;"
```

### Step 7.6 — Integration test (Rust)

Create `tests/integration.rs` at the workspace root:

```rust
use openclaw_messaging as msg;
use msg::types::*;
use fred::prelude::*;

#[tokio::test]
async fn test_broadcast_and_receive() {
    let config = RedisConfig::from_url("redis://127.0.0.1:6379").unwrap();
    let redis = RedisClient::new(config, None, None, None);
    redis.connect();
    redis.wait_for_connect().await.unwrap();

    let agent_id = "test-agent";
    let group = msg::consumer_group(agent_id);

    // Setup
    msg::register_agent(&redis, agent_id).await.unwrap();
    msg::ensure_consumer_group(&redis, msg::BROADCAST_STREAM, &group, "$")
        .await
        .unwrap();

    // Send a broadcast
    let msg_out = ClawMessage::new(
        "orchestrator".into(),
        MessageTarget::Broadcast,
        MsgType::System("ping".into()),
        serde_json::json!({}),
    );
    msg::broadcast(&redis, &msg_out).await.unwrap();

    // Read it back
    let result: Vec<(String, Vec<(String, Vec<(String, String)>)>)> = redis
        .xreadgroup(&group, agent_id, Some(1), Some(1000), false,
            &[(msg::BROADCAST_STREAM, ">")])
        .await
        .unwrap();

    assert!(!result.is_empty());

    // Cleanup
    msg::deregister_agent(&redis, agent_id).await.unwrap();
}
```

---

## Phase 8: Production Deployment & Auto-Discovery

### 8.0 — One-script installer

A single shell script handles every role. It auto-detects the cloud provider, region, and instance ID from metadata APIs, generates a unique agent ID, installs dependencies, builds binaries, creates systemd services, and registers the instance in Redis with full endpoint metadata.

**Install an agent sidecar on a DigitalOcean droplet:**
```bash
curl -sSL https://your-repo/install.sh | sudo bash -s -- \
  --role agent \
  --redis-url redis://10.0.0.5:6379 \
  --telegram-token "123456:ABC-DEF"
```

**Install the orchestrator:**
```bash
curl -sSL https://your-repo/install.sh | sudo bash -s -- \
  --role orchestrator \
  --redis-url redis://10.0.0.5:6379
```

**Install the tracer:**
```bash
curl -sSL https://your-repo/install.sh | sudo bash -s -- \
  --role tracer \
  --redis-url redis://10.0.0.5:6379 \
  --database-url postgres://tracer:pass@10.0.0.5:5432/openclaw_trace
```

**Install Redis + PostgreSQL infrastructure:**
```bash
curl -sSL https://your-repo/install.sh | sudo bash -s -- --role infra
```

**All-in-one for local dev/demo:**
```bash
curl -sSL https://your-repo/install.sh | sudo bash -s -- --role all
```

### 8.0.1 — How auto-detection works

The install script probes cloud metadata APIs in order: DigitalOcean → AWS → Tencent → BytePlus → GCP → bare metal fallback. Each provider exposes instance metadata at a well-known HTTP endpoint:

| Cloud | Metadata URL | What we read |
|-------|-------------|--------------|
| DigitalOcean | `http://169.254.169.254/metadata/v1/` | id, region, interfaces |
| AWS EC2 | `http://169.254.169.254/latest/meta-data/` (IMDSv2 token) | instance-id, region, IPs |
| Tencent CVM | `http://metadata.tencentyun.com/latest/meta-data/` | instance-id, zone, IPs |
| BytePlus ECS | `http://100.96.0.96/latest/meta-data/` | instance-id, zone, IPs |
| GCP | `http://metadata.google.internal/computeMetadata/v1/` | id, zone, IPs |
| Bare metal | — | hostname, first IP from `hostname -I` |

The agent ID is generated as `{cloud}-{region}-{short_instance_id}`:
```
do-sgp1-a1b2c3d4        # DigitalOcean Singapore
aws-ap-southeast-1-i7f8e # AWS Singapore
tencent-ap-singapore-ins123 # Tencent Singapore
bare-local-myhost        # Bare metal
```

### 8.0.2 — Agent discovery via Redis metadata

During installation (and on every startup), each agent writes a discovery record to Redis:

```
Redis Key: openclaw:agent:{agent_id}:meta  (Hash)

Fields:
  agent_id      = "do-sgp1-a1b2c3d4"
  endpoint      = "http://10.0.0.5:9090"
  cloud         = "do"
  region        = "sgp1"
  instance_id   = "12345678"
  private_ip    = "10.0.0.5"
  public_ip     = "128.199.1.2"
  hostname      = "openclaw-alpha"
  version       = "0.1.0"
  registered_at = "2026-03-19T10:30:00Z"
```

**The orchestrator uses this for fleet discovery:**

```bash
# List all registered agents
redis-cli SMEMBERS openclaw:agent:roster

# Get full metadata for one agent
redis-cli HGETALL openclaw:agent:do-sgp1-a1b2c3d4:meta

# Get just the endpoint (for A2A routing)
redis-cli HGET openclaw:agent:do-sgp1-a1b2c3d4:meta endpoint

# Find all agents in a specific region
for id in $(redis-cli SMEMBERS openclaw:agent:roster); do
  region=$(redis-cli HGET "openclaw:agent:${id}:meta" region)
  endpoint=$(redis-cli HGET "openclaw:agent:${id}:meta" endpoint)
  echo "$id  $region  $endpoint"
done
```

**From Rust (orchestrator fleet view):**

```rust
// Discover all agents with metadata
let agents = msg::discover_all_agents(&redis).await?;

for (agent_id, meta) in &agents {
    println!(
        "{} → {} ({}/{})",
        agent_id, meta.endpoint, meta.cloud, meta.region
    );
}

// Output:
// do-sgp1-a1b2c3d4 → http://10.0.0.5:9090 (do/sgp1)
// tencent-ap-sg-ins456 → http://10.0.1.8:9090 (tencent/ap-singapore)
// aws-us-east-1-i7f8e → http://10.1.0.3:9090 (aws/us-east-1)
```

**From clawmacdo CLI:**

```bash
# Show fleet status with metadata
clawmacdo fleet status

# Output:
# AGENT ID                  CLOUD    REGION         ENDPOINT                STATUS   UPTIME
# do-sgp1-a1b2c3d4          do       sgp1           http://10.0.0.5:9090    alive    3d 14h
# tencent-ap-sg-ins456      tencent  ap-singapore   http://10.0.1.8:9090    alive    2d 8h
# aws-us-east-1-i7f8e       aws      us-east-1      http://10.1.0.3:9090    dead     —

# Provision a new agent and auto-install
clawmacdo deploy --cloud do --region sgp1 --size s-2vcpu-4gb
# → Creates droplet, runs install.sh --role agent, agent self-registers
```

### 8.1 — Redis configuration

```
# redis.conf additions for production
maxmemory 256mb
maxmemory-policy allkeys-lru
tcp-keepalive 300
timeout 0

# Persistence (RDB + AOF for durability)
save 900 1
save 300 10
appendonly yes
appendfsync everysec

# TLS (if Redis is exposed across clouds)
tls-port 6380
tls-cert-file /etc/redis/tls/redis.crt
tls-key-file /etc/redis/tls/redis.key
tls-ca-cert-file /etc/redis/tls/ca.crt
```

### 8.2 — clawmacdo integration points

When provisioning an OpenClaw instance via clawmacdo:

1. **Generate a unique agent ID** (e.g., `{cloud}-{region}-{short_uuid}`)
2. **Inject environment variables**: `REDIS_URL`, `OPENCLAW_AGENT_ID`, `OPENCLAW_CORE_URL`, `SIDECAR_LISTEN`, and optionally `TELEGRAM_BOT_TOKEN`
3. **Deploy both binaries**: `openclaw-agent` (sidecar) + the OpenClaw core runtime
4. **Configure Telegram webhook** (if applicable): `curl -X POST "https://api.telegram.org/bot{token}/setWebhook?url=https://{agent-domain}:9090/webhook/telegram"`
5. **Configure DNS / reverse proxy**: point the agent's domain to the sidecar port (9090), NOT the OpenClaw core port (8080)
6. **Firewall**: sidecar port 9090 open for external channels; core port 8080 localhost only; Redis port reachable from sidecar
7. **Add to Ansible playbook**: start both the OpenClaw core and agent sidecar as systemd services

### 8.3 — Systemd service files

**Agent sidecar:**

```ini
[Unit]
Description=OpenClaw Agent Sidecar
After=network.target

[Service]
Type=simple
User=openclaw
Environment=OPENCLAW_AGENT_ID=do-sgp1-a1b2c3
Environment=REDIS_URL=rediss://user:pass@redis-host:6380
Environment=OPENCLAW_CORE_URL=http://127.0.0.1:8080
Environment=SIDECAR_LISTEN=0.0.0.0:9090
Environment=TELEGRAM_BOT_TOKEN=123456:ABC-DEF
ExecStart=/usr/local/bin/openclaw-agent
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

**Tracer** (runs on infrastructure host alongside Redis + PostgreSQL):

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

### 8.4 — Observability

| Signal | Tool | How |
|--------|------|-----|
| Logs | `tracing` + JSON format | Ship to Loki / CloudWatch / stdout |
| Metrics | Redis `INFO` + `XLEN` | Prometheus exporter or custom scraper |
| Traces | `tracing-opentelemetry` | Jaeger / Tempo — trace message flow across agents |
| Alerts | Heartbeat expiry detection | Orchestrator health loop → Telegram notification |
| Audit | PostgreSQL `message_trace` | Tracer records every message permanently |
| Dashboards | PostgreSQL queries | Message volume, latency, skill usage from trace DB |

### 8.5 — Scaling considerations

| Concern | Solution |
|---------|----------|
| Single Redis bottleneck | Redis Cluster with hash tags: `{openclaw}:agent:...` forces all keys to same slot initially; shard later |
| Cross-cloud latency | Deploy Redis in the cloud with the most agents; others tolerate ~50-100ms latency |
| Stream memory growth | `MAXLEN ~10000` on XADD + periodic XTRIM from orchestrator |
| Agent overload | Orchestrator implements backpressure — check `XLEN` of agent inbox before assigning |
| Orchestrator HA | Run 2+ orchestrators; only one holds a Redis lock (`SETNX openclaw:orch:leader`) |
| Trace DB growth | Partition `message_trace` by month; drop/archive partitions older than retention window |
| Tracer throughput | Increase batch size and buffer; add connection pool capacity |

---

## Summary: What Each Instance Runs

| Component | Runs On | Redis | PostgreSQL | External Channels | Responsibilities |
|-----------|---------|:-----:|:----------:|:-----------------:|------------------|
| `openclaw-messaging` (lib) | All binaries | — | — | — | Types, keys, transport functions |
| `openclaw-agent` (sidecar) | Every OpenClaw instance | ✅ R/W | ❌ | ✅ Telegram, Webchat, A2A | Reverse proxy: intercepts all channels → Redis → OpenClaw core → Redis → channel delivery |
| `openclaw-orchestrator` | One dedicated instance (or HA pair) | ✅ R/W | ❌ | ❌ (reads via Redis) | Broadcast, targeted sends, consume outboxes, health checks, DLQ sweep, CLI watch |
| **`openclaw-tracer`** | Infrastructure host (alongside Redis) | ✅ Read-only | ✅ Sole writer | ❌ | Passive observer — records every message (including Telegram/webchat) permanently |
| OpenClaw Core | Every instance (localhost:8080) | ❌ | ❌ | ❌ | AI agent — only talks to the sidecar via HTTP on localhost |
| Redis 7+ | Shared infrastructure | — | — | — | Streams, consumer groups, roster Set, heartbeat Strings |
| PostgreSQL 16+ | Infrastructure host | — | — | — | `message_trace` (with channel + session_id), `agent_events`, `dlq_audit` |

### Message flow examples

```
Human types in Telegram:
  Telegram API → POST /webhook/telegram → Agent Sidecar
    → XADD openclaw:channel:alpha:telegram:in  (tracer sees this)
    → POST http://localhost:8080/v1/chat        (OpenClaw core processes)
    → XADD openclaw:channel:alpha:telegram:out  (tracer sees this)
    → Telegram sendMessage API                   (human sees reply)

Orchestrator assigns a task:
  clawmacdo task --agent alpha --payload '...'
    → XADD openclaw:agent:alpha:inbox           (tracer sees this)
    → Agent Sidecar inbox loop picks it up
    → Forwards to OpenClaw core if needed
    → XADD openclaw:agent:alpha:outbox           (tracer sees this)
    → Orchestrator outbox consumer reads result

Agent alpha P2P to agent beta:
  Alpha handler calls send_to_agent("beta", msg)
    → XADD openclaw:agent:beta:inbox             (tracer sees this)
    → Beta sidecar inbox loop picks it up
    → Beta replies via send_to_agent("alpha", reply)
    → XADD openclaw:agent:alpha:inbox             (tracer sees this)

CLI watches Telegram traffic:
  clawmacdo watch --agent alpha --channel telegram
    → XREADGROUP on openclaw:channel:alpha:telegram:in
    → XREADGROUP on openclaw:channel:alpha:telegram:out
    → Pretty-prints: [TG] ◀─ IN | from=telegram:123456 | Hello!
                      [TG] ──▶ OUT | from=alpha | Hi there!
```

### Build order

1. Phase 1 — Workspace scaffold (4 crates)
2. Phase 2 — `openclaw-messaging` lib (types with channel/session_id → keys with channel streams → transport)
3. Phase 3 — Agent sidecar (main → heartbeat → inbox → handler → **channel handlers** → **outbound dispatcher**)
4. Phase 4 — Orchestrator binary (main → **watch** → task_router → outbox_consumer → health → dlq)
5. Phase 5 — P2P request-reply (correlation registry → p2p helper → inbox integration)
6. Phase 6 — Tracer binary (schema with channel columns → discovery with channel streams → writer → consumer → main)
7. Phase 7 — Local testing with Docker Redis + PostgreSQL
8. Phase 8 — Production deployment via clawmacdo + Ansible (Telegram webhook setup, sidecar config)
