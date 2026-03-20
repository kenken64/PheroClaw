# OpenClaw Messaging — Security & Design Audit

Comprehensive review of the messaging stack, sidecar, tracer, and install script.
Findings are ranked by severity: **🔴 CRITICAL**, **🟠 HIGH**, **🟡 MEDIUM**, **🟢 LOW**.

---

## Security Flaws

### 🔴 SEC-01: A2A endpoint has zero authentication

**Location:** `crates/agent/src/channels/a2a.rs` — `POST /a2a/messages`

**Problem:** The A2A endpoint accepts a raw `ClawMessage` JSON body from anyone. There is no API key, mTLS, HMAC signature, or any form of authentication. An attacker who discovers the sidecar URL (which is written in plain text to `openclaw:agent:{id}:meta` in Redis) can:

- Inject arbitrary messages into any agent's inbox
- Impersonate the orchestrator by setting `from: "orchestrator"`
- Send `MsgType::System("shutdown")` to kill the agent
- Send `MsgType::Skill("../../admin/drop-db")` to path-traverse the core

**Fix:** Add mutual authentication. At minimum, validate a shared HMAC token between sidecars:

```rust
// On send: sign the message body
let sig = hmac_sha256(&shared_secret, &body_bytes);
req.header("X-Claw-Signature", hex::encode(sig));

// On receive: verify before processing
let expected = hmac_sha256(&shared_secret, &body_bytes);
if !constant_time_eq(&received_sig, &expected) {
    return Err(StatusCode::UNAUTHORIZED);
}
```

Better: use mTLS between sidecars so each agent presents a certificate issued during `clawmacdo` provisioning.

---

### 🔴 SEC-02: Telegram webhook has no secret verification

**Location:** `crates/agent/src/channels/telegram.rs` — `POST /webhook/telegram`

**Problem:** Telegram supports a `secret_token` parameter when setting the webhook. It sends this token in the `X-Telegram-Bot-Api-Secret-Token` header on every webhook request. The current handler does not check it. Anyone who knows the webhook URL can POST fake Telegram updates and:

- Inject conversations that appear to be from real Telegram users
- Trigger the AI core with attacker-controlled prompts
- Pollute the trace DB with fake messages

**Fix:**

```rust
// When setting webhook (install script or startup):
// https://api.telegram.org/bot{token}/setWebhook?url=...&secret_token=<random>

// In the handler, verify before processing:
let secret = req.headers().get("x-telegram-bot-api-secret-token");
if secret.map(|v| v.as_bytes()) != Some(expected_secret.as_bytes()) {
    return Err(StatusCode::UNAUTHORIZED);
}
```

---

### 🔴 SEC-03: `from` field is self-reported — sender spoofing

**Location:** `ClawMessage.from` in `types.rs`, consumed everywhere

**Problem:** Every message sets its own `from` field. There is no server-side enforcement. A compromised or malicious agent can:

- Set `from: "orchestrator"` and send `System("shutdown")` broadcasts
- Set `from: "agent-beta"` to impersonate another agent in P2P
- Set `from: "telegram:12345"` to impersonate a human user

The inbox loop, handler, outbox consumer, and tracer all trust `from` at face value.

**Fix:** Messages should be signed at the transport layer, or the receiving side must validate the sender:

- For Redis-native messages: when the sidecar writes to Redis, the stream key itself (`openclaw:agent:alpha:outbox`) implicitly identifies the sender. The consumer should derive `from` from the stream key, not from the message body.
- For A2A messages: the sender identity comes from mTLS certificate CN or the HMAC key identity.

Add a `verified_from: Option<AgentId>` field that only the transport layer sets (not the sender).

---

### 🔴 SEC-04: Redis has no authentication, bound to 0.0.0.0

**Location:** `install.sh` — `install_redis()` function

**Problem:** The install script sets `bind 0.0.0.0` so Redis is reachable from the VPC, but never sets `requirepass`. Any machine on the network can:

- Read all messages (including Telegram conversations, payloads, credentials)
- Write fake messages to any stream
- `FLUSHALL` to destroy everything
- `CONFIG SET` to reconfigure Redis into a backdoor

**Fix:**

```bash
# In install_redis():
REDIS_PASSWORD=$(openssl rand -hex 32)
echo "requirepass $REDIS_PASSWORD" >> "$conf"
# Update REDIS_URL to include the password:
REDIS_URL="redis://:${REDIS_PASSWORD}@127.0.0.1:6379"
```

Also add `rename-command FLUSHALL ""` and `rename-command CONFIG ""` in production.

---

### 🟠 SEC-05: Skill name used in URL path without sanitization

**Location:** `handler.rs` — `MsgType::Skill(skill_name)` handler

```rust
state.http_client
    .post(format!("{}/v1/skills/{}", state.core_url, skill_name))
```

**Problem:** If `skill_name` is `../../admin/shutdown` or `../../../etc/passwd`, it's a path traversal against the OpenClaw core. The skill name comes from untrusted input (any agent can send a Skill message).

**Fix:**

```rust
// Validate skill name: alphanumeric + hyphens + underscores only
fn is_valid_skill_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 128
        && name.chars().all(|c| c.is_alphanumeric() || c == '-' || c == '_')
}

// In handler:
MsgType::Skill(skill_name) => {
    if !is_valid_skill_name(skill_name) {
        warn!(skill_name, "rejected invalid skill name");
        return Ok(());
    }
    // ... proceed
}
```

---

### 🟠 SEC-06: `System("shutdown")` via broadcast kills all agents, no authz

**Location:** `handler.rs` — `MsgType::System("shutdown")` match arm

```rust
"shutdown" => {
    msg::deregister_agent(redis, agent_id).await?;
    std::process::exit(0);
}
```

**Problem:** Any process with write access to the broadcast stream can shut down every agent in the fleet. There is no signature, no authorization token, no verification that this came from the orchestrator.

Combined with SEC-03 (spoofable `from` field), a single compromised agent can take down the entire fleet.

**Fix:** System commands must carry a signed token that only the orchestrator can produce:

```rust
MsgType::System(cmd) => {
    // Verify the system command was signed by the orchestrator
    let sig = incoming.payload["__sig"].as_str().unwrap_or("");
    if !verify_orchestrator_signature(sig, incoming) {
        warn!("unsigned system command rejected");
        return Ok(());
    }
    // ... proceed
}
```

---

### 🟠 SEC-07: Unbounded payload size — memory bomb

**Location:** `ClawMessage.payload: serde_json::Value` — deserialized everywhere

**Problem:** There is no size limit on the `payload` field or the overall `ClawMessage`. A malicious agent or external attacker (via the unauthenticated A2A endpoint) can send a 500MB JSON payload that:

- Exhausts Redis memory (even with `maxmemory` — a single XADD can exceed it)
- Causes OOM in every consumer that deserializes it (inbox loop, tracer, orchestrator)
- Fills the PostgreSQL trace DB with a single enormous row

**Fix:** Enforce limits at every boundary:

```rust
// In transport.rs — reject oversized messages before XADD
const MAX_MESSAGE_SIZE: usize = 1_048_576; // 1MB

pub async fn send_to_agent(redis: &RedisClient, target_id: &str, msg: &ClawMessage) -> Result<()> {
    let payload = serde_json::to_string(msg)?;
    if payload.len() > MAX_MESSAGE_SIZE {
        anyhow::bail!("message exceeds max size ({} > {})", payload.len(), MAX_MESSAGE_SIZE);
    }
    // ... XADD
}

// In Axum handlers — use tower::limit::RequestBodyLimitLayer
app.layer(axum::extract::DefaultBodyLimit::max(1_048_576));
```

---

### 🟠 SEC-08: Agent ID unsanitized — Redis key injection

**Location:** `keys.rs` — all key-building functions

```rust
pub fn agent_inbox(agent_id: &str) -> String {
    format!("openclaw:agent:{agent_id}:inbox")
}
```

**Problem:** If `agent_id` contains special Redis characters (`:`, `*`, `{`, `}`, spaces, newlines), it can:

- Collide with other agents' keys (e.g., `alpha:inbox` becomes `openclaw:agent:alpha:inbox:inbox`)
- Break SCAN patterns
- In Redis Cluster, `{` and `}` control hash slot routing, potentially causing messages to land in wrong slots

The install script generates IDs like `do-sgp1-a1b2c3d4` which is safe, but the `--agent-id` override accepts any string.

**Fix:**

```rust
pub fn validate_agent_id(id: &str) -> Result<()> {
    if id.is_empty() || id.len() > 64 {
        anyhow::bail!("agent_id must be 1-64 chars");
    }
    if !id.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_') {
        anyhow::bail!("agent_id can only contain [a-zA-Z0-9_-]");
    }
    Ok(())
}

// Call on startup:
validate_agent_id(&cli.agent_id)?;
```

---

### 🟡 SEC-09: Credentials stored in plain-text env file

**Location:** `install.sh` → `$CONFIG_DIR/openclaw.env`

**Problem:** `REDIS_URL` (with password), `DATABASE_URL` (with password), `TELEGRAM_BOT_TOKEN`, and `GATEWAY_API_KEY` are all in plain text. The file is `chmod 600` and owned by the `openclaw` user, which is the right baseline, but:

- Any root process or container escape reads them
- They appear in `systemctl show openclaw-agent` environment dump
- They're logged if `tracing` ever prints environment on startup

**Fix:** Use `EnvironmentFile` with `systemd-creds` or a secrets manager:

```ini
# In systemd service:
LoadCredential=redis-url:/run/secrets/openclaw-redis-url
LoadCredential=db-url:/run/secrets/openclaw-db-url
```

Or integrate with HashiCorp Vault / cloud KMS during startup.

---

### 🟡 SEC-10: Error messages leak internal state

**Location:** Multiple handlers returning `e.to_string()` in payloads

```rust
Err(e) => serde_json::json!({
    "status": "failed",
    "error": e.to_string(),  // ← leaks stack trace, internal URLs, etc.
})
```

**Problem:** Internal error details (Redis connection strings, file paths, Rust panics) are serialized into message payloads, which are then:

- Stored permanently in the trace DB
- Visible to the orchestrator and CLI
- Potentially sent back to Telegram users if the dispatcher routes the response

**Fix:** Map errors to sanitized codes before putting them in payloads:

```rust
Err(e) => {
    error!(error = %e, "task execution failed");  // full error in logs
    serde_json::json!({
        "status": "failed",
        "error_code": "TASK_EXECUTION_FAILED",    // sanitized for wire
    })
}
```

---

### 🟡 SEC-11: No rate limiting on sidecar HTTP endpoints

**Location:** `crates/agent/src/main.rs` — Router definition

**Problem:** The Telegram webhook, webchat, and A2A endpoints have no rate limiting. An attacker can:

- Flood the Telegram webhook with thousands of fake updates per second
- Overwhelm the OpenClaw core via the webchat endpoint
- Fill Redis streams faster than the tracer can drain them

**Fix:** Add `tower::limit::RateLimitLayer` or a per-IP/per-session rate limiter:

```rust
use tower::limit::RateLimitLayer;

let app = Router::new()
    .route("/webhook/telegram", post(channels::telegram::webhook))
    .layer(RateLimitLayer::new(100, Duration::from_secs(60)));  // 100 req/min
```

---

## Design Flaws

### 🔴 DES-01: Consumer group leak on broadcast stream

**Location:** Agent startup — `ensure_consumer_group` on `BROADCAST_STREAM`

**Problem:** Each agent creates `cg-{agent_id}` on `openclaw:orch:broadcast`. When an agent dies and is removed from the roster, its consumer group **remains on the stream permanently**. After months of churn, you have hundreds of orphaned consumer groups, each with its own PEL that accumulates unacknowledged messages forever.

Redis never auto-deletes consumer groups. `XINFO GROUPS openclaw:orch:broadcast` returns an ever-growing list.

**Impact:**

- `XINFO` becomes slow
- Each orphaned PEL holds references to old stream entries, preventing memory reclamation even after XTRIM
- The DLQ sweep scans orphaned groups and re-claims entries that will never be processed

**Fix:** The health checker must clean up consumer groups for dead agents:

```rust
// In health.rs, after removing dead agent from roster:
redis.xgroup_destroy(BROADCAST_STREAM, &format!("cg-{}", dead_id)).await?;
redis.xgroup_destroy(&agent_inbox(dead_id), &format!("cg-{}", dead_id)).await?;
```

---

### 🟠 DES-02: Orchestrator outbox polling doesn't scale

**Location:** `crates/orchestrator/src/outbox_consumer.rs`

```rust
// Every 2 seconds, for EVERY agent in the roster:
for agent_id in &roster {
    let outbox_key = msg::agent_outbox(agent_id);
    redis.xreadgroup(..., &[(&outbox_key, ">")]).await;
}
```

**Problem:** With N agents, this makes N separate `XREADGROUP` calls every 2 seconds. At 100 agents, that's 50 calls/second just for outbox polling. At 500 agents, it's 250 calls/second doing nothing most of the time.

This also means results are processed sequentially — if agent #1 has 1000 pending results, agents #2-#100 wait.

**Fix:** Use a single shared outbox stream instead of per-agent outboxes:

```
openclaw:outbox (single stream)
  - All agents XADD to this stream
  - Orchestrator has one XREADGROUP with BLOCK
  - Agent ID is in the message envelope, not the stream key
```

Or, batch multiple XREADGROUP calls into a single Redis pipeline.

---

### 🟠 DES-03: Heartbeat proves sidecar liveness, not core liveness

**Location:** `heartbeat.rs` — refreshes every 30s unconditionally

**Problem:** The heartbeat loop runs in the sidecar binary. It proves the sidecar process is alive and can reach Redis. It does NOT prove:

- The OpenClaw core on `localhost:8080` is running
- The core is responsive (not stuck in a long inference)
- The Telegram bot token is valid
- Any channel handler is functioning

A core crash leaves a "healthy" sidecar happily heartbeating while all channel messages fail with `502 Bad Gateway`.

**Fix:** The heartbeat should include a liveness probe to the core:

```rust
pub async fn run(state: AgentState) {
    loop {
        interval.tick().await;

        // Probe core health
        let core_healthy = state.http_client
            .get(format!("{}/health", state.core_url))
            .timeout(Duration::from_secs(5))
            .send()
            .await
            .map(|r| r.status().is_success())
            .unwrap_or(false);

        // Write heartbeat with health status
        let key = msg::agent_heartbeat(&state.agent_id);
        let value = if core_healthy { "healthy" } else { "degraded" };
        redis.set(&key, value, Some(Expiration::EX(60)), None, false).await?;
    }
}
```

---

### 🟠 DES-04: Tracer restart gap on new agent discovery

**Location:** `crates/tracer/src/main.rs` — discovery loop

```rust
if !new_streams.is_empty() || current_streams.is_empty() {
    if let Some(handle) = consumer_handle.take() {
        handle.abort();  // ← kills the consumer
    }
    // ... restart with new stream list
}
```

**Problem:** Every time a new agent joins (every 15 seconds the tracer checks), the entire consumer task is **aborted and restarted**. During the gap:

- Messages on existing streams are not consumed
- The PEL accumulates unacknowledged entries
- Under high message volume, this creates periodic 100-200ms blackout windows

**Fix:** Don't restart the whole consumer. Spawn additional consumer tasks for new streams without touching existing ones:

```rust
// Track per-stream consumer handles
let mut stream_handles: HashMap<String, JoinHandle<()>> = HashMap::new();

// On discovery:
for new_stream in &new_streams {
    let handle = tokio::spawn(consume_single_stream(redis.clone(), new_stream.clone(), tx.clone()));
    stream_handles.insert(new_stream.clone(), handle);
}
for removed in &removed_streams {
    if let Some(h) = stream_handles.remove(removed) {
        h.abort();
    }
}
```

---

### 🟡 DES-05: Telegram handler dual-write inconsistency

**Location:** `channels/telegram.rs` — webhook handler

**Problem:** The handler does three things:

1. Write to Redis channel stream (trace)
2. Call OpenClaw core (AI processing)
3. Call Telegram API (deliver response)

If step 1 succeeds, step 2 succeeds, but step 3 fails (Telegram rate limit, network blip), the trace shows a response was generated but the user never received it. No retry, no compensation, no record of the failure.

Conversely, if step 3 succeeds but the outbound Redis write (step after 3) fails, the trace is missing the outbound message but the user received the reply.

**Fix:** Apply the outbox pattern — write the intended outbound to Redis FIRST, then have the dispatcher deliver to Telegram with retries. The Telegram handler should NOT call the Telegram API directly:

```
Webhook → Redis inbound → Core → Redis outbound → [Dispatcher reads outbound] → Telegram API
                                                   (with retry + exponential backoff)
```

---

### 🟡 DES-06: Correlation registry memory leak

**Location:** `crates/agent/src/correlation.rs`

**Problem:** `CorrelationRegistry` is a `HashMap<Uuid, oneshot::Sender>`. The `cancel()` method removes entries on timeout, but only if the `p2p::request()` function reaches the timeout branch. If the calling task is dropped before the timeout fires (e.g., the inbox loop restarts, the process receives SIGTERM), the oneshot sender sits in the map forever.

Over time with many dropped P2P requests, this leaks memory.

**Fix:** Add a periodic sweep that removes entries older than a max age:

```rust
impl CorrelationRegistry {
    pub async fn sweep_expired(&self, max_age: Duration) {
        let mut map = self.pending.lock().await;
        map.retain(|_, sender| !sender.is_closed());
    }
}

// In a background task:
loop {
    tokio::time::sleep(Duration::from_secs(60)).await;
    registry.sweep_expired(Duration::from_secs(300)).await;
}
```

---

### 🟡 DES-07: DLQ sweep scans all agents sequentially

**Location:** `crates/orchestrator/src/dlq.rs` — `run()` + `scan_pel()`

**Problem:** The DLQ sweep iterates through every agent in the roster, calls `XPENDING` on their broadcast consumer group AND their inbox — that's 2 × N Redis calls per sweep cycle. With 100 agents, the sweep takes 200+ sequential Redis round trips every 60 seconds.

Each `XPENDING` also returns up to 50 entries, each of which may trigger an `XCLAIM` or `XRANGE` — worst case 3 calls per stuck entry.

**Fix:** Pipeline the XPENDING calls:

```rust
// Use fred's pipeline support to batch all XPENDING calls
let mut pipeline = redis.pipeline();
for agent_id in &roster {
    pipeline.xpending(BROADCAST_STREAM, &consumer_group(agent_id), ...);
    pipeline.xpending(&agent_inbox(agent_id), &consumer_group(agent_id), ...);
}
let results = pipeline.all().await?;
```

Or, switch to a single consumer group for all agents on the broadcast stream (load-balanced) with a dedicated `cg-agent-pool` instead of per-agent groups. This trades the "every agent sees every broadcast" semantic for simpler PEL management.

---

### 🟡 DES-08: No orchestrator HA — leader election mentioned but not implemented

**Location:** Summary table mentions `SETNX openclaw:orch:leader`, but no code exists.

**Problem:** If two orchestrator instances run simultaneously:

- Both sweep DLQ → double XCLAIM, double re-processing
- Both poll outboxes → double processing of results
- Both run health checks → double dead-agent cleanup (mostly idempotent, but log spam)
- Both broadcast → duplicate system commands

**Fix:** Implement the leader election:

```rust
async fn try_become_leader(redis: &RedisClient, orch_id: &str) -> bool {
    let acquired: bool = redis
        .set("openclaw:orch:leader", orch_id, Some(Expiration::EX(30)), Some(SetPolicy::NX), false)
        .await
        .unwrap_or(false);
    acquired
}

async fn renew_leadership(redis: &RedisClient, orch_id: &str) -> bool {
    // Only renew if we're still the leader (CAS)
    let current: Option<String> = redis.get("openclaw:orch:leader").await.ok();
    if current.as_deref() == Some(orch_id) {
        redis.expire("openclaw:orch:leader", 30).await.unwrap_or(false);
        true
    } else {
        false
    }
}
```

---

### 🟢 DES-09: Channel stream explosion

**Location:** `keys.rs` — `all_channel_streams()`

**Problem:** Each agent gets 8 channel streams (4 channels × in/out). With 50 agents, that's 400 streams. Most are empty — an agent may only use Telegram, never WhatsApp/Teams.

The tracer's discovery creates consumer groups on all 400 streams, even empty ones. Redis handles this fine, but it's unnecessary noise.

**Fix:** Create channel streams lazily — only when the first message is written. The tracer should discover streams via `SCAN openclaw:channel:*` instead of enumerating all possible combinations:

```rust
pub async fn discover_channel_streams(redis: &RedisClient) -> Vec<String> {
    let mut cursor = 0;
    let mut streams = Vec::new();
    loop {
        let (next, keys): (u64, Vec<String>) = redis
            .scan("openclaw:channel:*", Some(100), None)
            .await.unwrap_or((0, vec![]));
        streams.extend(keys);
        cursor = next;
        if cursor == 0 { break; }
    }
    streams
}
```

---

### 🟢 DES-10: `std::process::exit(0)` on shutdown command

**Location:** `handler.rs` — `System("shutdown")`

**Problem:** `std::process::exit(0)` is an immediate, non-graceful exit. It:

- Skips `drop()` on all Rust objects (Redis connections not cleanly closed)
- Doesn't flush the tracing subscriber (last log lines lost)
- Doesn't let the heartbeat or inbox loop finish their current iteration
- Doesn't ACK the shutdown message itself (stays in PEL)

**Fix:** Use a `tokio::sync::watch` or `CancellationToken` to signal all loops to stop:

```rust
// In handler:
"shutdown" => {
    info!("graceful shutdown requested");
    shutdown_signal.send(()).ok();  // signal all tasks
    // Don't exit here — let main() handle cleanup
}

// In main():
tokio::select! {
    _ = shutdown_rx.changed() => {
        info!("shutdown signal received");
        msg::deregister_agent(&redis, &agent_id).await?;
    }
    _ = tokio::signal::ctrl_c() => { ... }
}
```

---

## Summary Table

| ID | Type | Severity | Component | Summary |
|----|------|----------|-----------|---------|
| SEC-01 | Security | 🔴 Critical | A2A handler | No authentication — anyone can inject messages |
| SEC-02 | Security | 🔴 Critical | Telegram handler | No webhook secret verification |
| SEC-03 | Security | 🔴 Critical | Message envelope | `from` field spoofable — sender impersonation |
| SEC-04 | Security | 🔴 Critical | Install script | Redis bound to 0.0.0.0 with no password |
| SEC-05 | Security | 🟠 High | Handler | Skill name path traversal in URL |
| SEC-06 | Security | 🟠 High | Handler | Unauthenticated shutdown broadcast kills fleet |
| SEC-07 | Security | 🟠 High | Transport | No payload size limit — memory bomb |
| SEC-08 | Security | 🟠 High | Keys | Agent ID unsanitized — Redis key injection |
| SEC-09 | Security | 🟡 Medium | Install script | Credentials in plain-text env file |
| SEC-10 | Security | 🟡 Medium | Handler | Error messages leak internal state |
| SEC-11 | Security | 🟡 Medium | Sidecar HTTP | No rate limiting on endpoints |
| DES-01 | Design | 🔴 Critical | Broadcast stream | Orphaned consumer groups leak forever |
| DES-02 | Design | 🟠 High | Outbox consumer | N sequential polls don't scale |
| DES-03 | Design | 🟠 High | Heartbeat | Doesn't probe core liveness |
| DES-04 | Design | 🟠 High | Tracer | Consumer restart gap on discovery |
| DES-05 | Design | 🟡 Medium | Telegram handler | Dual-write inconsistency |
| DES-06 | Design | 🟡 Medium | Correlation | HashMap memory leak on dropped tasks |
| DES-07 | Design | 🟡 Medium | DLQ sweep | Sequential scan doesn't scale |
| DES-08 | Design | 🟡 Medium | Orchestrator | Leader election not implemented |
| DES-09 | Design | 🟢 Low | Channel streams | Eager creation of empty streams |
| DES-10 | Design | 🟢 Low | Handler | `process::exit` skips graceful cleanup |

---

## Recommended Fix Priority

**Immediate (before any deployment):**

1. SEC-04 — Add `requirepass` to Redis
2. SEC-01 — Add HMAC auth to A2A endpoint
3. SEC-02 — Verify Telegram webhook secret
4. SEC-07 — Add payload size limits
5. DES-01 — Add consumer group cleanup in health checker

**Before production:**

6. SEC-03 — Implement verified sender identity
7. SEC-05 — Sanitize skill names
8. SEC-06 — Sign system commands
9. SEC-08 — Validate agent IDs
10. DES-03 — Add core health probe to heartbeat
11. DES-04 — Fix tracer discovery to not restart consumers

**Before scaling beyond 20 agents:**

12. DES-02 — Replace per-agent outbox polling
13. DES-07 — Pipeline DLQ scans
14. DES-08 — Implement leader election
