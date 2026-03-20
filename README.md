# PheroClaw

A distributed multi-agent messaging system built in Rust. PheroClaw connects AI agent instances through a shared Redis Streams bus, with support for orchestrator-to-agent task dispatch, agent-to-agent P2P communication, and human conversation channels (Telegram, webchat, WhatsApp, Teams). All messages are permanently recorded to PostgreSQL by a passive tracer.

## Architecture

```
                    Infrastructure (VPC / Private Network)
  ┌──────────────────────────────────────────────────────────────┐
  │                                                              │
  │  ┌─────────────────────────────────────────────┐             │
  │  │           Redis (Shared Bus)                │             │
  │  │  openclaw:orch:broadcast                    │             │
  │  │  openclaw:agent:{id}:inbox / :outbox        │             │
  │  │  openclaw:channel:{id}:{ch}:in / :out       │             │
  │  │  openclaw:agent:roster / :heartbeat / :meta │             │
  │  │  openclaw:acl:p2p-rules                     │             │
  │  │  openclaw:dlq                               │             │
  │  └──────┬──────────┬──────────┬────────────────┘             │
  │         │          │          │                              │
  │    ┌────┘          │          └────────┐                     │
  │    ▼               ▼                   ▼                     │
  │ ┌──────────┐ ┌──────────────┐ ┌──────────────┐              │
  │ │Orchestr. │ │   Gateway    │ │   Tracer     │              │
  │ │          │ │ (identity    │ │  (passive)   │              │
  │ │          │ │  binding,    │ │              │              │
  │ │• broadcast│ │  ACL, relay) │ │• reads all   │              │
  │ │• send_to │ │              │ │  streams     │              │
  │ │• DLQ     │ │• anti-spoof  │ │• batch INS   │              │
  │ │• ACL mgmt│ │• P2P ACL     │ │  to PG       │              │
  │ │• fleet   │ │• Redis relay │ │              │              │
  │ └──────────┘ └──────┬───────┘ └──────┬───────┘              │
  │                     │                │                      │
  │                     │         ┌──────▼───────┐              │
  │                     │         │  PostgreSQL  │              │
  │                     │         │  (trace DB)  │              │
  │                     │         └──────────────┘              │
  └─────────────────────┼──────────────────────────────────────┘
                        │ HTTPS (API key → identity binding)
          ┌─────────────┼─────────────┐
          ▼             ▼             ▼
   ┌──────────────┐ ┌──────────────┐ ┌──────────────┐
   │ Agent alpha  │ │ Agent beta   │ │ Agent gamma  │
   │ (DO SGP)     │ │ (AWS NYC)    │ │ (Tencent)    │
   │ groups: nlp, │ │ groups:      │ │ groups:      │
   │   chat       │ │   search     │ │   chat       │
   │              │ │              │ │              │
   │ • Gateway +  │ │ • Gateway +  │ │ • Gateway +  │
   │   API key    │ │   API key    │ │   API key    │
   │ • Telegram   │ │ • Webchat    │ │ • A2A        │
   │ • Webchat    │ │ • A2A        │ │              │
   │      │       │ │      │       │ │      │       │
   │      ▼       │ │      ▼       │ │      ▼       │
   │ ┌──────────┐ │ │ ┌──────────┐ │ │ ┌──────────┐ │
   │ │ OpenClaw │ │ │ │ OpenClaw │ │ │ │ OpenClaw │ │
   │ │  Core    │ │ │ │  Core    │ │ │ │  Core    │ │
   │ │ :8080    │ │ │ │ :8080    │ │ │ │ :8080    │ │
   │ └──────────┘ │ │ └──────────┘ │ │ └──────────┘ │
   └──────────────┘ └──────────────┘ └──────────────┘
```

**Key security principle: Agents NEVER connect directly to Redis.** All agent communication is mediated by the gateway, which binds API keys to agent identities, enforces P2P ACL rules, and relays messages to/from Redis streams. Only infrastructure components (gateway, orchestrator, tracer) have direct Redis access within the VPC.

**Separation of concerns:**

| Component | Redis | Gateway | PostgreSQL | External Channels |
|-----------|:-----:|:-------:|:----------:|:-----------------:|
| OpenClaw Core (AI) | - | - | - | - |
| Agent Sidecar | - | API key | - | Telegram, Webchat, A2A |
| Gateway | R/W | — | - | - |
| Orchestrator / CLI | R/W | - | - | - |
| Tracer | Read-only | - | Sole writer | - |

The OpenClaw AI core is completely isolated — it only speaks HTTP on localhost. The agent sidecar handles all external communication, sends messages through the gateway, and forwards to/from the core.

## Components

### `pheroclaw-messaging` (shared library)

Common types, Redis key helpers, transport functions, and ACL shared by all binaries:

- **Message envelope** (`ClawMessage`) — UUID, routing target, message type, channel metadata, correlation ID
- **Redis key patterns** — broadcast, inbox/outbox, channel streams, heartbeat, roster, DLQ, ACL rules
- **Transport** — `broadcast()`, `send_to_agent()`, `send_to_orchestrator()`, `broadcast_to_group()`, consumer group management
- **Agent discovery** — roster registration, heartbeat refresh, metadata storage with group membership
- **ACL** — group-based P2P rules, agent group management, verdict cache with 5s TTL

### `pheroclaw-agent` (sidecar binary)

A reverse proxy that wraps each OpenClaw instance. Connects to Redis exclusively through the gateway:

- **Gateway connection** — authenticates with API key, sends/receives messages via gateway HTTP API
- **Telegram** — receives webhooks at `POST /webhook/telegram`, wraps in `ClawMessage`, sends through gateway, forwards to core, sends reply via Telegram API
- **Webchat** — `POST /webchat/message` endpoint for the OpenClaw web UI
- **A2A** — `POST /a2a/messages` for agent-to-agent communication
- **Heartbeat** — refreshes via gateway every 30s (60s TTL, missing two beats = detected dead)
- **Outbound dispatcher** — receives messages from gateway and routes responses back via the correct channel
- **Defense-in-depth** — rejects `Task`/`System`/`Skill` messages from non-orchestrator senders

### `pheroclaw-gateway` (Redis proxy binary)

Deployed alongside Redis in the VPC. The only entry point for agents into the Redis bus:

- **Identity binding** — API keys map to agent IDs (`key=agent-id`); `msg.from` is overwritten with the verified identity (anti-spoofing)
- **P2P ACL enforcement** — checks group-based rules before relaying P2P messages; returns 403 on deny
- **Broadcast control** — only the orchestrator identity can broadcast; supports group-targeted broadcast
- **Agent identity verification** — register/heartbeat/deregister/poll verify that request `agent_id` matches the authenticated identity
- **Redis relay** — translates gateway HTTP API calls to Redis stream operations

### `pheroclaw-orchestrator` (coordinator binary)

Central coordinator and CLI (`clawmacdo`), direct Redis access within the VPC:

- **Broadcast** — fan-out commands to all agents or to specific groups
- **Targeted dispatch** — assign tasks to specific agents
- **ACL management** — add/remove P2P rules between groups, assign groups to agents
- **Fleet management** — monitor agent roster, heartbeats, metadata
- **DLQ sweep** — re-queue or discard dead-lettered messages
- **Watch mode** — `clawmacdo watch --agent alpha --channel telegram` to observe live conversations

### `pheroclaw-tracer` (recorder binary)

A passive consumer that records all messages to PostgreSQL:

- **Stream discovery** — dynamically discovers agent streams from the roster
- **Consumer group** — uses its own `cg-tracer` group; agents are unaware of the tracer
- **Batch writer** — flushes to PostgreSQL every 500ms or every 200 messages
- **Tables** — `message_trace` (all messages), `agent_events` (join/leave/heartbeat-lost), `dlq_audit` (dead letters)

## Access Control (ACL)

PheroClaw uses group-based access control for P2P communication and targeted broadcast.

### Concepts

- **Groups**: Each agent belongs to one or more groups (e.g., `"nlp"`, `"search"`, `"chat"`). Declared at registration time.
- **P2P rules**: Bidirectional allow-pairs between groups. `("nlp", "search")` means agents in group `nlp` can P2P agents in group `search` and vice versa. Default stance: deny unless a matching rule exists.
- **Broadcast groups**: The orchestrator can broadcast to specific groups. Only agents in those groups receive the message.
- **Identity binding**: API keys map to agent IDs. The gateway overwrites `msg.from` with the verified identity, preventing spoofing.

### Example

```
Agent groups:
  agent-alpha  -> ["nlp", "chat"]
  agent-beta   -> ["search"]
  agent-gamma  -> ["chat"]

P2P rules (bidirectional):
  nlp <-> search    (alpha can talk to beta)
  nlp <-> chat      (alpha can talk to gamma)
  search <-> chat   NOT configured — beta cannot talk to gamma

Broadcast:
  orchestrator broadcasts to group "nlp"
    -> delivered to: agent-alpha (has "nlp")
    -> NOT delivered to: agent-beta, agent-gamma
```

### Enforcement flow

```
Agent A (group: nlp) sends P2P to Agent B (group: search):

1. Gateway auth: API key -> "agent-alpha"
2. Anti-spoof: overwrite msg.from = "agent-alpha"
3. ACL check:
   a. Look up sender groups: ["nlp", "chat"]
   b. Look up target groups: ["search"]
   c. Check rules: any pair in p2p-rules?
      "nlp:search" -> YES, found
   d. ALLOW
4. Route to Redis
```

### Redis keys

| Key | Type | Purpose |
|-----|------|---------|
| `openclaw:agent:{id}:meta` | Hash | `groups` field (comma-separated) |
| `openclaw:acl:p2p-rules` | Set | Normalized pairs: `"chat:nlp"`, `"nlp:search"` |
| `openclaw:acl:default-stance` | String | `"allow"` or `"deny"` |

### Gateway API key format

```bash
# Old format (bare keys):
GATEWAY_API_KEYS=key1,key2,key3

# New format (identity binding):
GATEWAY_API_KEYS=key1=agent-alpha,key2=agent-beta,orch-key=orchestrator

# Bare keys still work (key maps to itself for backwards compat)
```

## Message Flow

1. External message arrives (Telegram webhook, webchat HTTP, A2A request)
2. Agent sidecar wraps it in a `ClawMessage` with channel + session metadata
3. Sidecar sends message to the **gateway** via HTTP with API key auth
4. Gateway validates identity, overwrites `msg.from`, enforces ACL
5. Gateway publishes to Redis (channel stream + inbox)
6. Sidecar forwards to OpenClaw core on localhost for AI processing
7. Core responds; sidecar sends response through gateway to Redis (channel stream + outbox)
8. Sidecar delivers response via the original channel (Telegram API, HTTP response, etc.)
9. Tracer passively records all stream entries to PostgreSQL

## Redis Key Layout

| Key Pattern | Type | Purpose |
|-------------|------|---------|
| `openclaw:orch:broadcast` | Stream | Orchestrator fan-out to all agents |
| `openclaw:agent:{id}:inbox` | Stream | Targeted messages to a specific agent |
| `openclaw:agent:{id}:outbox` | Stream | Agent responses back to orchestrator |
| `openclaw:channel:{id}:{ch}:in` | Stream | Human-to-agent via external channel |
| `openclaw:channel:{id}:{ch}:out` | Stream | Agent-to-human via external channel |
| `openclaw:agent:{id}:heartbeat` | String (TTL) | Liveness signal (60s TTL) |
| `openclaw:agent:roster` | Set | All known agent IDs |
| `openclaw:agent:{id}:meta` | Hash | Agent discovery metadata + groups |
| `openclaw:acl:p2p-rules` | Set | Allowed P2P group pairs |
| `openclaw:acl:default-stance` | String | Default ACL stance (allow/deny) |
| `openclaw:dlq` | Stream | Dead letter queue |

## Installation

PheroClaw includes a single-script installer inspired by k3s and Tailscale:

```bash
# Install an agent sidecar (connects via gateway, not directly to Redis)
curl -sfL https://get.openclaw.dev | \
  OPENCLAW_ROLE=agent \
  GATEWAY_URL=https://gateway.example.com:8443 \
  GATEWAY_API_KEY=your-api-key sh

# Install the gateway (deployed in VPC alongside Redis)
curl -sfL https://get.openclaw.dev | \
  OPENCLAW_ROLE=gateway \
  REDIS_URL=redis://10.0.0.5:6379 sh

# Install the orchestrator (direct Redis access within VPC)
curl -sfL https://get.openclaw.dev | \
  OPENCLAW_ROLE=orchestrator \
  REDIS_URL=redis://10.0.0.5:6379 sh

# Install the tracer
curl -sfL https://get.openclaw.dev | \
  OPENCLAW_ROLE=tracer \
  REDIS_URL=redis://10.0.0.5:6379 \
  DATABASE_URL=postgres://user:pass@10.0.0.5:5432/openclaw_trace sh

# Install everything on one box (dev/demo)
curl -sfL https://get.openclaw.dev | OPENCLAW_ROLE=all sh

# Uninstall
/opt/openclaw/uninstall.sh
```

### Roles

| Role | Installs | Requires | Redis Access |
|------|----------|----------|:------------:|
| `agent` | Agent sidecar binary | `GATEWAY_URL`, `GATEWAY_API_KEY` | Via gateway |
| `gateway` | Gateway binary | `REDIS_URL` | Direct |
| `orchestrator` | Orchestrator binary | `REDIS_URL` | Direct |
| `tracer` | Tracer binary | `REDIS_URL`, `DATABASE_URL` | Direct |
| `infra` | Redis + PostgreSQL only | — | — |
| `all` | All binaries + Redis + PostgreSQL | — | All |

### Cloud Detection

The installer auto-detects the cloud environment (DigitalOcean, AWS, Tencent, BytePlus, GCP, or bare metal) and generates an agent ID from the cloud provider, region, and instance ID.

## Multi-Cloud Setup Guide

This guide walks through deploying one orchestrator with two OpenClaw agents on different cloud providers. The example uses DigitalOcean (Singapore) and AWS (us-east-1), but any combination works.

### Network topology

```
┌─────────────────────────────────────────────────────┐
│ DigitalOcean SGP1 (VPC: 10.104.0.0/20)             │
│                                                     │
│  ┌─────────┐  ┌───────────┐  ┌──────────────────┐  │
│  │ Redis   │  │ Gateway   │  │ Orchestrator     │  │
│  │ :6379   │  │ :8443     │  │                  │  │
│  │ (VPC)   │  │ (public)  │  │                  │  │
│  └─────────┘  └─────┬─────┘  └──────────────────┘  │
│                      │                              │
│              ┌───────┴────────┐                     │
│              │ Agent alpha    │                     │
│              │ groups: ["nlp"]│                     │
│              │ :9090          │                     │
│              │     ↓          │                     │
│              │ OpenClaw :8080 │                     │
│              └────────────────┘                     │
└─────────────────────────────────────────────────────┘
                       │ HTTPS (public IP or VPN)
                       │
┌──────────────────────┼──────────────────────────────┐
│ AWS us-east-1        │                              │
│                      │                              │
│              ┌───────┴────────┐                     │
│              │ Agent beta     │                     │
│              │ groups:        │                     │
│              │  ["search"]   │                     │
│              │ :9090          │                     │
│              │     ↓          │                     │
│              │ OpenClaw :8080 │                     │
│              └────────────────┘                     │
└─────────────────────────────────────────────────────┘
```

### Prerequisites

- Two cloud VMs (one on each provider), each running OpenClaw core on `localhost:8080`
- A Redis instance reachable by the gateway and orchestrator (VPC-internal)
- The gateway's port (8443) accessible from the remote agent (public IP, VPN, or Tailscale)

### Step 1: Infrastructure (DigitalOcean SGP1)

Start Redis and PostgreSQL on the infrastructure node:

```bash
# On the DO droplet (infrastructure)
docker compose up -d
```

Or install standalone:

```bash
curl -sfL https://get.openclaw.dev | OPENCLAW_ROLE=infra sh
```

### Step 2: Gateway (DigitalOcean SGP1)

The gateway must be reachable by agents on both clouds. Deploy it alongside Redis.

Generate API keys with identity bindings:

```bash
# Generate unique keys
ALPHA_KEY=$(openssl rand -hex 24)
BETA_KEY=$(openssl rand -hex 24)
ORCH_KEY=$(openssl rand -hex 24)

echo "Alpha key: $ALPHA_KEY"
echo "Beta key:  $BETA_KEY"
echo "Orch key:  $ORCH_KEY"
```

Start the gateway:

```bash
gateway \
  --redis-url redis://10.104.0.2:6379 \
  --listen 0.0.0.0:8443 \
  --api-keys "$ALPHA_KEY=agent-alpha,$BETA_KEY=agent-beta,$ORCH_KEY=orchestrator"
```

Or via environment variables:

```bash
export REDIS_URL=redis://10.104.0.2:6379
export GATEWAY_LISTEN=0.0.0.0:8443
export GATEWAY_API_KEYS="$ALPHA_KEY=agent-alpha,$BETA_KEY=agent-beta,$ORCH_KEY=orchestrator"
gateway
```

### Step 3: Orchestrator (DigitalOcean SGP1)

Deploy alongside Redis in the same VPC:

```bash
orchestrator --redis-url redis://10.104.0.2:6379
```

### Step 4: Configure ACL rules

Before starting agents, set up groups and P2P rules via `redis-cli`:

```bash
redis-cli -h 10.104.0.2

# Allow agents in "nlp" to P2P with agents in "search"
SADD openclaw:acl:p2p-rules "nlp:search"

# Verify
SMEMBERS openclaw:acl:p2p-rules
```

The orchestrator module also exposes these programmatically via `acl::add_rule()`.

### Step 5: Agent Alpha (DigitalOcean SGP1)

On the same DO droplet (or another in the same VPC):

```bash
agent \
  --gateway-url http://10.104.0.3:8443 \
  --gateway-api-key "$ALPHA_KEY" \
  --agent-id agent-alpha \
  --core-url http://127.0.0.1:8080 \
  --listen 0.0.0.0:9090
```

Register with groups (the agent does this automatically, but you can also set groups via the registration request or directly in Redis):

```bash
# Set groups for agent-alpha
redis-cli -h 10.104.0.2 HSET openclaw:agent:agent-alpha:meta groups "nlp"
```

Or include groups in the registration payload sent by the agent's `GatewayClient::register()`.

### Step 6: Agent Beta (AWS us-east-1)

On the AWS EC2 instance, the agent connects to the gateway's **public** IP:

```bash
agent \
  --gateway-url https://159.65.10.50:8443 \
  --gateway-api-key "$BETA_KEY" \
  --agent-id agent-beta \
  --core-url http://127.0.0.1:8080 \
  --listen 0.0.0.0:9090
```

Set groups:

```bash
# From the orchestrator/Redis host
redis-cli -h 10.104.0.2 HSET openclaw:agent:agent-beta:meta groups "search"
```

### Step 7: Verify

Check that both agents are registered and visible:

```bash
# From any machine with access to the gateway
curl -s -H "X-API-Key: $ORCH_KEY" \
  http://10.104.0.3:8443/api/v1/roster | jq .
```

Expected output:

```json
[
  {
    "agent_id": "agent-alpha",
    "meta": { "cloud": "do", "region": "sgp1", "groups": ["nlp"], ... }
  },
  {
    "agent_id": "agent-beta",
    "meta": { "cloud": "aws", "region": "us-east-1", "groups": ["search"], ... }
  }
]
```

Test P2P communication (alpha -> beta should be allowed by the `nlp:search` rule):

```bash
curl -s -X POST \
  -H "X-API-Key: $ALPHA_KEY" \
  -H "Content-Type: application/json" \
  http://10.104.0.3:8443/api/v1/send \
  -d '{
    "id": "00000000-0000-0000-0000-000000000001",
    "from": "agent-alpha",
    "to": {"agent": "agent-beta"},
    "msg_type": "p2p",
    "payload": {"query": "search for rust concurrency patterns"},
    "channel": "internal",
    "ts": 1710000000000,
    "retry_count": 0
  }'
```

Expected: `{"sent": true}`

Test targeted broadcast (orchestrator -> nlp group):

```bash
curl -s -X POST \
  -H "X-API-Key: $ORCH_KEY" \
  -H "Content-Type: application/json" \
  http://10.104.0.3:8443/api/v1/broadcast-group \
  -d '{
    "group": "nlp",
    "message": {
      "id": "00000000-0000-0000-0000-000000000002",
      "from": "orchestrator",
      "to": "broadcast",
      "msg_type": {"system": "reload_skills"},
      "payload": {},
      "channel": "internal",
      "ts": 1710000000000,
      "retry_count": 0
    }
  }'
```

Expected: `{"broadcast": true, "reached": ["agent-alpha"]}`

### Firewall rules

| Source | Destination | Port | Protocol | Purpose |
|--------|-------------|------|----------|---------|
| Agent VMs | Gateway public IP | 8443 | TCP | Agent -> Gateway |
| Gateway | Redis | 6379 | TCP (VPC) | Gateway -> Redis |
| Orchestrator | Redis | 6379 | TCP (VPC) | Orchestrator -> Redis |
| Tracer | Redis + PostgreSQL | 6379, 5432 | TCP (VPC) | Tracer reads/writes |

### Production considerations

- **TLS**: Put the gateway behind a reverse proxy (nginx, Caddy) with TLS, or use Tailscale for encrypted point-to-point connectivity between clouds
- **Redis auth**: Use `redis://default:password@host:6379` and enable `requirepass` in Redis config
- **Key rotation**: Generate new API keys, update `GATEWAY_API_KEYS`, restart the gateway; agents reconnect with new keys
- **Monitoring**: Use `orchestrator --watch agent-alpha` to live-tail an agent's streams; deploy the tracer for persistent audit logging
- **Scaling**: Multiple gateways can be deployed behind a load balancer; each agent connects to a single gateway instance at a time

## Project Structure

```
PheroClaw/
├── .cargo/config.toml              # Cargo aliases
├── .github/workflows/ci.yml        # CI pipeline
├── .gitignore
├── .env.example                     # Env var template
├── Cargo.toml                       # Virtual workspace manifest
├── docker-compose.yml               # Redis + PostgreSQL for local dev
├── rust-toolchain.toml              # Pin to stable
├── PRDs/                            # Product Requirements Documents
│   ├── pheroclaw-messaging.md       # Messaging architecture
│   ├── pheroclaw-tracer.md          # Tracer design + PG schema
│   └── pheroclaw-install.md         # Single-script installer
├── crates/
│   ├── messaging/                   # Shared library (types, keys, transport, ACL)
│   ├── agent/                       # Agent sidecar (connects via gateway)
│   ├── gateway/                     # Redis proxy for agents (identity + ACL)
│   ├── orchestrator/                # Orchestrator + CLI (direct Redis)
│   └── tracer/                      # Message tracer (direct Redis + PG)
├── CLAUDE.md
└── README.md
```

## Tech Stack

- **Language**: Rust
- **Message bus**: Redis Streams (with consumer groups)
- **Trace database**: PostgreSQL (via sqlx)
- **HTTP framework**: Axum (agent sidecar + gateway)
- **Redis client**: Fred
- **HTTP client**: Reqwest (agent -> gateway, agent -> core)
- **Serialization**: serde + serde_json
- **CLI parsing**: clap

## License

_TBD_
