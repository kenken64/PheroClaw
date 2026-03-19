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
  │  │  openclaw:dlq                               │             │
  │  └──────┬──────────┬──────────┬────────────────┘             │
  │         │          │          │                              │
  │    ┌────┘          │          └────────┐                     │
  │    ▼               ▼                   ▼                     │
  │ ┌──────────┐ ┌──────────────┐ ┌──────────────┐              │
  │ │Orchestr. │ │   Gateway    │ │   Tracer     │              │
  │ │+clawmacdo│ │ (API key     │ │  (passive)   │              │
  │ │          │ │  auth, relay)│ │              │              │
  │ │• broadcast│ │              │ │• reads all   │              │
  │ │• send_to │ │• agent auth  │ │  streams     │              │
  │ │• DLQ     │ │• rate limit  │ │• batch INS   │              │
  │ │• fleet   │ │• Redis relay │ │  to PG       │              │
  │ └──────────┘ └──────┬───────┘ └──────┬───────┘              │
  │                     │                │                      │
  │                     │         ┌──────▼───────┐              │
  │                     │         │  PostgreSQL  │              │
  │                     │         │  (trace DB)  │              │
  │                     │         └──────────────┘              │
  └─────────────────────┼──────────────────────────────────────┘
                        │ HTTPS (API key auth)
          ┌─────────────┼─────────────┐
          ▼             ▼             ▼
   ┌──────────────┐ ┌──────────────┐ ┌──────────────┐
   │ Agent alpha  │ │ Agent beta   │ │ Agent gamma  │
   │ (DO SGP)     │ │ (AWS NYC)    │ │ (Tencent)    │
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

**Key security principle: Agents NEVER connect directly to Redis.** All agent communication is mediated by the gateway, which authenticates agents via API key and relays messages to/from Redis streams. Only infrastructure components (gateway, orchestrator, tracer) have direct Redis access within the VPC.

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

Common types, Redis key helpers, and transport functions shared by all binaries:

- **Message envelope** (`ClawMessage`) — UUID, routing target, message type, channel metadata, correlation ID
- **Redis key patterns** — broadcast, inbox/outbox, channel streams, heartbeat, roster, DLQ
- **Transport** — `broadcast()`, `send_to_agent()`, `send_to_orchestrator()`, consumer group management
- **Agent discovery** — roster registration, heartbeat refresh, metadata storage

### `pheroclaw-agent` (sidecar binary)

A reverse proxy that wraps each OpenClaw instance. Connects to Redis exclusively through the gateway:

- **Gateway connection** — authenticates with API key, sends/receives messages via gateway HTTP API
- **Telegram** — receives webhooks at `POST /webhook/telegram`, wraps in `ClawMessage`, sends through gateway, forwards to core, sends reply via Telegram API
- **Webchat** — `POST /webchat/message` endpoint for the OpenClaw web UI
- **A2A** — `POST /a2a/messages` for agent-to-agent communication
- **Heartbeat** — refreshes via gateway every 30s (60s TTL, missing two beats = detected dead)
- **Outbound dispatcher** — receives messages from gateway and routes responses back via the correct channel

### `pheroclaw-gateway` (Redis proxy binary)

Deployed alongside Redis in the VPC. The only entry point for agents into the Redis bus:

- **API key authentication** — validates agent identity before relaying messages
- **Rate limiting** — prevents abuse from misbehaving agents
- **Redis relay** — translates gateway HTTP API calls to Redis stream operations
- **Agent registration** — handles roster and metadata updates on behalf of agents

### `pheroclaw-orchestrator` (coordinator binary)

Central coordinator and CLI (`clawmacdo`), direct Redis access within the VPC:

- **Broadcast** — fan-out commands to all agents
- **Targeted dispatch** — assign tasks to specific agents
- **Fleet management** — monitor agent roster, heartbeats, metadata
- **DLQ sweep** — re-queue or discard dead-lettered messages
- **Watch mode** — `clawmacdo watch --agent alpha --channel telegram` to observe live conversations

### `pheroclaw-tracer` (recorder binary)

A passive consumer that records all messages to PostgreSQL:

- **Stream discovery** — dynamically discovers agent streams from the roster
- **Consumer group** — uses its own `cg-tracer` group; agents are unaware of the tracer
- **Batch writer** — flushes to PostgreSQL every 500ms or every 200 messages
- **Tables** — `message_trace` (all messages), `agent_events` (join/leave/heartbeat-lost), `dlq_audit` (dead letters)

## Message Flow

1. External message arrives (Telegram webhook, webchat HTTP, A2A request)
2. Agent sidecar wraps it in a `ClawMessage` with channel + session metadata
3. Sidecar sends message to the **gateway** via HTTP with API key auth
4. Gateway publishes to Redis (channel stream + inbox)
5. Sidecar forwards to OpenClaw core on localhost for AI processing
6. Core responds; sidecar sends response through gateway to Redis (channel stream + outbox)
7. Sidecar delivers response via the original channel (Telegram API, HTTP response, etc.)
8. Tracer passively records all stream entries to PostgreSQL

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
| `openclaw:agent:{id}:meta` | Hash | Agent discovery metadata |
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
│   ├── messaging/                   # Shared library (types, keys, transport)
│   ├── agent/                       # Agent sidecar (connects via gateway)
│   ├── gateway/                     # Redis proxy for agents (API key auth)
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
