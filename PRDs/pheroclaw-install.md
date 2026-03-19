#!/usr/bin/env bash
# ═══════════════════════════════════════════════════════════════════
# OpenClaw Messaging Stack — Single-Script Installer
# ═══════════════════════════════════════════════════════════════════
#
# Usage:
#   curl -sSL https://raw.githubusercontent.com/openclaw/messaging/main/install.sh | bash -s -- --role agent --redis-url redis://10.0.0.5:6379
#
# Roles:
#   agent        — sidecar reverse proxy + OpenClaw core wrapper
#   orchestrator — central coordinator + CLI
#   tracer       — passive message recorder (needs PostgreSQL)
#   infra        — Redis + PostgreSQL only (no application binaries)
#   all          — everything on one box (dev/demo)
#
# The script will:
#   1. Detect cloud provider, region, instance ID from metadata APIs
#   2. Generate a unique agent ID: {cloud}-{region}-{short_id}
#   3. Install dependencies (Redis, PostgreSQL, Rust toolchain as needed)
#   4. Build or download the binaries
#   5. Create systemd services
#   6. Register the instance in Redis with full endpoint metadata
#   7. Print a summary with connection details
#
set -euo pipefail

# ─── Defaults ───
ROLE=""
REDIS_URL=""
DATABASE_URL=""
OPENCLAW_CORE_URL="http://127.0.0.1:8080"
SIDECAR_LISTEN="0.0.0.0:9090"
TELEGRAM_BOT_TOKEN=""
INSTALL_DIR="/opt/openclaw"
BIN_DIR="/usr/local/bin"
CONFIG_DIR="/etc/openclaw"
DATA_DIR="/var/lib/openclaw"
OPENCLAW_USER="openclaw"
SKIP_BUILD=false
BINARY_URL=""  # if set, download pre-built binaries instead of compiling

# ─── Colors ───
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

log()  { echo -e "${GREEN}[openclaw]${NC} $*"; }
warn() { echo -e "${YELLOW}[openclaw]${NC} $*"; }
err()  { echo -e "${RED}[openclaw]${NC} $*" >&2; }
banner() { echo -e "\n${CYAN}═══ $* ═══${NC}\n"; }

# ═══════════════════════════════════════════════════════════════════
# PHASE 1: Parse arguments
# ═══════════════════════════════════════════════════════════════════

usage() {
    cat <<EOF
Usage: $0 --role <role> [options]

Required:
  --role <role>           One of: agent, orchestrator, tracer, infra, all

Connection (required for agent/orchestrator/tracer):
  --redis-url <url>       Redis connection URL (e.g. redis://10.0.0.5:6379)
  --database-url <url>    PostgreSQL URL (required for tracer role)

Agent options:
  --core-url <url>        OpenClaw core URL (default: http://127.0.0.1:8080)
  --listen <addr>         Sidecar listen address (default: 0.0.0.0:9090)
  --telegram-token <tok>  Telegram bot token (optional)
  --agent-id <id>         Override auto-detected agent ID

Build options:
  --binary-url <url>      Download pre-built binaries from this URL
  --skip-build            Skip building from source (requires --binary-url)

EOF
    exit 1
}

while [[ $# -gt 0 ]]; do
    case $1 in
        --role)             ROLE="$2"; shift 2 ;;
        --redis-url)        REDIS_URL="$2"; shift 2 ;;
        --database-url)     DATABASE_URL="$2"; shift 2 ;;
        --core-url)         OPENCLAW_CORE_URL="$2"; shift 2 ;;
        --listen)           SIDECAR_LISTEN="$2"; shift 2 ;;
        --telegram-token)   TELEGRAM_BOT_TOKEN="$2"; shift 2 ;;
        --agent-id)         OVERRIDE_AGENT_ID="$2"; shift 2 ;;
        --binary-url)       BINARY_URL="$2"; shift 2 ;;
        --skip-build)       SKIP_BUILD=true; shift ;;
        --help|-h)          usage ;;
        *)                  err "Unknown option: $1"; usage ;;
    esac
done

[[ -z "$ROLE" ]] && { err "--role is required"; usage; }
[[ "$ROLE" =~ ^(agent|orchestrator|tracer|infra|all)$ ]] || { err "Invalid role: $ROLE"; usage; }

# Validate required args per role
if [[ "$ROLE" == "agent" || "$ROLE" == "orchestrator" ]]; then
    [[ -z "$REDIS_URL" ]] && { err "--redis-url is required for role=$ROLE"; exit 1; }
fi
if [[ "$ROLE" == "tracer" ]]; then
    [[ -z "$REDIS_URL" ]] && { err "--redis-url is required for role=tracer"; exit 1; }
    [[ -z "$DATABASE_URL" ]] && { err "--database-url is required for role=tracer"; exit 1; }
fi

# ═══════════════════════════════════════════════════════════════════
# PHASE 2: Detect cloud environment + generate agent ID
# ═══════════════════════════════════════════════════════════════════

banner "Detecting environment"

CLOUD_PROVIDER="bare"
CLOUD_REGION="local"
INSTANCE_ID=""
PUBLIC_IP=""
PRIVATE_IP=""

# ─── DigitalOcean ───
detect_digitalocean() {
    local meta="http://169.254.169.254/metadata/v1"
    if curl -sf --connect-timeout 2 "$meta/id" > /dev/null 2>&1; then
        CLOUD_PROVIDER="do"
        INSTANCE_ID=$(curl -sf "$meta/id")
        CLOUD_REGION=$(curl -sf "$meta/region")
        PUBLIC_IP=$(curl -sf "$meta/interfaces/public/0/ipv4/address" 2>/dev/null || echo "")
        PRIVATE_IP=$(curl -sf "$meta/interfaces/private/0/ipv4/address" 2>/dev/null || echo "")
        log "DigitalOcean droplet detected: id=$INSTANCE_ID region=$CLOUD_REGION"
        return 0
    fi
    return 1
}

# ─── AWS / EC2 ───
detect_aws() {
    local token
    token=$(curl -sf --connect-timeout 2 -X PUT \
        -H "X-aws-ec2-metadata-token-ttl-seconds: 60" \
        "http://169.254.169.254/latest/api/token" 2>/dev/null || echo "")
    if [[ -n "$token" ]]; then
        CLOUD_PROVIDER="aws"
        INSTANCE_ID=$(curl -sf -H "X-aws-ec2-metadata-token: $token" \
            "http://169.254.169.254/latest/meta-data/instance-id")
        CLOUD_REGION=$(curl -sf -H "X-aws-ec2-metadata-token: $token" \
            "http://169.254.169.254/latest/meta-data/placement/region")
        PUBLIC_IP=$(curl -sf -H "X-aws-ec2-metadata-token: $token" \
            "http://169.254.169.254/latest/meta-data/public-ipv4" 2>/dev/null || echo "")
        PRIVATE_IP=$(curl -sf -H "X-aws-ec2-metadata-token: $token" \
            "http://169.254.169.254/latest/meta-data/local-ipv4" 2>/dev/null || echo "")
        log "AWS EC2 detected: id=$INSTANCE_ID region=$CLOUD_REGION"
        return 0
    fi
    return 1
}

# ─── Tencent Cloud (CVM) ───
detect_tencent() {
    local meta="http://metadata.tencentyun.com/latest/meta-data"
    if curl -sf --connect-timeout 2 "$meta/instance-id" > /dev/null 2>&1; then
        CLOUD_PROVIDER="tencent"
        INSTANCE_ID=$(curl -sf "$meta/instance-id")
        CLOUD_REGION=$(curl -sf "$meta/placement/zone" | sed 's/-[0-9]*$//')
        PUBLIC_IP=$(curl -sf "$meta/public-ipv4" 2>/dev/null || echo "")
        PRIVATE_IP=$(curl -sf "$meta/local-ipv4" 2>/dev/null || echo "")
        log "Tencent CVM detected: id=$INSTANCE_ID region=$CLOUD_REGION"
        return 0
    fi
    return 1
}

# ─── BytePlus / Volcengine (ECS) ───
detect_byteplus() {
    local meta="http://100.96.0.96/latest/meta-data"
    if curl -sf --connect-timeout 2 "$meta/instance-id" > /dev/null 2>&1; then
        CLOUD_PROVIDER="byteplus"
        INSTANCE_ID=$(curl -sf "$meta/instance-id")
        CLOUD_REGION=$(curl -sf "$meta/placement/availability-zone" | sed 's/-[a-z]$//')
        PUBLIC_IP=$(curl -sf "$meta/public-ipv4" 2>/dev/null || echo "")
        PRIVATE_IP=$(curl -sf "$meta/local-ipv4" 2>/dev/null || echo "")
        log "BytePlus ECS detected: id=$INSTANCE_ID region=$CLOUD_REGION"
        return 0
    fi
    return 1
}

# ─── GCP ───
detect_gcp() {
    if curl -sf --connect-timeout 2 -H "Metadata-Flavor: Google" \
        "http://metadata.google.internal/computeMetadata/v1/instance/id" > /dev/null 2>&1; then
        CLOUD_PROVIDER="gcp"
        INSTANCE_ID=$(curl -sf -H "Metadata-Flavor: Google" \
            "http://metadata.google.internal/computeMetadata/v1/instance/id")
        CLOUD_REGION=$(curl -sf -H "Metadata-Flavor: Google" \
            "http://metadata.google.internal/computeMetadata/v1/instance/zone" | awk -F/ '{print $NF}' | sed 's/-[a-z]$//')
        PUBLIC_IP=$(curl -sf -H "Metadata-Flavor: Google" \
            "http://metadata.google.internal/computeMetadata/v1/instance/network-interfaces/0/access-configs/0/external-ip" 2>/dev/null || echo "")
        PRIVATE_IP=$(curl -sf -H "Metadata-Flavor: Google" \
            "http://metadata.google.internal/computeMetadata/v1/instance/network-interfaces/0/ip" 2>/dev/null || echo "")
        log "GCP instance detected: id=$INSTANCE_ID region=$CLOUD_REGION"
        return 0
    fi
    return 1
}

# ─── Bare metal / VM fallback ───
detect_bare() {
    CLOUD_PROVIDER="bare"
    CLOUD_REGION="local"
    INSTANCE_ID=$(hostname -s)
    PRIVATE_IP=$(hostname -I 2>/dev/null | awk '{print $1}' || echo "127.0.0.1")
    PUBLIC_IP=""
    log "Bare metal / VM detected: hostname=$INSTANCE_ID ip=$PRIVATE_IP"
}

# Try each provider in order
detect_digitalocean || detect_aws || detect_tencent || detect_byteplus || detect_gcp || detect_bare

# ─── Generate agent ID ───
if [[ -n "${OVERRIDE_AGENT_ID:-}" ]]; then
    AGENT_ID="$OVERRIDE_AGENT_ID"
    log "Using override agent ID: $AGENT_ID"
else
    # Format: {cloud}-{region}-{short_instance_id}
    # Truncate instance ID to 8 chars for readability
    SHORT_ID=$(echo "$INSTANCE_ID" | tail -c 9 | tr -d '\n')
    AGENT_ID="${CLOUD_PROVIDER}-${CLOUD_REGION}-${SHORT_ID}"
    log "Generated agent ID: $AGENT_ID"
fi

# Determine the best reachable IP (prefer private for VPC, fall back to public)
ENDPOINT_IP="${PRIVATE_IP:-${PUBLIC_IP:-127.0.0.1}}"
SIDECAR_PORT=$(echo "$SIDECAR_LISTEN" | grep -oP ':\K[0-9]+$' || echo "9090")
AGENT_ENDPOINT="http://${ENDPOINT_IP}:${SIDECAR_PORT}"

log "Agent ID:       $AGENT_ID"
log "Cloud:          $CLOUD_PROVIDER"
log "Region:         $CLOUD_REGION"
log "Instance ID:    $INSTANCE_ID"
log "Private IP:     ${PRIVATE_IP:-n/a}"
log "Public IP:      ${PUBLIC_IP:-n/a}"
log "Endpoint:       $AGENT_ENDPOINT"

# ═══════════════════════════════════════════════════════════════════
# PHASE 3: Install system dependencies
# ═══════════════════════════════════════════════════════════════════

banner "Installing dependencies"

export DEBIAN_FRONTEND=noninteractive

install_base() {
    apt-get update -qq
    apt-get install -y -qq curl wget jq build-essential pkg-config libssl-dev > /dev/null
    log "Base packages installed"
}

install_redis() {
    if command -v redis-server &>/dev/null; then
        log "Redis already installed: $(redis-server --version | head -1)"
        return
    fi
    apt-get install -y -qq redis-server > /dev/null
    # Bind to all interfaces for VPC access
    sed -i 's/^bind 127.0.0.1/bind 0.0.0.0/' /etc/redis/redis.conf
    sed -i 's/^# maxmemory <bytes>/maxmemory 256mb/' /etc/redis/redis.conf
    echo "maxmemory-policy allkeys-lru" >> /etc/redis/redis.conf
    # Enable AOF persistence
    sed -i 's/^appendonly no/appendonly yes/' /etc/redis/redis.conf
    systemctl enable redis-server
    systemctl restart redis-server
    log "Redis installed and configured"
}

install_postgres() {
    if command -v psql &>/dev/null; then
        log "PostgreSQL already installed: $(psql --version)"
        return
    fi
    apt-get install -y -qq postgresql postgresql-client > /dev/null
    systemctl enable postgresql
    systemctl start postgresql

    # Create database and user for the tracer
    sudo -u postgres psql -c "CREATE USER openclaw_tracer WITH PASSWORD 'openclaw_tracer';" 2>/dev/null || true
    sudo -u postgres psql -c "CREATE DATABASE openclaw_trace OWNER openclaw_tracer;" 2>/dev/null || true
    sudo -u postgres psql -c "GRANT ALL PRIVILEGES ON DATABASE openclaw_trace TO openclaw_tracer;" 2>/dev/null || true

    # Allow connections from VPC
    local pg_hba
    pg_hba=$(find /etc/postgresql -name pg_hba.conf | head -1)
    if [[ -n "$pg_hba" ]]; then
        echo "host openclaw_trace openclaw_tracer 0.0.0.0/0 md5" >> "$pg_hba"
        # Allow listening on all interfaces
        local pg_conf
        pg_conf=$(find /etc/postgresql -name postgresql.conf | head -1)
        sed -i "s/#listen_addresses = 'localhost'/listen_addresses = '*'/" "$pg_conf"
        systemctl restart postgresql
    fi

    log "PostgreSQL installed and configured"
    if [[ -z "$DATABASE_URL" ]]; then
        DATABASE_URL="postgres://openclaw_tracer:openclaw_tracer@127.0.0.1:5432/openclaw_trace"
        log "DATABASE_URL set to: $DATABASE_URL"
    fi
}

install_rust() {
    if command -v cargo &>/dev/null; then
        log "Rust already installed: $(rustc --version)"
        return
    fi
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain stable
    source "$HOME/.cargo/env"
    log "Rust installed: $(rustc --version)"
}

# Create openclaw system user
create_user() {
    if id "$OPENCLAW_USER" &>/dev/null; then
        log "User $OPENCLAW_USER already exists"
        return
    fi
    useradd --system --shell /bin/false --home-dir "$DATA_DIR" --create-home "$OPENCLAW_USER"
    log "Created system user: $OPENCLAW_USER"
}

install_base
create_user
mkdir -p "$INSTALL_DIR" "$CONFIG_DIR" "$DATA_DIR" "$BIN_DIR"

case "$ROLE" in
    infra)
        install_redis
        install_postgres
        ;;
    agent|orchestrator)
        install_rust
        ;;
    tracer)
        install_rust
        ;;
    all)
        install_redis
        install_postgres
        install_rust
        REDIS_URL="${REDIS_URL:-redis://127.0.0.1:6379}"
        DATABASE_URL="${DATABASE_URL:-postgres://openclaw_tracer:openclaw_tracer@127.0.0.1:5432/openclaw_trace}"
        ;;
esac

# ═══════════════════════════════════════════════════════════════════
# PHASE 4: Build or download binaries
# ═══════════════════════════════════════════════════════════════════

if [[ "$ROLE" != "infra" ]]; then
    banner "Building binaries"

    if [[ "$SKIP_BUILD" == true && -n "$BINARY_URL" ]]; then
        log "Downloading pre-built binaries from $BINARY_URL"
        cd /tmp
        wget -q "$BINARY_URL" -O openclaw-binaries.tar.gz
        tar xzf openclaw-binaries.tar.gz -C "$BIN_DIR"
        rm -f openclaw-binaries.tar.gz
    else
        # Clone and build from source
        cd "$INSTALL_DIR"
        if [[ ! -d "openclaw-messaging-ws" ]]; then
            log "Cloning repository..."
            git clone https://github.com/openclaw/openclaw-messaging-ws.git 2>/dev/null || {
                warn "Git clone failed — initialising workspace from scratch"
                mkdir -p openclaw-messaging-ws
            }
        fi
        cd openclaw-messaging-ws

        log "Building workspace (release mode)..."
        source "$HOME/.cargo/env" 2>/dev/null || true

        case "$ROLE" in
            agent)
                cargo build --release -p openclaw-agent 2>&1 | tail -5
                cp target/release/agent "$BIN_DIR/openclaw-agent"
                ;;
            orchestrator)
                cargo build --release -p openclaw-orchestrator 2>&1 | tail -5
                cp target/release/orchestrator "$BIN_DIR/openclaw-orchestrator"
                ;;
            tracer)
                cargo build --release -p openclaw-tracer 2>&1 | tail -5
                cp target/release/tracer "$BIN_DIR/openclaw-tracer"
                ;;
            all)
                cargo build --release 2>&1 | tail -5
                cp target/release/agent "$BIN_DIR/openclaw-agent"
                cp target/release/orchestrator "$BIN_DIR/openclaw-orchestrator"
                cp target/release/tracer "$BIN_DIR/openclaw-tracer"
                ;;
        esac

        log "Binaries installed to $BIN_DIR"
    fi
fi

# ═══════════════════════════════════════════════════════════════════
# PHASE 5: Generate config + API key
# ═══════════════════════════════════════════════════════════════════

banner "Generating configuration"

# Generate a random API key for gateway auth
API_KEY=$(openssl rand -hex 32)

cat > "$CONFIG_DIR/openclaw.env" <<EOF
# ═══ OpenClaw Messaging Configuration ═══
# Generated by install.sh on $(date -u +"%Y-%m-%dT%H:%M:%SZ")
# Role: $ROLE
# Agent ID: $AGENT_ID

OPENCLAW_AGENT_ID=$AGENT_ID
REDIS_URL=$REDIS_URL
DATABASE_URL=$DATABASE_URL
OPENCLAW_CORE_URL=$OPENCLAW_CORE_URL
SIDECAR_LISTEN=$SIDECAR_LISTEN
TELEGRAM_BOT_TOKEN=$TELEGRAM_BOT_TOKEN
GATEWAY_API_KEY=$API_KEY

# Cloud metadata (auto-detected)
OPENCLAW_CLOUD=$CLOUD_PROVIDER
OPENCLAW_REGION=$CLOUD_REGION
OPENCLAW_INSTANCE_ID=$INSTANCE_ID
OPENCLAW_ENDPOINT=$AGENT_ENDPOINT
OPENCLAW_PRIVATE_IP=${PRIVATE_IP:-}
OPENCLAW_PUBLIC_IP=${PUBLIC_IP:-}
EOF

chmod 600 "$CONFIG_DIR/openclaw.env"
chown "$OPENCLAW_USER:$OPENCLAW_USER" "$CONFIG_DIR/openclaw.env"
log "Config written to $CONFIG_DIR/openclaw.env"

# ═══════════════════════════════════════════════════════════════════
# PHASE 6: Register in Redis (agent discovery)
# ═══════════════════════════════════════════════════════════════════

register_in_redis() {
    banner "Registering with Redis"

    # Wait for Redis to be reachable
    local redis_host redis_port
    redis_host=$(echo "$REDIS_URL" | sed -E 's|redis://([^:]+):([0-9]+).*|\1|')
    redis_port=$(echo "$REDIS_URL" | sed -E 's|redis://([^:]+):([0-9]+).*|\2|')

    local retries=10
    while ! redis-cli -h "$redis_host" -p "$redis_port" PING &>/dev/null; do
        retries=$((retries - 1))
        if [[ $retries -le 0 ]]; then
            warn "Cannot reach Redis at $REDIS_URL — skipping registration"
            warn "The agent will self-register on first startup"
            return
        fi
        log "Waiting for Redis... ($retries attempts left)"
        sleep 2
    done

    # ─── Add to roster (Set) ───
    redis-cli -h "$redis_host" -p "$redis_port" \
        SADD "openclaw:agent:roster" "$AGENT_ID" > /dev/null
    log "Added $AGENT_ID to roster"

    # ─── Write agent metadata (Hash) ───
    # This is the discovery record. The orchestrator reads this to know
    # how to reach each agent (endpoint URL, cloud, region, etc.)
    redis-cli -h "$redis_host" -p "$redis_port" HSET "openclaw:agent:${AGENT_ID}:meta" \
        "agent_id"      "$AGENT_ID" \
        "role"          "$ROLE" \
        "cloud"         "$CLOUD_PROVIDER" \
        "region"        "$CLOUD_REGION" \
        "instance_id"   "$INSTANCE_ID" \
        "endpoint"      "$AGENT_ENDPOINT" \
        "private_ip"    "${PRIVATE_IP:-}" \
        "public_ip"     "${PUBLIC_IP:-}" \
        "sidecar_port"  "$SIDECAR_PORT" \
        "hostname"      "$(hostname -f 2>/dev/null || hostname)" \
        "os"            "$(lsb_release -ds 2>/dev/null || cat /etc/os-release | grep PRETTY_NAME | cut -d= -f2 | tr -d '"')" \
        "installed_at"  "$(date -u +"%Y-%m-%dT%H:%M:%SZ")" \
        "version"       "0.1.0" \
        > /dev/null
    log "Written discovery metadata to openclaw:agent:${AGENT_ID}:meta"

    # ─── Verify registration ───
    local count
    count=$(redis-cli -h "$redis_host" -p "$redis_port" SCARD "openclaw:agent:roster")
    log "Roster now has $count registered agent(s)"

    # Show all registered agents
    log "Current roster:"
    redis-cli -h "$redis_host" -p "$redis_port" SMEMBERS "openclaw:agent:roster" | while read -r id; do
        local ep
        ep=$(redis-cli -h "$redis_host" -p "$redis_port" HGET "openclaw:agent:${id}:meta" "endpoint")
        local cloud
        cloud=$(redis-cli -h "$redis_host" -p "$redis_port" HGET "openclaw:agent:${id}:meta" "cloud")
        local region
        region=$(redis-cli -h "$redis_host" -p "$redis_port" HGET "openclaw:agent:${id}:meta" "region")
        echo "  ${id} → ${ep} (${cloud}/${region})"
    done
}

# Install redis-cli if needed (for registration even if this isn't the Redis host)
if ! command -v redis-cli &>/dev/null; then
    apt-get install -y -qq redis-tools > /dev/null 2>&1 || true
fi

if [[ -n "$REDIS_URL" ]]; then
    register_in_redis
fi

# ═══════════════════════════════════════════════════════════════════
# PHASE 7: Create systemd services
# ═══════════════════════════════════════════════════════════════════

banner "Creating systemd services"

create_agent_service() {
    cat > /etc/systemd/system/openclaw-agent.service <<EOF
[Unit]
Description=OpenClaw Agent Sidecar ($AGENT_ID)
After=network.target

[Service]
Type=simple
User=$OPENCLAW_USER
EnvironmentFile=$CONFIG_DIR/openclaw.env
ExecStart=$BIN_DIR/openclaw-agent
Restart=always
RestartSec=5
StandardOutput=journal
StandardError=journal
SyslogIdentifier=openclaw-agent

# Hardening
NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=$DATA_DIR

[Install]
WantedBy=multi-user.target
EOF
    systemctl daemon-reload
    systemctl enable openclaw-agent
    log "Created openclaw-agent.service"
}

create_orchestrator_service() {
    cat > /etc/systemd/system/openclaw-orchestrator.service <<EOF
[Unit]
Description=OpenClaw Orchestrator
After=network.target

[Service]
Type=simple
User=$OPENCLAW_USER
EnvironmentFile=$CONFIG_DIR/openclaw.env
ExecStart=$BIN_DIR/openclaw-orchestrator
Restart=always
RestartSec=5
StandardOutput=journal
StandardError=journal
SyslogIdentifier=openclaw-orch

NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=$DATA_DIR

[Install]
WantedBy=multi-user.target
EOF
    systemctl daemon-reload
    systemctl enable openclaw-orchestrator
    log "Created openclaw-orchestrator.service"
}

create_tracer_service() {
    cat > /etc/systemd/system/openclaw-tracer.service <<EOF
[Unit]
Description=OpenClaw Message Tracer
After=network.target postgresql.service

[Service]
Type=simple
User=$OPENCLAW_USER
EnvironmentFile=$CONFIG_DIR/openclaw.env
ExecStart=$BIN_DIR/openclaw-tracer
Restart=always
RestartSec=5
StandardOutput=journal
StandardError=journal
SyslogIdentifier=openclaw-tracer

NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=$DATA_DIR

[Install]
WantedBy=multi-user.target
EOF
    systemctl daemon-reload
    systemctl enable openclaw-tracer
    log "Created openclaw-tracer.service"
}

case "$ROLE" in
    agent)        create_agent_service ;;
    orchestrator) create_orchestrator_service ;;
    tracer)       create_tracer_service ;;
    all)
        create_agent_service
        create_orchestrator_service
        create_tracer_service
        ;;
esac

# ═══════════════════════════════════════════════════════════════════
# PHASE 8: Start services
# ═══════════════════════════════════════════════════════════════════

banner "Starting services"

case "$ROLE" in
    agent)
        systemctl start openclaw-agent
        log "openclaw-agent started"
        ;;
    orchestrator)
        systemctl start openclaw-orchestrator
        log "openclaw-orchestrator started"
        ;;
    tracer)
        systemctl start openclaw-tracer
        log "openclaw-tracer started"
        ;;
    infra)
        log "Infrastructure services (Redis + PostgreSQL) are running"
        ;;
    all)
        systemctl start openclaw-agent
        systemctl start openclaw-orchestrator
        systemctl start openclaw-tracer
        log "All services started"
        ;;
esac

# ═══════════════════════════════════════════════════════════════════
# PHASE 9: Summary
# ═══════════════════════════════════════════════════════════════════

banner "Installation complete"

cat <<EOF

  Role:           $ROLE
  Agent ID:       $AGENT_ID
  Cloud:          $CLOUD_PROVIDER ($CLOUD_REGION)
  Endpoint:       $AGENT_ENDPOINT
  Redis:          $REDIS_URL
  Config:         $CONFIG_DIR/openclaw.env
  Logs:           journalctl -u openclaw-${ROLE} -f

EOF

if [[ "$ROLE" == "agent" || "$ROLE" == "all" ]]; then
cat <<EOF
  ── Agent Sidecar ──
  Sidecar:        http://${ENDPOINT_IP}:${SIDECAR_PORT}
  Core proxy:     $OPENCLAW_CORE_URL
  Telegram hook:  POST ${AGENT_ENDPOINT}/webhook/telegram
  Webchat:        POST ${AGENT_ENDPOINT}/webchat/message
  A2A:            POST ${AGENT_ENDPOINT}/a2a/messages
  Health:         GET  ${AGENT_ENDPOINT}/health

EOF
fi

if [[ "$ROLE" == "orchestrator" || "$ROLE" == "all" ]]; then
cat <<EOF
  ── Orchestrator ──
  Watch agent:    clawmacdo watch --agent $AGENT_ID --channel telegram
  List roster:    redis-cli SMEMBERS openclaw:agent:roster
  Agent meta:     redis-cli HGETALL openclaw:agent:${AGENT_ID}:meta

EOF
fi

if [[ "$ROLE" == "tracer" || "$ROLE" == "all" ]]; then
cat <<EOF
  ── Tracer ──
  Database:       $DATABASE_URL
  Query traces:   psql "$DATABASE_URL" -c "SELECT * FROM message_trace ORDER BY recorded_at DESC LIMIT 10;"

EOF
fi

if [[ "$ROLE" == "agent" ]]; then
cat <<EOF
  ── Next steps ──
  1. Set Telegram webhook:
     curl -X POST "https://api.telegram.org/bot\${TELEGRAM_BOT_TOKEN}/setWebhook?url=${AGENT_ENDPOINT}/webhook/telegram"

  2. Verify registration:
     redis-cli HGETALL openclaw:agent:${AGENT_ID}:meta

  3. Watch logs:
     journalctl -u openclaw-agent -f

EOF
fi

log "Done! 🦀"
