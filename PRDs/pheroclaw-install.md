#!/usr/bin/env bash
# ═══════════════════════════════════════════════════════════════════
# OpenClaw Messaging Stack — Single-Script Installer
# ═══════════════════════════════════════════════════════════════════
#
# Inspired by: k3s (get.k3s.io), Tailscale, Docker (get.docker.com)
#
# ─── Install (env vars — k3s style) ───
#
#   curl -sfL https://get.openclaw.dev | OPENCLAW_ROLE=agent REDIS_URL=redis://10.0.0.5:6379 sh
#
# ─── Install (flags) ───
#
#   curl -sfL https://get.openclaw.dev | sh -s -- --role agent --redis-url redis://10.0.0.5:6379
#
# ─── Uninstall ───
#
#   /opt/openclaw/uninstall.sh
#
# ─── Roles ───
#
#   agent        — sidecar reverse proxy wrapping an OpenClaw instance
#   orchestrator — central coordinator + clawmacdo CLI
#   tracer       — passive message recorder → PostgreSQL
#   infra        — Redis + PostgreSQL only (no app binaries)
#   all          — everything on one box (dev / demo)
#
# ─── Environment variables (all optional, flags override) ───
#
#   OPENCLAW_ROLE           Required. One of: agent, orchestrator, tracer, infra, all
#   REDIS_URL               Redis connection string
#   DATABASE_URL            PostgreSQL connection string (tracer only)
#   OPENCLAW_AGENT_ID       Override auto-detected agent ID
#   OPENCLAW_CORE_URL       OpenClaw AI core address (default: http://127.0.0.1:8080)
#   OPENCLAW_LISTEN         Sidecar listen address (default: 0.0.0.0:9090)
#   TELEGRAM_BOT_TOKEN      Telegram bot token (agent only, optional)
#   OPENCLAW_CHANNEL        GitHub release channel: stable / nightly (default: stable)
#   OPENCLAW_VERSION        Pin to a specific release tag (e.g. v0.2.1)
#   OPENCLAW_SKIP_BUILD     Set to "true" to download pre-built binaries
#
# ═══════════════════════════════════════════════════════════════════

set -euo pipefail

# ─── Version ───
INSTALLER_VERSION="0.1.0"

# ─── Defaults (env vars populate these; flags override below) ───
ROLE="${OPENCLAW_ROLE:-}"
REDIS_URL="${REDIS_URL:-}"
DATABASE_URL="${DATABASE_URL:-}"
AGENT_ID="${OPENCLAW_AGENT_ID:-}"
CORE_URL="${OPENCLAW_CORE_URL:-http://127.0.0.1:8080}"
LISTEN="${OPENCLAW_LISTEN:-0.0.0.0:9090}"
TG_TOKEN="${TELEGRAM_BOT_TOKEN:-}"
CHANNEL="${OPENCLAW_CHANNEL:-stable}"
VERSION="${OPENCLAW_VERSION:-latest}"
SKIP_BUILD="${OPENCLAW_SKIP_BUILD:-false}"

# ─── Fixed paths ───
INSTALL_DIR="/opt/openclaw"
BIN_DIR="/usr/local/bin"
CONFIG_DIR="/etc/openclaw"
DATA_DIR="/var/lib/openclaw"
SVC_USER="openclaw"
GITHUB_REPO="openclaw/openclaw-messaging-ws"

# ─── Colors ───
R='\033[0;31m' G='\033[0;32m' Y='\033[1;33m' C='\033[0;36m' B='\033[1m' N='\033[0m'

info()  { echo -e "${G}[openclaw]${N} $*"; }
warn()  { echo -e "${Y}[openclaw]${N} $*"; }
fatal() { echo -e "${R}[openclaw]${N} $*" >&2; exit 1; }
banner(){ echo -e "\n${C}═══ $* ═══${N}\n"; }

# ═══════════════════════════════════════════════════════════════════
# Parse flags (override env vars)
# ═══════════════════════════════════════════════════════════════════

while [[ $# -gt 0 ]]; do
    case $1 in
        --role)             ROLE="$2";      shift 2 ;;
        --redis-url)        REDIS_URL="$2"; shift 2 ;;
        --database-url)     DATABASE_URL="$2"; shift 2 ;;
        --agent-id)         AGENT_ID="$2";  shift 2 ;;
        --core-url)         CORE_URL="$2";  shift 2 ;;
        --listen)           LISTEN="$2";    shift 2 ;;
        --telegram-token)   TG_TOKEN="$2";  shift 2 ;;
        --channel)          CHANNEL="$2";   shift 2 ;;
        --version)          VERSION="$2";   shift 2 ;;
        --skip-build)       SKIP_BUILD=true; shift ;;
        --help|-h)
            sed -n '2,/^set -euo/{ /^#/s/^# \?//p }' "$0"
            exit 0 ;;
        *) fatal "Unknown flag: $1 (try --help)" ;;
    esac
done

# ─── Validate ───
[[ -z "$ROLE" ]] && fatal "OPENCLAW_ROLE or --role is required (agent|orchestrator|tracer|infra|all)"
[[ "$ROLE" =~ ^(agent|orchestrator|tracer|infra|all)$ ]] || fatal "Invalid role: $ROLE"

case "$ROLE" in
    agent|orchestrator) [[ -z "$REDIS_URL" ]] && fatal "REDIS_URL is required for role=$ROLE" ;;
    tracer)
        [[ -z "$REDIS_URL" ]]    && fatal "REDIS_URL is required for role=tracer"
        [[ -z "$DATABASE_URL" ]] && fatal "DATABASE_URL is required for role=tracer"
        ;;
esac

banner "OpenClaw installer v${INSTALLER_VERSION}  •  role=${ROLE}"

# ═══════════════════════════════════════════════════════════════════
# 1. OS check
# ═══════════════════════════════════════════════════════════════════

verify_os() {
    [[ "$(uname -s)" != "Linux" ]] && fatal "This installer only supports Linux (detected: $(uname -s))"
    command -v apt-get &>/dev/null || command -v yum &>/dev/null || fatal "Need apt-get or yum"
    [[ "$(id -u)" -ne 0 ]] && fatal "Run as root or with sudo"
    info "OS: $(. /etc/os-release && echo "$PRETTY_NAME") • $(uname -m)"
}

verify_os

# ═══════════════════════════════════════════════════════════════════
# 2. Detect cloud environment
# ═══════════════════════════════════════════════════════════════════

banner "Detecting cloud environment"

CLOUD="bare"; REGION="local"; INSTANCE_ID=""; PRIVATE_IP=""; PUBLIC_IP=""

detect_do() {
    local m="http://169.254.169.254/metadata/v1"
    curl -sf --connect-timeout 2 "$m/id" >/dev/null 2>&1 || return 1
    CLOUD="do"
    INSTANCE_ID=$(curl -sf "$m/id")
    REGION=$(curl -sf "$m/region")
    PUBLIC_IP=$(curl -sf "$m/interfaces/public/0/ipv4/address" 2>/dev/null || true)
    PRIVATE_IP=$(curl -sf "$m/interfaces/private/0/ipv4/address" 2>/dev/null || true)
}

detect_aws() {
    local tok
    tok=$(curl -sf --connect-timeout 2 -X PUT \
        -H "X-aws-ec2-metadata-token-ttl-seconds: 60" \
        "http://169.254.169.254/latest/api/token" 2>/dev/null) || return 1
    [[ -z "$tok" ]] && return 1
    CLOUD="aws"
    INSTANCE_ID=$(curl -sf -H "X-aws-ec2-metadata-token: $tok" "http://169.254.169.254/latest/meta-data/instance-id")
    REGION=$(curl -sf -H "X-aws-ec2-metadata-token: $tok" "http://169.254.169.254/latest/meta-data/placement/region")
    PUBLIC_IP=$(curl -sf -H "X-aws-ec2-metadata-token: $tok" "http://169.254.169.254/latest/meta-data/public-ipv4" 2>/dev/null || true)
    PRIVATE_IP=$(curl -sf -H "X-aws-ec2-metadata-token: $tok" "http://169.254.169.254/latest/meta-data/local-ipv4" 2>/dev/null || true)
}

detect_tencent() {
    local m="http://metadata.tencentyun.com/latest/meta-data"
    curl -sf --connect-timeout 2 "$m/instance-id" >/dev/null 2>&1 || return 1
    CLOUD="tencent"
    INSTANCE_ID=$(curl -sf "$m/instance-id")
    REGION=$(curl -sf "$m/placement/zone" | sed 's/-[0-9]*$//')
    PUBLIC_IP=$(curl -sf "$m/public-ipv4" 2>/dev/null || true)
    PRIVATE_IP=$(curl -sf "$m/local-ipv4" 2>/dev/null || true)
}

detect_byteplus() {
    local m="http://100.96.0.96/latest/meta-data"
    curl -sf --connect-timeout 2 "$m/instance-id" >/dev/null 2>&1 || return 1
    CLOUD="byteplus"
    INSTANCE_ID=$(curl -sf "$m/instance-id")
    REGION=$(curl -sf "$m/placement/availability-zone" | sed 's/-[a-z]$//')
    PUBLIC_IP=$(curl -sf "$m/public-ipv4" 2>/dev/null || true)
    PRIVATE_IP=$(curl -sf "$m/local-ipv4" 2>/dev/null || true)
}

detect_gcp() {
    local m="http://metadata.google.internal/computeMetadata/v1"
    curl -sf --connect-timeout 2 -H "Metadata-Flavor: Google" "$m/instance/id" >/dev/null 2>&1 || return 1
    CLOUD="gcp"
    INSTANCE_ID=$(curl -sf -H "Metadata-Flavor: Google" "$m/instance/id")
    REGION=$(curl -sf -H "Metadata-Flavor: Google" "$m/instance/zone" | awk -F/ '{print $NF}' | sed 's/-[a-z]$//')
    PUBLIC_IP=$(curl -sf -H "Metadata-Flavor: Google" "$m/instance/network-interfaces/0/access-configs/0/external-ip" 2>/dev/null || true)
    PRIVATE_IP=$(curl -sf -H "Metadata-Flavor: Google" "$m/instance/network-interfaces/0/ip" 2>/dev/null || true)
}

detect_bare() {
    CLOUD="bare"; REGION="local"
    INSTANCE_ID=$(hostname -s)
    PRIVATE_IP=$(hostname -I 2>/dev/null | awk '{print $1}' || echo "127.0.0.1")
}

detect_do || detect_aws || detect_tencent || detect_byteplus || detect_gcp || detect_bare

# Generate agent ID if not overridden
if [[ -z "$AGENT_ID" ]]; then
    SHORT_ID=$(echo "$INSTANCE_ID" | tail -c 9 | tr -d '\n')
    AGENT_ID="${CLOUD}-${REGION}-${SHORT_ID}"
fi

ENDPOINT_IP="${PRIVATE_IP:-${PUBLIC_IP:-127.0.0.1}}"
SIDECAR_PORT=$(echo "$LISTEN" | grep -oP ':\K[0-9]+$' || echo "9090")
AGENT_ENDPOINT="http://${ENDPOINT_IP}:${SIDECAR_PORT}"

info "Agent ID:    ${B}${AGENT_ID}${N}"
info "Cloud:       $CLOUD / $REGION"
info "Instance:    $INSTANCE_ID"
info "IPs:         private=${PRIVATE_IP:-n/a}  public=${PUBLIC_IP:-n/a}"
info "Endpoint:    $AGENT_ENDPOINT"

# ═══════════════════════════════════════════════════════════════════
# 3. Install system dependencies (idempotent)
# ═══════════════════════════════════════════════════════════════════

banner "Installing dependencies"

export DEBIAN_FRONTEND=noninteractive

pkg_install() {
    if command -v apt-get &>/dev/null; then
        apt-get update -qq && apt-get install -y -qq "$@" >/dev/null
    elif command -v yum &>/dev/null; then
        yum install -y -q "$@" >/dev/null
    fi
}

ensure_user() {
    id "$SVC_USER" &>/dev/null && return
    useradd --system --shell /bin/false --home-dir "$DATA_DIR" --create-home "$SVC_USER"
    info "Created system user: $SVC_USER"
}

ensure_dirs() {
    mkdir -p "$INSTALL_DIR" "$CONFIG_DIR" "$DATA_DIR" "$BIN_DIR"
    chown "$SVC_USER:$SVC_USER" "$DATA_DIR"
}

install_redis() {
    command -v redis-server &>/dev/null && { info "Redis: $(redis-server --version | head -1)"; return; }
    pkg_install redis-server
    local conf="/etc/redis/redis.conf"
    [[ -f "$conf" ]] && {
        sed -i 's/^bind 127.0.0.1.*/bind 0.0.0.0/' "$conf"
        sed -i 's/^appendonly no/appendonly yes/' "$conf"
        grep -q '^maxmemory ' "$conf" || echo "maxmemory 256mb" >> "$conf"
        grep -q '^maxmemory-policy ' "$conf" || echo "maxmemory-policy allkeys-lru" >> "$conf"
    }
    systemctl enable --now redis-server
    info "Redis installed"
}

install_postgres() {
    command -v psql &>/dev/null && { info "PostgreSQL: $(psql --version)"; return; }
    pkg_install postgresql postgresql-client
    systemctl enable --now postgresql
    sudo -u postgres psql -c "SELECT 1 FROM pg_roles WHERE rolname='openclaw_tracer'" | grep -q 1 || \
        sudo -u postgres psql -c "CREATE USER openclaw_tracer WITH PASSWORD 'openclaw_tracer';"
    sudo -u postgres psql -lqt | cut -d\| -f1 | grep -qw openclaw_trace || \
        sudo -u postgres psql -c "CREATE DATABASE openclaw_trace OWNER openclaw_tracer;"
    local hba; hba=$(find /etc/postgresql -name pg_hba.conf 2>/dev/null | head -1)
    [[ -n "$hba" ]] && {
        grep -q openclaw_tracer "$hba" || echo "host openclaw_trace openclaw_tracer 0.0.0.0/0 md5" >> "$hba"
        local pgconf; pgconf=$(find /etc/postgresql -name postgresql.conf | head -1)
        sed -i "s/#listen_addresses = 'localhost'/listen_addresses = '*'/" "$pgconf"
        systemctl restart postgresql
    }
    info "PostgreSQL installed"
    [[ -z "$DATABASE_URL" ]] && DATABASE_URL="postgres://openclaw_tracer:openclaw_tracer@127.0.0.1:5432/openclaw_trace"
}

install_rust() {
    command -v cargo &>/dev/null && { info "Rust: $(rustc --version)"; return; }
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain stable
    source "$HOME/.cargo/env"
    info "Rust: $(rustc --version)"
}

pkg_install curl wget jq openssl
ensure_user
ensure_dirs

case "$ROLE" in
    infra)              install_redis; install_postgres ;;
    agent|orchestrator) install_rust ;;
    tracer)             install_rust ;;
    all)
        install_redis; install_postgres; install_rust
        REDIS_URL="${REDIS_URL:-redis://127.0.0.1:6379}"
        DATABASE_URL="${DATABASE_URL:-postgres://openclaw_tracer:openclaw_tracer@127.0.0.1:5432/openclaw_trace}"
        ;;
esac

# ═══════════════════════════════════════════════════════════════════
# 4. Build or download binaries
# ═══════════════════════════════════════════════════════════════════

needs_binary() { [[ "$ROLE" != "infra" ]]; }

download_binaries() {
    local arch; arch=$(uname -m)
    case "$arch" in x86_64) arch="x86_64" ;; aarch64) arch="aarch64" ;; *) fatal "Unsupported arch: $arch" ;; esac

    local tag="$VERSION"
    if [[ "$tag" == "latest" ]]; then
        tag=$(curl -sf "https://api.github.com/repos/${GITHUB_REPO}/releases/latest" | jq -r .tag_name)
        [[ -z "$tag" || "$tag" == "null" ]] && fatal "Cannot determine latest release"
    fi

    local url="https://github.com/${GITHUB_REPO}/releases/download/${tag}/openclaw-${arch}-linux.tar.gz"
    local checksum_url="${url}.sha256"

    info "Downloading $tag for $arch..."
    local tmp; tmp=$(mktemp -d)
    curl -sfL "$url" -o "$tmp/openclaw.tar.gz" || fatal "Download failed: $url"

    # Verify SHA256 checksum if available
    if curl -sfL "$checksum_url" -o "$tmp/SHA256SUMS" 2>/dev/null; then
        (cd "$tmp" && sha256sum -c SHA256SUMS) || fatal "Checksum verification failed!"
        info "SHA256 checksum verified ✓"
    else
        warn "No checksum file — skipping verification"
    fi

    tar xzf "$tmp/openclaw.tar.gz" -C "$BIN_DIR"
    rm -rf "$tmp"
    info "Binaries installed from release $tag"
}

build_binaries() {
    source "$HOME/.cargo/env" 2>/dev/null || true
    cd "$INSTALL_DIR"

    if [[ ! -d "openclaw-messaging-ws" ]]; then
        info "Cloning repository..."
        git clone "https://github.com/${GITHUB_REPO}.git" openclaw-messaging-ws 2>/dev/null || {
            warn "Clone failed — initialise the workspace manually"; return 1
        }
    else
        info "Updating repository..."
        (cd openclaw-messaging-ws && git pull --ff-only 2>/dev/null || true)
    fi

    cd openclaw-messaging-ws
    info "Building (release)... this may take a few minutes"

    case "$ROLE" in
        agent)        cargo build --release -p openclaw-agent        2>&1 | tail -3 ;;
        orchestrator) cargo build --release -p openclaw-orchestrator 2>&1 | tail -3 ;;
        tracer)       cargo build --release -p openclaw-tracer       2>&1 | tail -3 ;;
        all)          cargo build --release                          2>&1 | tail -3 ;;
    esac

    local t="target/release"
    [[ "$ROLE" == "agent"        || "$ROLE" == "all" ]] && cp "$t/agent"        "$BIN_DIR/openclaw-agent"
    [[ "$ROLE" == "orchestrator" || "$ROLE" == "all" ]] && cp "$t/orchestrator" "$BIN_DIR/openclaw-orchestrator"
    [[ "$ROLE" == "tracer"       || "$ROLE" == "all" ]] && cp "$t/tracer"       "$BIN_DIR/openclaw-tracer"

    info "Build complete"
}

if needs_binary; then
    banner "Installing binaries"
    if [[ "$SKIP_BUILD" == "true" ]]; then
        download_binaries
    else
        pkg_install build-essential pkg-config libssl-dev git
        build_binaries
    fi
fi

# ═══════════════════════════════════════════════════════════════════
# 5. Write config
# ═══════════════════════════════════════════════════════════════════

banner "Writing configuration"

API_KEY=$(openssl rand -hex 32)

cat > "$CONFIG_DIR/openclaw.env" <<EOF
# OpenClaw — generated $(date -u +"%Y-%m-%dT%H:%M:%SZ")
# Role: $ROLE | Agent: $AGENT_ID
OPENCLAW_AGENT_ID=$AGENT_ID
OPENCLAW_ROLE=$ROLE
REDIS_URL=$REDIS_URL
DATABASE_URL=$DATABASE_URL
OPENCLAW_CORE_URL=$CORE_URL
SIDECAR_LISTEN=$LISTEN
TELEGRAM_BOT_TOKEN=$TG_TOKEN
GATEWAY_API_KEY=$API_KEY
OPENCLAW_CLOUD=$CLOUD
OPENCLAW_REGION=$REGION
OPENCLAW_INSTANCE_ID=$INSTANCE_ID
OPENCLAW_ENDPOINT=$AGENT_ENDPOINT
OPENCLAW_PRIVATE_IP=${PRIVATE_IP:-}
OPENCLAW_PUBLIC_IP=${PUBLIC_IP:-}
EOF

chmod 600 "$CONFIG_DIR/openclaw.env"
chown "$SVC_USER:$SVC_USER" "$CONFIG_DIR/openclaw.env"
info "Config: $CONFIG_DIR/openclaw.env"

# ═══════════════════════════════════════════════════════════════════
# 6. Register in Redis (agent auto-discovery)
# ═══════════════════════════════════════════════════════════════════

register_redis() {
    [[ -z "$REDIS_URL" ]] && return

    command -v redis-cli &>/dev/null || pkg_install redis-tools 2>/dev/null || true
    command -v redis-cli &>/dev/null || { warn "redis-cli not available — agent will self-register on startup"; return; }

    local host port
    host=$(echo "$REDIS_URL" | sed -E 's|redis[s]?://([^:@]+@)?([^:]+):([0-9]+).*|\2|')
    port=$(echo "$REDIS_URL" | sed -E 's|redis[s]?://([^:@]+@)?([^:]+):([0-9]+).*|\3|')

    local retries=5
    while ! redis-cli -h "$host" -p "$port" PING &>/dev/null; do
        retries=$((retries - 1))
        [[ $retries -le 0 ]] && { warn "Cannot reach Redis — agent will self-register on startup"; return; }
        sleep 2
    done

    # Roster Set
    redis-cli -h "$host" -p "$port" SADD "openclaw:agent:roster" "$AGENT_ID" >/dev/null

    # Metadata Hash — this is the discovery record the orchestrator reads
    redis-cli -h "$host" -p "$port" HSET "openclaw:agent:${AGENT_ID}:meta" \
        agent_id     "$AGENT_ID" \
        role         "$ROLE" \
        cloud        "$CLOUD" \
        region       "$REGION" \
        instance_id  "$INSTANCE_ID" \
        endpoint     "$AGENT_ENDPOINT" \
        private_ip   "${PRIVATE_IP:-}" \
        public_ip    "${PUBLIC_IP:-}" \
        hostname     "$(hostname -f 2>/dev/null || hostname)" \
        version      "$INSTALLER_VERSION" \
        installed_at "$(date -u +"%Y-%m-%dT%H:%M:%SZ")" \
        >/dev/null

    local count; count=$(redis-cli -h "$host" -p "$port" SCARD "openclaw:agent:roster")
    info "Registered in Redis (${count} agent(s) in roster)"
}

banner "Registering"
register_redis

# ═══════════════════════════════════════════════════════════════════
# 7. Create systemd services
# ═══════════════════════════════════════════════════════════════════

banner "Creating systemd services"

create_service() {
    local name="$1" desc="$2" bin="$3"
    cat > "/etc/systemd/system/openclaw-${name}.service" <<UNIT
[Unit]
Description=OpenClaw ${desc} (${AGENT_ID})
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=$SVC_USER
EnvironmentFile=$CONFIG_DIR/openclaw.env
ExecStart=${BIN_DIR}/${bin}
Restart=always
RestartSec=5
StandardOutput=journal
StandardError=journal
SyslogIdentifier=openclaw-${name}

NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=true
PrivateTmp=true
ReadWritePaths=$DATA_DIR

[Install]
WantedBy=multi-user.target
UNIT
    systemctl daemon-reload
    systemctl enable "openclaw-${name}" >/dev/null
    info "Created openclaw-${name}.service"
}

case "$ROLE" in
    agent)        create_service agent        "Agent Sidecar"  openclaw-agent ;;
    orchestrator) create_service orchestrator "Orchestrator"   openclaw-orchestrator ;;
    tracer)       create_service tracer       "Tracer"         openclaw-tracer ;;
    all)
        create_service agent        "Agent Sidecar"  openclaw-agent
        create_service orchestrator "Orchestrator"   openclaw-orchestrator
        create_service tracer       "Tracer"         openclaw-tracer
        ;;
esac

# ═══════════════════════════════════════════════════════════════════
# 8. Generate uninstall script
# ═══════════════════════════════════════════════════════════════════

cat > "$INSTALL_DIR/uninstall.sh" <<'UNINSTALL'
#!/usr/bin/env bash
set -euo pipefail
echo "[openclaw] Stopping and removing services..."
for svc in openclaw-agent openclaw-orchestrator openclaw-tracer; do
    systemctl stop "$svc" 2>/dev/null || true
    systemctl disable "$svc" 2>/dev/null || true
    rm -f "/etc/systemd/system/${svc}.service"
done
systemctl daemon-reload

echo "[openclaw] Removing binaries..."
rm -f /usr/local/bin/openclaw-agent /usr/local/bin/openclaw-orchestrator /usr/local/bin/openclaw-tracer

echo "[openclaw] Removing config..."
rm -rf /etc/openclaw

echo "[openclaw] Data preserved in /var/lib/openclaw and /opt/openclaw"
echo "[openclaw] Uninstalled. Run 'userdel openclaw' to remove the service user."
UNINSTALL
chmod +x "$INSTALL_DIR/uninstall.sh"
info "Uninstall: $INSTALL_DIR/uninstall.sh"

# ═══════════════════════════════════════════════════════════════════
# 9. Start services
# ═══════════════════════════════════════════════════════════════════

banner "Starting services"

start_svc() { systemctl start "openclaw-$1" && info "openclaw-$1 ● active"; }

case "$ROLE" in
    agent)        start_svc agent ;;
    orchestrator) start_svc orchestrator ;;
    tracer)       start_svc tracer ;;
    infra)        info "Infrastructure services already running" ;;
    all)          start_svc agent; start_svc orchestrator; start_svc tracer ;;
esac

# ═══════════════════════════════════════════════════════════════════
# 10. Summary
# ═══════════════════════════════════════════════════════════════════

banner "✓ Installation complete"

cat <<SUMMARY

  ${B}Role:${N}        $ROLE
  ${B}Agent ID:${N}    $AGENT_ID
  ${B}Cloud:${N}       $CLOUD / $REGION
  ${B}Endpoint:${N}    $AGENT_ENDPOINT

SUMMARY

[[ "$ROLE" == "agent" || "$ROLE" == "all" ]] && cat <<AGENT
  ── Agent Sidecar ──
  Health:      GET  ${AGENT_ENDPOINT}/health
  Telegram:    POST ${AGENT_ENDPOINT}/webhook/telegram
  Webchat:     POST ${AGENT_ENDPOINT}/webchat/message
  A2A:         POST ${AGENT_ENDPOINT}/a2a/messages
  Set webhook: curl -X POST "https://api.telegram.org/bot\${TELEGRAM_BOT_TOKEN}/setWebhook?url=${AGENT_ENDPOINT}/webhook/telegram"

AGENT

[[ "$ROLE" == "orchestrator" || "$ROLE" == "all" ]] && cat <<ORCH
  ── Orchestrator ──
  Watch:       clawmacdo watch --agent ${AGENT_ID} --channel telegram
  Fleet:       clawmacdo fleet status
  Roster:      redis-cli SMEMBERS openclaw:agent:roster
  Agent meta:  redis-cli HGETALL openclaw:agent:${AGENT_ID}:meta

ORCH

[[ "$ROLE" == "tracer" || "$ROLE" == "all" ]] && cat <<TRACER
  ── Tracer ──
  DB:          ${DATABASE_URL}
  Query:       psql "${DATABASE_URL}" -c "SELECT * FROM message_trace LIMIT 5;"

TRACER

cat <<COMMON
  ── Common ──
  Logs:        journalctl -u openclaw-${ROLE} -f
  Config:      $CONFIG_DIR/openclaw.env
  Uninstall:   $INSTALL_DIR/uninstall.sh

COMMON

info "Done 🦀"
