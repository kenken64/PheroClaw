#!/usr/bin/env bash
# ═══════════════════════════════════════════════════════════════════
# PheroClaw Deploy — centralized deployment from local machine
# ═══════════════════════════════════════════════════════════════════
#
# Runs locally, SSHes into remote hosts to deploy the full stack.
# Usage: bash deploy.sh
#
# ═══════════════════════════════════════════════════════════════════

set -euo pipefail

# ─── Fleet configuration ─────────────────────────────────────────
# Infrastructure host (runs Redis, PostgreSQL, gateway, orchestrator, tracer)
INFRA_HOST="root@10.104.0.2"
INFRA_REDIS_URL="redis://127.0.0.1:6379"
INFRA_DB_URL="postgres://openclaw_tracer:openclaw_tracer@127.0.0.1:5432/openclaw_trace"
GATEWAY_LISTEN="0.0.0.0:8443"
GATEWAY_PUBLIC="10.104.0.2:8443"

# Agent definitions
AGENT_1_HOST="root@agent-alpha-host"
AGENT_1_ID="agent-alpha"
AGENT_1_GROUPS="nlp,chat"
AGENT_1_CORE_URL="http://127.0.0.1:8080"
AGENT_1_LISTEN="0.0.0.0:9090"

AGENT_2_HOST="root@agent-beta-host"
AGENT_2_ID="agent-beta"
AGENT_2_GROUPS="search"
AGENT_2_CORE_URL="http://127.0.0.1:8080"
AGENT_2_LISTEN="0.0.0.0:9090"

AGENT_COUNT=2

# ACL rules: each entry is "groupA:groupB" — normalized alphabetically
ACL_RULES=("nlp:search" "chat:nlp")

# ─── Fixed paths ─────────────────────────────────────────────────
REMOTE_BIN="/usr/local/bin"
REMOTE_CFG="/etc/openclaw"
KEYS_FILE=".deploy-keys"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# ─── Colors & logging ───────────────────────────────────────────
R='\033[0;31m' G='\033[0;32m' Y='\033[1;33m' C='\033[0;36m' B='\033[1m' N='\033[0m'

info()   { echo -e "${G}[deploy]${N} $*"; }
warn()   { echo -e "${Y}[deploy]${N} $*"; }
fail()   { echo -e "${R}[deploy]${N} $*" >&2; return 1; }
banner() { echo -e "\n${C}═══ $* ═══${N}\n"; }
step()   { echo -e "  ${B}→${N} $*"; }

# ─── Preflight: check local prerequisites ───────────────────────

preflight() {
    local missing=()
    local warnings=()

    # Always required (local tools)
    for cmd in ssh scp curl openssl git; do
        command -v "$cmd" &>/dev/null || missing+=("$cmd")
    done

    # Rust build toolchain
    if ! command -v cargo &>/dev/null; then
        missing+=("cargo (install via https://rustup.rs)")
    else
        # cargo subcommands needed for pre-build checks
        if ! cargo fmt --version &>/dev/null; then
            missing+=("rustfmt (rustup component add rustfmt)")
        fi
        if ! cargo clippy --version &>/dev/null; then
            missing+=("clippy (rustup component add clippy)")
        fi
    fi

    # macOS cross-compilation needs cross + Docker
    if [[ "$(uname -s)" == "Darwin" ]]; then
        if ! command -v cross &>/dev/null; then
            missing+=("cross (cargo install cross --git https://github.com/cross-rs/cross)")
        fi
        if ! command -v docker &>/dev/null; then
            missing+=("docker (cross requires Docker — https://docs.docker.com/get-docker/)")
        fi
    fi

    # gh CLI — needed if you want GitHub release integration later
    if ! command -v gh &>/dev/null; then
        warnings+=("gh CLI not found — GitHub operations won't be available")
    fi

    if [[ ${#missing[@]} -gt 0 ]]; then
        echo ""
        echo -e "${R}[deploy]${N} Missing required tools:"
        for tool in "${missing[@]}"; do
            echo -e "  ${R}✗${N} ${tool}"
        done
        echo ""
        fail "Install the tools above and retry"
    fi

    for w in "${warnings[@]}"; do
        warn "$w"
    done

    info "Preflight OK — ssh, scp, curl, openssl, git, cargo, rustfmt, clippy"
}

preflight

# ─── Helpers ─────────────────────────────────────────────────────

# Read AGENT_N_FIELD via indirect expansion
agent_var() {
    local idx="$1" field="$2"
    local varname="AGENT_${idx}_${field}"
    echo "${!varname}"
}

# Normalize a rule pair alphabetically (matches Rust acl::normalize_rule)
normalize_rule() {
    local pair="$1"
    local a b
    a="${pair%%:*}"
    b="${pair##*:}"
    if [[ "$a" < "$b" || "$a" == "$b" ]]; then
        echo "${a}:${b}"
    else
        echo "${b}:${a}"
    fi
}

# Extract host/IP from SSH target (strip user@)
ssh_host_ip() {
    echo "$1" | sed 's/^[^@]*@//'
}

# ─── API Key Management ─────────────────────────────────────────
# Uses .deploy-keys file as flat store (bash 3 compatible — no assoc arrays)

keys_path() { echo "$SCRIPT_DIR/$KEYS_FILE"; }

load_keys() {
    [[ -f "$(keys_path)" ]] || touch "$(keys_path)"
    chmod 600 "$(keys_path)"
}

# Get key for an agent_id; returns empty string if not found
get_key() {
    local agent_id="$1"
    [[ -f "$(keys_path)" ]] || return 0
    grep "^${agent_id}=" "$(keys_path)" 2>/dev/null | head -1 | cut -d'=' -f2-
}

# Set key for an agent_id (upsert)
set_key() {
    local agent_id="$1" hex_key="$2"
    local kp
    kp="$(keys_path)"
    # Remove old entry if present, then append
    if grep -q "^${agent_id}=" "$kp" 2>/dev/null; then
        sed -i.bak "/^${agent_id}=/d" "$kp" && rm -f "${kp}.bak"
    fi
    echo "${agent_id}=${hex_key}" >> "$kp"
}

ensure_key() {
    local agent_id="$1"
    local existing
    existing="$(get_key "$agent_id")"
    if [[ -z "$existing" ]]; then
        local new_key
        new_key="$(openssl rand -hex 24)"
        set_key "$agent_id" "$new_key"
        step "Generated API key for ${B}${agent_id}${N}"
    fi
}

# Build GATEWAY_API_KEYS env value: key1=agent-alpha,key2=agent-beta,...
build_gateway_keys() {
    local result=""
    while IFS='=' read -r agent_id hex_key; do
        [[ -z "$agent_id" || "$agent_id" == \#* ]] && continue
        [[ -n "$result" ]] && result="${result},"
        result="${result}${hex_key}=${agent_id}"
    done < "$(keys_path)"
    echo "$result"
}

# ─── Build target detection ─────────────────────────────────────

detect_build_target() {
    if [[ "$(uname -s)" == "Darwin" ]]; then
        echo "cross"
    else
        echo "cargo"
    fi
}

bin_dir() {
    if [[ "$(uname -s)" == "Darwin" ]]; then
        echo "$SCRIPT_DIR/target/x86_64-unknown-linux-gnu/release"
    else
        echo "$SCRIPT_DIR/target/release"
    fi
}

# ═══════════════════════════════════════════════════════════════════
# 1) Build release binaries
# ═══════════════════════════════════════════════════════════════════

do_build() {
    banner "Build release binaries"

    # 1. Format check
    step "Formatting (cargo fmt --all --check)..."
    if ! (cd "$SCRIPT_DIR" && cargo fmt --all --check 2>&1); then
        warn "Code is not formatted — running cargo fmt --all"
        (cd "$SCRIPT_DIR" && cargo fmt --all)
        info "Formatted"
    else
        info "Code is formatted"
    fi

    # 2. Lint
    step "Linting (cargo clippy)..."
    (cd "$SCRIPT_DIR" && cargo clippy -- -D warnings)
    info "Clippy passed"

    # 3. Build
    local builder
    builder="$(detect_build_target)"

    if [[ "$builder" == "cross" ]]; then
        step "Building with cross for x86_64-unknown-linux-gnu..."
        (cd "$SCRIPT_DIR" && cross build --release --target x86_64-unknown-linux-gnu)
    else
        step "Building with cargo (native Linux)..."
        (cd "$SCRIPT_DIR" && cargo build --release)
    fi

    # 4. Report binary sizes
    local bd
    bd="$(bin_dir)"

    echo ""
    info "Binary sizes:"
    for bin in gateway orchestrator tracer agent; do
        if [[ -f "$bd/$bin" ]]; then
            local size
            size=$(ls -lh "$bd/$bin" | awk '{print $5}')
            step "$bin  ${size}"
        else
            warn "$bin not found at $bd/$bin"
        fi
    done

    info "Build complete"
}

# ═══════════════════════════════════════════════════════════════════
# 2) Deploy infrastructure (Redis + PostgreSQL)
# ═══════════════════════════════════════════════════════════════════

do_deploy_infra() {
    banner "Deploy infrastructure"

    step "Uploading docker-compose.yml to ${INFRA_HOST}"
    ssh "$INFRA_HOST" "mkdir -p /opt/openclaw"
    scp "$SCRIPT_DIR/docker-compose.yml" "${INFRA_HOST}:/opt/openclaw/docker-compose.yml"

    step "Starting containers"
    ssh "$INFRA_HOST" "cd /opt/openclaw && docker compose up -d"

    # Wait for Redis
    step "Waiting for Redis..."
    local retries=15
    while ! ssh "$INFRA_HOST" "redis-cli -u '$INFRA_REDIS_URL' PING" 2>/dev/null | grep -q PONG; do
        retries=$((retries - 1))
        if [[ $retries -le 0 ]]; then
            fail "Redis did not become ready"
        fi
        sleep 2
    done
    info "Redis is ready"

    # Wait for PostgreSQL
    step "Waiting for PostgreSQL..."
    retries=15
    while ! ssh "$INFRA_HOST" "pg_isready -h 127.0.0.1 -p 5432 -U openclaw_tracer" &>/dev/null; do
        retries=$((retries - 1))
        if [[ $retries -le 0 ]]; then
            fail "PostgreSQL did not become ready"
        fi
        sleep 2
    done
    info "PostgreSQL is ready"

    info "Infrastructure deployed"
}

# ═══════════════════════════════════════════════════════════════════
# 3) Deploy gateway
# ═══════════════════════════════════════════════════════════════════

do_deploy_gateway() {
    banner "Deploy gateway"

    load_keys

    # Ensure keys for all agents + orchestrator
    for i in $(seq 1 "$AGENT_COUNT"); do
        ensure_key "$(agent_var "$i" ID)"
    done
    ensure_key "orchestrator"

    local gw_keys
    gw_keys="$(build_gateway_keys)"

    local bd
    bd="$(bin_dir)"
    [[ -f "$bd/gateway" ]] || { fail "gateway binary not found — run build first"; }

    step "Uploading gateway binary to ${INFRA_HOST}"
    scp "$bd/gateway" "${INFRA_HOST}:${REMOTE_BIN}/gateway"
    ssh "$INFRA_HOST" "chmod +x ${REMOTE_BIN}/gateway"

    step "Writing gateway.env"
    ssh "$INFRA_HOST" "mkdir -p ${REMOTE_CFG} && cat > ${REMOTE_CFG}/gateway.env" <<EOF
REDIS_URL=${INFRA_REDIS_URL}
GATEWAY_LISTEN=${GATEWAY_LISTEN}
GATEWAY_API_KEYS=${gw_keys}
EOF

    step "Creating systemd unit"
    ssh "$INFRA_HOST" "cat > /etc/systemd/system/pheroclaw-gateway.service" <<EOF
[Unit]
Description=PheroClaw Gateway
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
EnvironmentFile=${REMOTE_CFG}/gateway.env
ExecStart=${REMOTE_BIN}/gateway
Restart=always
RestartSec=5
StandardOutput=journal
StandardError=journal
SyslogIdentifier=pheroclaw-gateway

[Install]
WantedBy=multi-user.target
EOF

    ssh "$INFRA_HOST" "systemctl daemon-reload && systemctl enable pheroclaw-gateway && systemctl restart pheroclaw-gateway"

    # Health check
    step "Health check..."
    sleep 2
    local gw_ip
    gw_ip="$(ssh_host_ip "$INFRA_HOST")"
    if curl -sf "http://${GATEWAY_PUBLIC}/health" >/dev/null 2>&1; then
        info "Gateway is healthy"
    else
        warn "Gateway health check failed — may need a moment to start"
    fi

    info "Gateway deployed"
}

# ═══════════════════════════════════════════════════════════════════
# 4) Deploy orchestrator
# ═══════════════════════════════════════════════════════════════════

do_deploy_orchestrator() {
    banner "Deploy orchestrator"

    local bd
    bd="$(bin_dir)"
    [[ -f "$bd/orchestrator" ]] || { fail "orchestrator binary not found — run build first"; }

    step "Uploading orchestrator binary to ${INFRA_HOST}"
    scp "$bd/orchestrator" "${INFRA_HOST}:${REMOTE_BIN}/orchestrator"
    ssh "$INFRA_HOST" "chmod +x ${REMOTE_BIN}/orchestrator"

    step "Writing orchestrator.env"
    ssh "$INFRA_HOST" "mkdir -p ${REMOTE_CFG} && cat > ${REMOTE_CFG}/orchestrator.env" <<EOF
REDIS_URL=${INFRA_REDIS_URL}
EOF

    step "Creating systemd unit"
    ssh "$INFRA_HOST" "cat > /etc/systemd/system/pheroclaw-orchestrator.service" <<EOF
[Unit]
Description=PheroClaw Orchestrator
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
EnvironmentFile=${REMOTE_CFG}/orchestrator.env
ExecStart=${REMOTE_BIN}/orchestrator
Restart=always
RestartSec=5
StandardOutput=journal
StandardError=journal
SyslogIdentifier=pheroclaw-orchestrator

[Install]
WantedBy=multi-user.target
EOF

    ssh "$INFRA_HOST" "systemctl daemon-reload && systemctl enable pheroclaw-orchestrator && systemctl restart pheroclaw-orchestrator"

    step "Verifying..."
    sleep 2
    if ssh "$INFRA_HOST" "systemctl is-active pheroclaw-orchestrator" | grep -q active; then
        info "Orchestrator is active"
    else
        warn "Orchestrator may not have started — check logs with: ssh ${INFRA_HOST} journalctl -u pheroclaw-orchestrator -n 20"
    fi

    info "Orchestrator deployed"
}

# ═══════════════════════════════════════════════════════════════════
# 5) Deploy tracer
# ═══════════════════════════════════════════════════════════════════

do_deploy_tracer() {
    banner "Deploy tracer"

    local bd
    bd="$(bin_dir)"
    [[ -f "$bd/tracer" ]] || { fail "tracer binary not found — run build first"; }

    step "Uploading tracer binary to ${INFRA_HOST}"
    scp "$bd/tracer" "${INFRA_HOST}:${REMOTE_BIN}/tracer"
    ssh "$INFRA_HOST" "chmod +x ${REMOTE_BIN}/tracer"

    step "Writing tracer.env"
    ssh "$INFRA_HOST" "mkdir -p ${REMOTE_CFG} && cat > ${REMOTE_CFG}/tracer.env" <<EOF
REDIS_URL=${INFRA_REDIS_URL}
DATABASE_URL=${INFRA_DB_URL}
EOF

    step "Creating systemd unit"
    ssh "$INFRA_HOST" "cat > /etc/systemd/system/pheroclaw-tracer.service" <<EOF
[Unit]
Description=PheroClaw Tracer
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
EnvironmentFile=${REMOTE_CFG}/tracer.env
ExecStart=${REMOTE_BIN}/tracer
Restart=always
RestartSec=5
StandardOutput=journal
StandardError=journal
SyslogIdentifier=pheroclaw-tracer

[Install]
WantedBy=multi-user.target
EOF

    ssh "$INFRA_HOST" "systemctl daemon-reload && systemctl enable pheroclaw-tracer && systemctl restart pheroclaw-tracer"

    step "Verifying..."
    sleep 2
    if ssh "$INFRA_HOST" "systemctl is-active pheroclaw-tracer" | grep -q active; then
        info "Tracer is active"
    else
        warn "Tracer may not have started — check logs with: ssh ${INFRA_HOST} journalctl -u pheroclaw-tracer -n 20"
    fi

    info "Tracer deployed"
}

# ═══════════════════════════════════════════════════════════════════
# 6) Deploy agent(s)
# ═══════════════════════════════════════════════════════════════════

deploy_single_agent() {
    local idx="$1"
    local host id groups core_url listen
    host="$(agent_var "$idx" HOST)"
    id="$(agent_var "$idx" ID)"
    groups="$(agent_var "$idx" GROUPS)"
    core_url="$(agent_var "$idx" CORE_URL)"
    listen="$(agent_var "$idx" LISTEN)"

    load_keys
    ensure_key "$id"

    local bd
    bd="$(bin_dir)"
    [[ -f "$bd/agent" ]] || { fail "agent binary not found — run build first"; }

    step "Deploying agent ${B}${id}${N} to ${host}"

    scp "$bd/agent" "${host}:${REMOTE_BIN}/agent"
    ssh "$host" "chmod +x ${REMOTE_BIN}/agent"

    ssh "$host" "mkdir -p ${REMOTE_CFG} && cat > ${REMOTE_CFG}/agent.env" <<EOF
GATEWAY_URL=http://${GATEWAY_PUBLIC}
GATEWAY_API_KEY=$(get_key "$id")
OPENCLAW_AGENT_ID=${id}
OPENCLAW_CORE_URL=${core_url}
SIDECAR_LISTEN=${listen}
EOF

    ssh "$host" "cat > /etc/systemd/system/pheroclaw-agent.service" <<EOF
[Unit]
Description=PheroClaw Agent (${id})
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
EnvironmentFile=${REMOTE_CFG}/agent.env
ExecStart=${REMOTE_BIN}/agent
Restart=always
RestartSec=5
StandardOutput=journal
StandardError=journal
SyslogIdentifier=pheroclaw-agent

[Install]
WantedBy=multi-user.target
EOF

    ssh "$host" "systemctl daemon-reload && systemctl enable pheroclaw-agent && systemctl restart pheroclaw-agent"

    # Health check
    sleep 2
    local agent_ip port
    agent_ip="$(ssh_host_ip "$host")"
    port="${listen##*:}"
    if curl -sf "http://${agent_ip}:${port}/health" >/dev/null 2>&1; then
        info "Agent ${id} is healthy"
    else
        warn "Agent ${id} health check failed — may need a moment to start"
    fi
}

do_deploy_agents() {
    banner "Deploy agent(s)"

    echo "  Which agents to deploy?"
    echo "    a) All agents (1-${AGENT_COUNT})"
    for i in $(seq 1 "$AGENT_COUNT"); do
        echo "    ${i}) $(agent_var "$i" ID) ($(agent_var "$i" HOST))"
    done
    echo ""
    read -rp "  Choice [a]: " choice
    choice="${choice:-a}"

    if [[ "$choice" == "a" ]]; then
        for i in $(seq 1 "$AGENT_COUNT"); do
            deploy_single_agent "$i"
        done
    elif [[ "$choice" =~ ^[0-9]+$ ]] && (( choice >= 1 && choice <= AGENT_COUNT )); then
        deploy_single_agent "$choice"
    else
        warn "Invalid choice: $choice"
        return 1
    fi

    info "Agent deployment complete"
}

# ═══════════════════════════════════════════════════════════════════
# 7) Configure ACL (groups + P2P rules)
# ═══════════════════════════════════════════════════════════════════

acl_set_groups() {
    step "Setting agent groups in Redis"
    for i in $(seq 1 "$AGENT_COUNT"); do
        local id groups
        id="$(agent_var "$i" ID)"
        groups="$(agent_var "$i" GROUPS)"
        ssh "$INFRA_HOST" "redis-cli HSET 'openclaw:agent:${id}:meta' groups '${groups}'" >/dev/null
        info "  ${id} → groups: ${groups}"
    done
}

acl_add_rules() {
    step "Adding P2P rules"
    for rule in "${ACL_RULES[@]}"; do
        local normalized
        normalized="$(normalize_rule "$rule")"
        ssh "$INFRA_HOST" "redis-cli SADD 'openclaw:acl:p2p-rules' '${normalized}'" >/dev/null
        info "  Added rule: ${normalized}"
    done
}

acl_list() {
    step "Current ACL state"
    echo ""
    echo -e "  ${B}P2P Rules:${N}"
    ssh "$INFRA_HOST" "redis-cli SMEMBERS 'openclaw:acl:p2p-rules'" | while read -r rule; do
        echo "    - ${rule}"
    done
    echo ""
    echo -e "  ${B}Agent Groups:${N}"
    for i in $(seq 1 "$AGENT_COUNT"); do
        local id groups
        id="$(agent_var "$i" ID)"
        groups=$(ssh "$INFRA_HOST" "redis-cli HGET 'openclaw:agent:${id}:meta' groups" 2>/dev/null || echo "(not set)")
        echo "    ${id}: ${groups}"
    done
}

acl_remove_rule() {
    echo ""
    echo -e "  Current rules:"
    local rules_list
    rules_list=$(ssh "$INFRA_HOST" "redis-cli SMEMBERS 'openclaw:acl:p2p-rules'" 2>/dev/null)
    if [[ -z "$rules_list" ]]; then
        info "No rules to remove"
        return
    fi
    local idx=1
    declare -a rule_arr
    while read -r rule; do
        echo "    ${idx}) ${rule}"
        rule_arr[$idx]="$rule"
        idx=$((idx + 1))
    done <<< "$rules_list"

    read -rp "  Remove which rule? [number]: " pick
    if [[ -n "${rule_arr[$pick]:-}" ]]; then
        ssh "$INFRA_HOST" "redis-cli SREM 'openclaw:acl:p2p-rules' '${rule_arr[$pick]}'" >/dev/null
        info "Removed: ${rule_arr[$pick]}"
    else
        warn "Invalid selection"
    fi
}

do_configure_acl() {
    banner "Configure ACL"

    echo "  a) Set agent groups (from config)"
    echo "  b) Add P2P rules (from config)"
    echo "  c) List current rules + groups"
    echo "  d) Remove a rule"
    echo "  e) Apply all from config (a + b)"
    echo ""
    read -rp "  Choice: " choice

    case "$choice" in
        a) acl_set_groups ;;
        b) acl_add_rules ;;
        c) acl_list ;;
        d) acl_remove_rule ;;
        e) acl_set_groups; acl_add_rules ;;
        *) warn "Invalid choice: $choice" ;;
    esac
}

# ═══════════════════════════════════════════════════════════════════
# 8) Verify fleet
# ═══════════════════════════════════════════════════════════════════

do_verify() {
    banner "Verify fleet"

    local all_ok=true

    # Gateway health
    echo -e "  ${B}Gateway${N}"
    if curl -sf "http://${GATEWAY_PUBLIC}/health" >/dev/null 2>&1; then
        echo -e "    health: ${G}OK${N}"
    else
        echo -e "    health: ${R}FAIL${N}"
        all_ok=false
    fi

    # Roster via gateway API
    load_keys
    local orch_key
    orch_key="$(get_key orchestrator)"
    if [[ -n "$orch_key" ]]; then
        echo -e "  ${B}Roster${N}"
        local roster
        roster=$(curl -sf -H "X-API-Key: ${orch_key}" "http://${GATEWAY_PUBLIC}/api/v1/roster" 2>/dev/null || echo "FAIL")
        if [[ "$roster" != "FAIL" ]]; then
            echo -e "    ${roster}"
        else
            echo -e "    ${R}Could not fetch roster${N}"
            all_ok=false
        fi
    fi

    # Orchestrator
    echo -e "  ${B}Orchestrator${N}"
    if ssh "$INFRA_HOST" "systemctl is-active pheroclaw-orchestrator" 2>/dev/null | grep -q active; then
        echo -e "    status: ${G}active${N}"
    else
        echo -e "    status: ${R}inactive${N}"
        all_ok=false
    fi

    # Tracer
    echo -e "  ${B}Tracer${N}"
    if ssh "$INFRA_HOST" "systemctl is-active pheroclaw-tracer" 2>/dev/null | grep -q active; then
        echo -e "    status: ${G}active${N}"
    else
        echo -e "    status: ${R}inactive${N}"
        all_ok=false
    fi

    # Each agent health
    for i in $(seq 1 "$AGENT_COUNT"); do
        local id host listen agent_ip port
        id="$(agent_var "$i" ID)"
        host="$(agent_var "$i" HOST)"
        listen="$(agent_var "$i" LISTEN)"
        agent_ip="$(ssh_host_ip "$host")"
        port="${listen##*:}"

        echo -e "  ${B}Agent: ${id}${N}"
        if curl -sf "http://${agent_ip}:${port}/health" >/dev/null 2>&1; then
            echo -e "    health: ${G}OK${N}"
        else
            echo -e "    health: ${R}FAIL${N}"
            all_ok=false
        fi
    done

    # Redis ACL state
    echo -e "  ${B}Redis ACL${N}"
    echo -e "    Rules:"
    ssh "$INFRA_HOST" "redis-cli SMEMBERS 'openclaw:acl:p2p-rules'" 2>/dev/null | while read -r rule; do
        echo "      - ${rule}"
    done
    echo -e "    Groups:"
    for i in $(seq 1 "$AGENT_COUNT"); do
        local id groups
        id="$(agent_var "$i" ID)"
        groups=$(ssh "$INFRA_HOST" "redis-cli HGET 'openclaw:agent:${id}:meta' groups" 2>/dev/null || echo "(unknown)")
        echo "      ${id}: ${groups}"
    done

    # Summary
    echo ""
    if $all_ok; then
        info "Fleet is ${G}healthy${N}"
    else
        warn "Some components are not healthy — check details above"
    fi
}

# ═══════════════════════════════════════════════════════════════════
# 9) Full deploy (1-8 in sequence)
# ═══════════════════════════════════════════════════════════════════

do_full_deploy() {
    banner "Full deploy"
    info "This will run steps 1-8 in sequence."
    read -rp "  Continue? [y/N]: " confirm
    [[ "$confirm" =~ ^[Yy]$ ]] || { info "Aborted"; return; }

    do_build

    read -rp "  Build complete. Continue to infrastructure? [y/N]: " confirm
    [[ "$confirm" =~ ^[Yy]$ ]] || { info "Stopped after build"; return; }
    do_deploy_infra

    read -rp "  Infrastructure ready. Continue to gateway? [y/N]: " confirm
    [[ "$confirm" =~ ^[Yy]$ ]] || { info "Stopped after infrastructure"; return; }
    do_deploy_gateway

    read -rp "  Gateway deployed. Continue to orchestrator? [y/N]: " confirm
    [[ "$confirm" =~ ^[Yy]$ ]] || { info "Stopped after gateway"; return; }
    do_deploy_orchestrator

    read -rp "  Orchestrator deployed. Continue to tracer? [y/N]: " confirm
    [[ "$confirm" =~ ^[Yy]$ ]] || { info "Stopped after orchestrator"; return; }
    do_deploy_tracer

    read -rp "  Tracer deployed. Deploy all agents? [y/N]: " confirm
    if [[ "$confirm" =~ ^[Yy]$ ]]; then
        for i in $(seq 1 "$AGENT_COUNT"); do
            deploy_single_agent "$i"
        done
    else
        info "Skipped agent deployment"
    fi

    read -rp "  Configure ACL from config? [y/N]: " confirm
    if [[ "$confirm" =~ ^[Yy]$ ]]; then
        acl_set_groups
        acl_add_rules
    else
        info "Skipped ACL configuration"
    fi

    read -rp "  Run fleet verification? [y/N]: " confirm
    if [[ "$confirm" =~ ^[Yy]$ ]]; then
        do_verify
    fi

    banner "Full deploy complete"
}

# ═══════════════════════════════════════════════════════════════════
# Main menu
# ═══════════════════════════════════════════════════════════════════

show_menu() {
    echo ""
    echo -e "${C}═══ PheroClaw Deploy ═══${N}"
    echo ""
    echo "  1) Build release binaries"
    echo "  2) Deploy infrastructure (Redis + PostgreSQL)"
    echo "  3) Deploy gateway"
    echo "  4) Deploy orchestrator"
    echo "  5) Deploy tracer"
    echo "  6) Deploy agent(s)"
    echo "  7) Configure ACL (groups + P2P rules)"
    echo "  8) Verify fleet"
    echo "  9) Full deploy (1-8 in sequence)"
    echo "  0) Exit"
    echo ""
}

main() {
    while true; do
        show_menu
        read -rp "  Choice: " choice
        case "$choice" in
            1) do_build ;;
            2) do_deploy_infra ;;
            3) do_deploy_gateway ;;
            4) do_deploy_orchestrator ;;
            5) do_deploy_tracer ;;
            6) do_deploy_agents ;;
            7) do_configure_acl ;;
            8) do_verify ;;
            9) do_full_deploy ;;
            0) info "Goodbye"; exit 0 ;;
            *) warn "Invalid option: $choice" ;;
        esac
    done
}

main
