use anyhow::Result;
use fred::prelude::*;

use crate::types::ClawMessage;

/// Maximum stream length before trimming (approximate).
pub const MAX_STREAM_LEN: u64 = 10_000;

/// Publish a message to the broadcast stream (orchestrator -> all agents).
pub async fn broadcast(redis: &Client, msg: &ClawMessage) -> Result<()> {
    let _ = (redis, msg);
    todo!("broadcast: XADD to broadcast stream")
}

/// Send a message directly to a specific agent's inbox.
pub async fn send_to_agent(redis: &Client, target_id: &str, msg: &ClawMessage) -> Result<()> {
    let _ = (redis, target_id, msg);
    todo!("send_to_agent: XADD to agent inbox")
}

/// Send a message to the orchestrator via the agent's outbox.
pub async fn send_to_orchestrator(redis: &Client, agent_id: &str, msg: &ClawMessage) -> Result<()> {
    let _ = (redis, agent_id, msg);
    todo!("send_to_orchestrator: XADD to agent outbox")
}

/// Create a consumer group on a stream (idempotent).
pub async fn ensure_consumer_group(
    redis: &Client,
    stream: &str,
    group: &str,
    start_from: &str,
) -> Result<()> {
    let _ = (redis, stream, group, start_from);
    todo!("ensure_consumer_group: XGROUP CREATE with MKSTREAM")
}

/// Agent discovery metadata.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct AgentMeta {
    pub endpoint: String,
    pub cloud: String,
    pub region: String,
    pub instance_id: String,
    pub private_ip: Option<String>,
    pub public_ip: Option<String>,
    pub hostname: String,
    pub version: String,
}

/// Register an agent in the global roster and set its heartbeat.
pub async fn register_agent(redis: &Client, agent_id: &str) -> Result<()> {
    let _ = (redis, agent_id);
    todo!("register_agent: SADD roster + SET heartbeat")
}

/// Refresh an agent's heartbeat (SET with 60s TTL).
pub async fn refresh_heartbeat(redis: &Client, agent_id: &str) -> Result<()> {
    let _ = (redis, agent_id);
    todo!("refresh_heartbeat: SET with EX 60")
}

/// Remove an agent from the roster (graceful shutdown).
pub async fn deregister_agent(redis: &Client, agent_id: &str) -> Result<()> {
    let _ = (redis, agent_id);
    todo!("deregister_agent: SREM roster + DEL heartbeat + DEL meta")
}

/// Get all known agent IDs from the roster.
pub async fn get_roster(redis: &Client) -> Result<Vec<String>> {
    let _ = redis;
    todo!("get_roster: SMEMBERS roster")
}

/// Decode a ClawMessage from a raw stream entry field.
pub fn decode_message(raw: &str) -> Result<ClawMessage> {
    let msg: ClawMessage = serde_json::from_str(raw)?;
    Ok(msg)
}
