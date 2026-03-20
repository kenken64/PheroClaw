use std::collections::HashMap;

use anyhow::Result;
use fred::prelude::*;
use tracing::debug;

use crate::keys;
use crate::types::ClawMessage;

/// Maximum stream length before trimming (approximate).
pub const MAX_STREAM_LEN: u64 = 10_000;

/// Publish a message to the broadcast stream (orchestrator -> all agents).
pub async fn broadcast(redis: &Client, msg: &ClawMessage) -> Result<()> {
    let payload = serde_json::to_string(msg)?;
    redis
        .xadd::<String, _, _, _, _>(
            keys::BROADCAST_STREAM,
            false,
            ("MAXLEN", "~", MAX_STREAM_LEN),
            "*",
            vec![("msg", payload.as_str())],
        )
        .await?;
    debug!(stream = keys::BROADCAST_STREAM, msg_id = %msg.id, "broadcast published");
    Ok(())
}

/// Send a message directly to a specific agent's inbox.
pub async fn send_to_agent(redis: &Client, target_id: &str, msg: &ClawMessage) -> Result<()> {
    let key = keys::agent_inbox(target_id);
    let payload = serde_json::to_string(msg)?;
    redis
        .xadd::<String, _, _, _, _>(
            key.as_str(),
            false,
            ("MAXLEN", "~", MAX_STREAM_LEN),
            "*",
            vec![("msg", payload.as_str())],
        )
        .await?;
    debug!(stream = %key, msg_id = %msg.id, "sent to agent");
    Ok(())
}

/// Send a message to the orchestrator via the agent's outbox.
pub async fn send_to_orchestrator(
    redis: &Client,
    agent_id: &str,
    msg: &ClawMessage,
) -> Result<()> {
    let key = keys::agent_outbox(agent_id);
    let payload = serde_json::to_string(msg)?;
    redis
        .xadd::<String, _, _, _, _>(
            key.as_str(),
            false,
            ("MAXLEN", "~", MAX_STREAM_LEN),
            "*",
            vec![("msg", payload.as_str())],
        )
        .await?;
    debug!(stream = %key, msg_id = %msg.id, "sent to orchestrator");
    Ok(())
}

/// Create a consumer group on a stream (idempotent).
pub async fn ensure_consumer_group(
    redis: &Client,
    stream: &str,
    group: &str,
    start_from: &str,
) -> Result<()> {
    match redis
        .xgroup_create::<(), _, _, _>(stream, group, start_from, true)
        .await
    {
        Ok(()) => {
            debug!(stream, group, "consumer group created");
        }
        Err(e) if e.to_string().contains("BUSYGROUP") => {
            debug!(stream, group, "consumer group already exists");
        }
        Err(e) => return Err(e.into()),
    }
    Ok(())
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
    /// Groups this agent belongs to (for ACL and targeted broadcast).
    #[serde(default)]
    pub groups: Vec<String>,
}

/// Register an agent in the global roster and set its heartbeat.
pub async fn register_agent(redis: &Client, agent_id: &str) -> Result<()> {
    redis
        .sadd::<(), _, _>(keys::ROSTER_KEY, agent_id)
        .await?;
    refresh_heartbeat(redis, agent_id).await?;
    debug!(agent_id, "agent registered");
    Ok(())
}

/// Register an agent with metadata in the global roster.
pub async fn register_agent_with_meta(
    redis: &Client,
    agent_id: &str,
    meta: &AgentMeta,
) -> Result<()> {
    redis
        .sadd::<(), _, _>(keys::ROSTER_KEY, agent_id)
        .await?;

    let key = keys::agent_meta(agent_id);
    let fields: Vec<(&str, String)> = vec![
        ("endpoint", meta.endpoint.clone()),
        ("cloud", meta.cloud.clone()),
        ("region", meta.region.clone()),
        ("instance_id", meta.instance_id.clone()),
        ("private_ip", meta.private_ip.clone().unwrap_or_default()),
        ("public_ip", meta.public_ip.clone().unwrap_or_default()),
        ("hostname", meta.hostname.clone()),
        ("version", meta.version.clone()),
        ("groups", meta.groups.join(",")),
    ];
    redis.hset::<(), _, _>(key.as_str(), fields).await?;

    refresh_heartbeat(redis, agent_id).await?;
    debug!(agent_id, "agent registered with metadata");
    Ok(())
}

/// Refresh an agent's heartbeat (SET with 60s TTL).
pub async fn refresh_heartbeat(redis: &Client, agent_id: &str) -> Result<()> {
    let key = keys::agent_heartbeat(agent_id);
    redis
        .set::<(), _, _>(key.as_str(), "alive", Some(Expiration::EX(60)), None, false)
        .await?;
    Ok(())
}

/// Remove an agent from the roster (graceful shutdown).
pub async fn deregister_agent(redis: &Client, agent_id: &str) -> Result<()> {
    redis
        .srem::<(), _, _>(keys::ROSTER_KEY, agent_id)
        .await?;
    redis
        .del::<(), _>(keys::agent_heartbeat(agent_id))
        .await?;
    redis
        .del::<(), _>(keys::agent_meta(agent_id))
        .await?;
    debug!(agent_id, "agent deregistered");
    Ok(())
}

/// Get all known agent IDs from the roster.
pub async fn get_roster(redis: &Client) -> Result<Vec<String>> {
    let roster: Vec<String> = redis.smembers(keys::ROSTER_KEY).await?;
    Ok(roster)
}

/// Get metadata for a specific agent, if it exists.
pub async fn get_agent_meta(redis: &Client, agent_id: &str) -> Result<Option<AgentMeta>> {
    let key = keys::agent_meta(agent_id);
    let fields: HashMap<String, String> = redis.hgetall(key.as_str()).await?;
    if fields.is_empty() {
        return Ok(None);
    }
    Ok(Some(AgentMeta {
        endpoint: fields.get("endpoint").cloned().unwrap_or_default(),
        cloud: fields.get("cloud").cloned().unwrap_or_default(),
        region: fields.get("region").cloned().unwrap_or_default(),
        instance_id: fields.get("instance_id").cloned().unwrap_or_default(),
        private_ip: fields.get("private_ip").filter(|s| !s.is_empty()).cloned(),
        public_ip: fields.get("public_ip").filter(|s| !s.is_empty()).cloned(),
        hostname: fields.get("hostname").cloned().unwrap_or_default(),
        version: fields.get("version").cloned().unwrap_or_default(),
        groups: fields
            .get("groups")
            .filter(|s| !s.is_empty())
            .map(|s| s.split(',').map(|g| g.trim().to_string()).collect())
            .unwrap_or_default(),
    }))
}

/// Discover all agents with their metadata.
pub async fn discover_all_agents(
    redis: &Client,
) -> Result<Vec<(String, Option<AgentMeta>)>> {
    let roster = get_roster(redis).await?;
    let mut agents = Vec::with_capacity(roster.len());
    for agent_id in roster {
        let meta = get_agent_meta(redis, &agent_id).await?;
        agents.push((agent_id, meta));
    }
    Ok(agents)
}

/// Broadcast a message to all agents in a specific group.
/// Returns the list of agent IDs the message was delivered to.
pub async fn broadcast_to_group(
    redis: &Client,
    group: &str,
    msg: &ClawMessage,
) -> Result<Vec<String>> {
    let agents = crate::acl::agents_in_group(redis, group).await?;
    for agent_id in &agents {
        send_to_agent(redis, agent_id, msg).await?;
    }
    debug!(group, count = agents.len(), "broadcast to group");
    Ok(agents)
}

/// Decode a ClawMessage from a raw stream entry field.
pub fn decode_message(raw: &str) -> Result<ClawMessage> {
    let msg: ClawMessage = serde_json::from_str(raw)?;
    Ok(msg)
}
