use anyhow::Result;
use fred::prelude::Client;
use pheroclaw_messaging as msg;
use tracing::debug;

/// Allow P2P communication between two groups.
pub async fn add_rule(redis: &Client, group_a: &str, group_b: &str) -> Result<()> {
    msg::acl::add_p2p_rule(redis, group_a, group_b).await
}

/// Revoke P2P communication between two groups.
pub async fn remove_rule(redis: &Client, group_a: &str, group_b: &str) -> Result<()> {
    msg::acl::remove_p2p_rule(redis, group_a, group_b).await
}

/// List all active P2P rules as (group_a, group_b) pairs.
pub async fn list_rules(redis: &Client) -> Result<Vec<(String, String)>> {
    let rules = msg::acl::load_p2p_rules(redis).await?;
    let pairs: Vec<(String, String)> = rules
        .into_iter()
        .filter_map(|r| {
            r.split_once(':')
                .map(|(a, b)| (a.to_string(), b.to_string()))
        })
        .collect();
    Ok(pairs)
}

/// Assign groups to an agent.
pub async fn set_agent_groups(
    redis: &Client,
    agent_id: &str,
    groups: &[String],
) -> Result<()> {
    msg::acl::set_agent_groups(redis, agent_id, groups).await
}

/// Broadcast a message to all agents in a specific group.
pub async fn broadcast_to_group(
    redis: &Client,
    group: &str,
    message: &msg::ClawMessage,
) -> Result<Vec<String>> {
    let reached = msg::broadcast_to_group(redis, group, message).await?;
    debug!(group, reached = reached.len(), "orchestrator broadcast to group");
    Ok(reached)
}
