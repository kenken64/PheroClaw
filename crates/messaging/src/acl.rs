use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use fred::prelude::*;
use tokio::sync::RwLock;
use tracing::debug;

use crate::keys;

// ---------------------------------------------------------------------------
// Verdict
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub enum AclVerdict {
    Allow,
    Deny(String),
}

// ---------------------------------------------------------------------------
// Rule helpers
// ---------------------------------------------------------------------------

/// Normalize a group pair so order doesn't matter: always `min:max`.
pub fn normalize_rule(a: &str, b: &str) -> String {
    if a <= b {
        format!("{a}:{b}")
    } else {
        format!("{b}:{a}")
    }
}

// ---------------------------------------------------------------------------
// Redis operations
// ---------------------------------------------------------------------------

/// Load all P2P rules from Redis.
pub async fn load_p2p_rules(redis: &Client) -> Result<HashSet<String>> {
    let rules: Vec<String> = redis.smembers(keys::P2P_RULES_KEY).await?;
    Ok(rules.into_iter().collect())
}

/// Add a bidirectional P2P rule between two groups.
pub async fn add_p2p_rule(redis: &Client, group_a: &str, group_b: &str) -> Result<()> {
    let rule = normalize_rule(group_a, group_b);
    redis.sadd::<(), _, _>(keys::P2P_RULES_KEY, &rule).await?;
    debug!(rule, "P2P rule added");
    Ok(())
}

/// Remove a bidirectional P2P rule between two groups.
pub async fn remove_p2p_rule(redis: &Client, group_a: &str, group_b: &str) -> Result<()> {
    let rule = normalize_rule(group_a, group_b);
    redis.srem::<(), _, _>(keys::P2P_RULES_KEY, &rule).await?;
    debug!(rule, "P2P rule removed");
    Ok(())
}

/// Get the groups an agent belongs to (from the `groups` field in agent meta hash).
pub async fn get_agent_groups(redis: &Client, agent_id: &str) -> Result<Vec<String>> {
    let key = keys::agent_meta(agent_id);
    let raw: Option<String> = redis.hget(key.as_str(), "groups").await?;
    match raw {
        Some(s) if !s.is_empty() => Ok(s.split(',').map(|g| g.trim().to_string()).collect()),
        _ => Ok(Vec::new()),
    }
}

/// Set the groups an agent belongs to.
pub async fn set_agent_groups(redis: &Client, agent_id: &str, groups: &[String]) -> Result<()> {
    let key = keys::agent_meta(agent_id);
    let value = groups.join(",");
    redis.hset::<(), _, _>(key.as_str(), vec![("groups", value.as_str())]).await?;
    debug!(agent_id, groups = ?groups, "agent groups updated");
    Ok(())
}

/// Check whether a P2P message from sender to target is allowed.
pub async fn check_p2p(redis: &Client, sender_id: &str, target_id: &str) -> Result<AclVerdict> {
    let sender_groups = get_agent_groups(redis, sender_id).await?;
    let target_groups = get_agent_groups(redis, target_id).await?;

    if sender_groups.is_empty() || target_groups.is_empty() {
        return Ok(AclVerdict::Deny(format!(
            "sender or target has no groups: sender={sender_id} target={target_id}"
        )));
    }

    let rules = load_p2p_rules(redis).await?;

    for sg in &sender_groups {
        for tg in &target_groups {
            let normalized = normalize_rule(sg, tg);
            if rules.contains(&normalized) {
                return Ok(AclVerdict::Allow);
            }
        }
    }

    Ok(AclVerdict::Deny(format!(
        "no P2P rule matches: sender groups={sender_groups:?} target groups={target_groups:?}"
    )))
}

/// Find all agents in the roster that belong to a given group.
pub async fn agents_in_group(redis: &Client, group: &str) -> Result<Vec<String>> {
    let roster: Vec<String> = redis.smembers(keys::ROSTER_KEY).await?;
    let mut matched = Vec::new();
    for agent_id in roster {
        let groups = get_agent_groups(redis, &agent_id).await?;
        if groups.iter().any(|g| g == group) {
            matched.push(agent_id);
        }
    }
    Ok(matched)
}

// ---------------------------------------------------------------------------
// In-process cache with TTL
// ---------------------------------------------------------------------------

const CACHE_TTL: Duration = Duration::from_secs(5);

struct CacheEntry<T> {
    value: T,
    fetched_at: Instant,
}

impl<T> CacheEntry<T> {
    fn is_fresh(&self) -> bool {
        self.fetched_at.elapsed() < CACHE_TTL
    }
}

type GroupMap = std::collections::HashMap<String, Vec<String>>;

/// In-process cache for ACL lookups to avoid hitting Redis on every message.
/// Entries expire after 5 seconds.
#[derive(Clone)]
pub struct AclCache {
    rules: Arc<RwLock<Option<CacheEntry<HashSet<String>>>>>,
    agent_groups: Arc<RwLock<Option<CacheEntry<GroupMap>>>>,
}

impl AclCache {
    pub fn new() -> Self {
        Self {
            rules: Arc::new(RwLock::new(None)),
            agent_groups: Arc::new(RwLock::new(None)),
        }
    }

    /// Get P2P rules, using cache if fresh.
    pub async fn get_rules(&self, redis: &Client) -> Result<HashSet<String>> {
        {
            let guard = self.rules.read().await;
            if let Some(entry) = guard.as_ref() {
                if entry.is_fresh() {
                    return Ok(entry.value.clone());
                }
            }
        }
        let rules = load_p2p_rules(redis).await?;
        {
            let mut guard = self.rules.write().await;
            *guard = Some(CacheEntry {
                value: rules.clone(),
                fetched_at: Instant::now(),
            });
        }
        Ok(rules)
    }

    /// Get groups for a specific agent, using cache if fresh.
    pub async fn get_agent_groups(
        &self,
        redis: &Client,
        agent_id: &str,
    ) -> Result<Vec<String>> {
        {
            let guard = self.agent_groups.read().await;
            if let Some(entry) = guard.as_ref() {
                if entry.is_fresh() {
                    if let Some(groups) = entry.value.get(agent_id) {
                        return Ok(groups.clone());
                    }
                }
            }
        }
        let groups = get_agent_groups(redis, agent_id).await?;
        {
            let mut guard = self.agent_groups.write().await;
            let map = guard.get_or_insert_with(|| CacheEntry {
                value: std::collections::HashMap::new(),
                fetched_at: Instant::now(),
            });
            map.value.insert(agent_id.to_string(), groups.clone());
        }
        Ok(groups)
    }

    /// Check P2P access using cached rules and groups.
    pub async fn check_p2p(
        &self,
        redis: &Client,
        sender_id: &str,
        target_id: &str,
    ) -> Result<AclVerdict> {
        let sender_groups = self.get_agent_groups(redis, sender_id).await?;
        let target_groups = self.get_agent_groups(redis, target_id).await?;

        if sender_groups.is_empty() || target_groups.is_empty() {
            return Ok(AclVerdict::Deny(format!(
                "sender or target has no groups: sender={sender_id} target={target_id}"
            )));
        }

        let rules = self.get_rules(redis).await?;

        for sg in &sender_groups {
            for tg in &target_groups {
                let normalized = normalize_rule(sg, tg);
                if rules.contains(&normalized) {
                    return Ok(AclVerdict::Allow);
                }
            }
        }

        Ok(AclVerdict::Deny(format!(
            "no P2P rule matches: sender groups={sender_groups:?} target groups={target_groups:?}"
        )))
    }
}

impl Default for AclCache {
    fn default() -> Self {
        Self::new()
    }
}
