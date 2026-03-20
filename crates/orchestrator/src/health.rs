use fred::prelude::*;
use pheroclaw_messaging as msg;

/// Run health check loop every 30 seconds.
pub async fn health_loop(redis: Client) {
    loop {
        if let Err(e) = check_health(&redis).await {
            tracing::warn!(error = %e, "health check error");
        }
        tokio::time::sleep(std::time::Duration::from_secs(30)).await;
    }
}

async fn check_health(redis: &Client) -> anyhow::Result<()> {
    let roster = msg::get_roster(redis).await?;
    let mut alive = 0u32;
    let mut dead = Vec::new();

    for agent_id in &roster {
        let hb_key = msg::agent_heartbeat(agent_id);
        let exists: bool = redis.exists(&hb_key).await?;
        if exists {
            alive += 1;
        } else {
            dead.push(agent_id.clone());
        }
    }

    if !dead.is_empty() {
        tracing::warn!(dead_agents = ?dead, "agents with expired heartbeats");
        for agent_id in &dead {
            msg::deregister_agent(redis, agent_id).await?;
            tracing::info!(agent_id, "removed dead agent from roster");
        }
    }

    tracing::debug!(total = roster.len(), alive, dead = dead.len(), "fleet health check");
    Ok(())
}
