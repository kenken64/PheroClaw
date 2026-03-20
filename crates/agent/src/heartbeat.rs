use std::sync::Arc;
use std::time::Duration;

use crate::AgentState;

/// Background loop that sends heartbeats to the gateway every 30 seconds.
/// Keeps the agent registered in the roster and prevents TTL expiry.
pub async fn heartbeat_loop(state: Arc<AgentState>) {
    loop {
        if let Err(e) = state.gateway.heartbeat(&state.agent_id).await {
            tracing::warn!(error = %e, "heartbeat failed");
        }
        tokio::time::sleep(Duration::from_secs(30)).await;
    }
}
