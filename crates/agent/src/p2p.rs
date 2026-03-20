use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use pheroclaw_messaging::{ClawMessage, MessageTarget, MsgType};
use tracing::debug;

use crate::AgentState;

/// Send a P2P message to another agent and wait for a correlated reply.
///
/// Creates a P2P message, registers its ID in the correlation registry,
/// sends it via the gateway, and waits for the reply with a timeout.
#[allow(dead_code)]
pub async fn send_and_wait(
    state: &Arc<AgentState>,
    target_id: &str,
    payload: serde_json::Value,
    timeout: Duration,
) -> Result<ClawMessage> {
    let msg = ClawMessage::new(
        state.agent_id.clone(),
        MessageTarget::Agent(target_id.to_string()),
        MsgType::P2P,
        payload,
    );

    let correlation_id = msg.id;
    let rx = state.correlations.register(correlation_id);

    debug!(
        msg_id = %msg.id,
        target = target_id,
        "sending P2P request and waiting for reply"
    );

    state
        .gateway
        .send_message(&msg)
        .await
        .context("failed to send P2P message")?;

    let reply = tokio::time::timeout(timeout, rx)
        .await
        .context("P2P request timed out")?
        .context("correlation channel closed (sender dropped)")?;

    debug!(
        original_id = %correlation_id,
        reply_id = %reply.id,
        "P2P reply received"
    );

    Ok(reply)
}
