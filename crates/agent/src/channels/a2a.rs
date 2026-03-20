use std::sync::Arc;

use anyhow::{Context, Result};
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use tracing::{info, warn};

use pheroclaw_messaging::ClawMessage;

use crate::AgentState;

// ---------------------------------------------------------------------------
// POST /a2a/messages — receive an A2A message from another agent
// ---------------------------------------------------------------------------

pub async fn receive(
    State(state): State<Arc<AgentState>>,
    Json(msg): Json<ClawMessage>,
) -> impl IntoResponse {
    info!(
        msg_id = %msg.id,
        from = %msg.from,
        "A2A message received"
    );

    // Route the inbound A2A message through the gateway so it enters the
    // normal inbox processing pipeline.
    if let Err(e) = state.gateway.send_message(&msg).await {
        warn!(error = %e, msg_id = %msg.id, "failed to forward A2A message to gateway");
        return StatusCode::INTERNAL_SERVER_ERROR;
    }

    StatusCode::OK
}

// ---------------------------------------------------------------------------
// Outbound A2A helper — send a message directly to another agent's HTTP endpoint
// ---------------------------------------------------------------------------

/// Send a ClawMessage to another agent's A2A endpoint over HTTP.
#[allow(dead_code)]
pub async fn send_a2a(state: &AgentState, target_url: &str, msg: &ClawMessage) -> Result<()> {
    let url = format!("{target_url}/a2a/messages");

    let resp = state
        .http_client
        .post(&url)
        .json(msg)
        .send()
        .await
        .context("A2A send request failed")?;

    let status = resp.status();
    if !status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        anyhow::bail!("A2A send failed ({status}): {body}");
    }

    info!(msg_id = %msg.id, target_url, "A2A message sent");
    Ok(())
}
