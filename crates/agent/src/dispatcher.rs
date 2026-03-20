use std::sync::Arc;

use pheroclaw_messaging::{ClawMessage, MessageChannel};
use tracing::{debug, info, warn};

use crate::AgentState;

/// Dispatch a ChannelResponse message to the appropriate external channel API.
pub async fn dispatch_channel_response(state: &Arc<AgentState>, msg: &ClawMessage) {
    match &msg.channel {
        MessageChannel::Telegram => dispatch_telegram(state, msg).await,
        MessageChannel::Webchat => {
            // Webchat responses are typically pulled by the frontend via polling or SSE.
            // For now, log the response. A real implementation would push to a session store.
            debug!(
                msg_id = %msg.id,
                session_id = ?msg.session_id,
                "webchat response ready for delivery"
            );
        }
        channel => {
            debug!(channel = ?channel, msg_id = %msg.id, "channel dispatch not implemented");
        }
    }
}

/// Send a response to a Telegram chat via the Bot API.
async fn dispatch_telegram(state: &Arc<AgentState>, msg: &ClawMessage) {
    let token = match &state.telegram_token {
        Some(t) => t,
        None => {
            warn!(msg_id = %msg.id, "telegram token not configured, cannot dispatch");
            return;
        }
    };

    let chat_id = match &msg.session_id {
        Some(id) => id,
        None => {
            warn!(msg_id = %msg.id, "no session_id (chat_id) for telegram dispatch");
            return;
        }
    };

    // Extract the text from the payload. Accept either a "text" field or the raw string value.
    let text = msg
        .payload
        .get("text")
        .and_then(|v| v.as_str())
        .or_else(|| msg.payload.as_str())
        .unwrap_or("[no text]");

    let url = format!("https://api.telegram.org/bot{token}/sendMessage");

    match state
        .http_client
        .post(&url)
        .json(&serde_json::json!({
            "chat_id": chat_id,
            "text": text,
        }))
        .send()
        .await
    {
        Ok(resp) if resp.status().is_success() => {
            info!(chat_id, msg_id = %msg.id, "telegram message sent");
        }
        Ok(resp) => {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            warn!(status = %status, body, "telegram API error");
        }
        Err(e) => {
            warn!(error = %e, "failed to call telegram API");
        }
    }
}
