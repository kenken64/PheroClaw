use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use pheroclaw_messaging::{ClawMessage, MessageChannel, MessageTarget};

use crate::AgentState;

// ---------------------------------------------------------------------------
// Request / Response types
// ---------------------------------------------------------------------------

#[derive(Deserialize, Debug)]
pub struct WebchatRequest {
    pub session_id: String,
    pub text: String,
}

#[derive(Serialize)]
struct WebchatResponse {
    pub accepted: bool,
    pub message_id: String,
}

// ---------------------------------------------------------------------------
// POST /webchat/message
// ---------------------------------------------------------------------------

pub async fn message(
    State(state): State<Arc<AgentState>>,
    Json(req): Json<WebchatRequest>,
) -> impl IntoResponse {
    info!(
        session_id = %req.session_id,
        text = %req.text,
        "webchat message received"
    );

    let payload = serde_json::json!({
        "text": req.text,
        "session_id": req.session_id,
    });

    let msg = ClawMessage::from_channel(
        format!("webchat:{}", req.session_id),
        MessageTarget::Agent(state.agent_id.clone()),
        MessageChannel::Webchat,
        req.session_id,
        payload,
    );

    let message_id = msg.id.to_string();

    if let Err(e) = state.gateway.send_message(&msg).await {
        warn!(error = %e, "failed to send webchat message to gateway");
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(WebchatResponse {
                accepted: false,
                message_id: String::new(),
            }),
        );
    }

    (
        StatusCode::OK,
        Json(WebchatResponse {
            accepted: true,
            message_id,
        }),
    )
}
