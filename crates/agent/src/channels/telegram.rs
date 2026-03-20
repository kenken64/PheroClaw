use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use serde::Deserialize;
use tracing::{info, warn};

use pheroclaw_messaging::{ClawMessage, MessageChannel, MessageTarget};

use crate::AgentState;

// ---------------------------------------------------------------------------
// Minimal Telegram types (only what we need for webhook handling)
// ---------------------------------------------------------------------------

#[derive(Deserialize, Debug)]
pub struct TelegramUpdate {
    #[allow(dead_code)]
    pub update_id: i64,
    pub message: Option<TelegramMessage>,
}

#[derive(Deserialize, Debug)]
pub struct TelegramMessage {
    pub message_id: i64,
    pub chat: TelegramChat,
    pub text: Option<String>,
}

#[derive(Deserialize, Debug)]
pub struct TelegramChat {
    pub id: i64,
}

// ---------------------------------------------------------------------------
// POST /webhook/telegram
// ---------------------------------------------------------------------------

pub async fn webhook(
    State(state): State<Arc<AgentState>>,
    Json(update): Json<TelegramUpdate>,
) -> impl IntoResponse {
    let message = match update.message {
        Some(m) => m,
        None => {
            // Not every update contains a message (could be callback_query, etc.).
            return StatusCode::OK;
        }
    };

    let text = match &message.text {
        Some(t) => t.clone(),
        None => {
            // Non-text messages (photos, stickers, etc.) — skip for now.
            return StatusCode::OK;
        }
    };

    let chat_id = message.chat.id.to_string();

    info!(
        chat_id = %chat_id,
        text = %text,
        "telegram webhook received"
    );

    let payload = serde_json::json!({
        "text": text,
        "chat_id": chat_id,
        "message_id": message.message_id,
    });

    let msg = ClawMessage::from_channel(
        format!("telegram:{chat_id}"),
        MessageTarget::Agent(state.agent_id.clone()),
        MessageChannel::Telegram,
        chat_id,
        payload,
    );

    if let Err(e) = state.gateway.send_message(&msg).await {
        warn!(error = %e, "failed to send telegram message to gateway");
        return StatusCode::INTERNAL_SERVER_ERROR;
    }

    StatusCode::OK
}
