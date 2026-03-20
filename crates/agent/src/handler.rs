use std::sync::Arc;

use pheroclaw_messaging::{ClawMessage, MsgType};
use tracing::{debug, info, warn};

use crate::dispatcher;
use crate::AgentState;

/// Dispatch an inbound message to the appropriate handler based on its type.
pub async fn handle_message(state: &Arc<AgentState>, msg: &ClawMessage) {
    // Defense-in-depth: only accept Task/System/Skill from the orchestrator.
    match &msg.msg_type {
        MsgType::Task | MsgType::System(_) | MsgType::Skill(_)
            if msg.from != "orchestrator" =>
        {
            warn!(
                msg_id = %msg.id,
                from = %msg.from,
                msg_type = ?msg.msg_type,
                "rejected privileged message from non-orchestrator sender"
            );
            return;
        }
        _ => {}
    }

    match &msg.msg_type {
        MsgType::Task => handle_task(state, msg).await,
        MsgType::P2P => {
            info!(
                msg_id = %msg.id,
                from = %msg.from,
                "P2P message received (sender verified by gateway)"
            );
            handle_p2p(state, msg).await;
        }
        MsgType::Skill(name) => handle_skill(state, msg, name).await,
        MsgType::System(cmd) => handle_system(state, msg, cmd).await,
        MsgType::ChannelMessage => handle_channel_message(state, msg).await,
        MsgType::ChannelResponse => {
            dispatcher::dispatch_channel_response(state, msg).await;
        }
        _ => {
            debug!(msg_type = ?msg.msg_type, msg_id = %msg.id, "unhandled message type");
        }
    }
}

/// Forward task to the OpenClaw core backend and send the result back.
async fn handle_task(state: &Arc<AgentState>, msg: &ClawMessage) {
    info!(msg_id = %msg.id, "handling task");

    let url = format!("{}/task", state.core_url);
    let result = state
        .http_client
        .post(&url)
        .json(&msg.payload)
        .send()
        .await;

    let response_payload = match result {
        Ok(resp) => match resp.json::<serde_json::Value>().await {
            Ok(body) => body,
            Err(e) => {
                warn!(error = %e, "failed to parse core response for task");
                serde_json::json!({ "error": e.to_string() })
            }
        },
        Err(e) => {
            warn!(error = %e, "failed to forward task to core");
            serde_json::json!({ "error": e.to_string() })
        }
    };

    let reply = msg.reply(
        state.agent_id.clone(),
        MsgType::TaskResult,
        response_payload,
    );

    if let Err(e) = state.gateway.send_message(&reply).await {
        warn!(error = %e, msg_id = %msg.id, "failed to send task result");
    }
}

/// Handle a P2P message. Check the correlation registry first — if there's a pending
/// request-reply, deliver the message to the waiting future. Otherwise, log it.
async fn handle_p2p(state: &Arc<AgentState>, msg: &ClawMessage) {
    debug!(msg_id = %msg.id, from = %msg.from, "handling P2P message");

    // Check if this is a reply to a pending correlation.
    if let Some(correlation_id) = &msg.correlation_id {
        if state.correlations.resolve(correlation_id, msg.clone()) {
            debug!(correlation_id = %correlation_id, "P2P reply delivered to correlation");
            return;
        }
    }

    // Not a correlated reply — forward to core as an unsolicited P2P message.
    let url = format!("{}/p2p", state.core_url);
    match state.http_client.post(&url).json(&msg.payload).send().await {
        Ok(_) => debug!(msg_id = %msg.id, "P2P message forwarded to core"),
        Err(e) => warn!(error = %e, msg_id = %msg.id, "failed to forward P2P to core"),
    }
}

/// Forward skill invocation to the core backend with the skill name.
async fn handle_skill(state: &Arc<AgentState>, msg: &ClawMessage, skill_name: &str) {
    info!(msg_id = %msg.id, skill = skill_name, "handling skill invocation");

    let url = format!("{}/skill/{}", state.core_url, skill_name);
    let result = state
        .http_client
        .post(&url)
        .json(&msg.payload)
        .send()
        .await;

    let response_payload = match result {
        Ok(resp) => match resp.json::<serde_json::Value>().await {
            Ok(body) => body,
            Err(e) => {
                warn!(error = %e, "failed to parse core response for skill");
                serde_json::json!({ "error": e.to_string() })
            }
        },
        Err(e) => {
            warn!(error = %e, skill = skill_name, "failed to forward skill to core");
            serde_json::json!({ "error": e.to_string() })
        }
    };

    let reply = msg.reply(
        state.agent_id.clone(),
        MsgType::TaskResult,
        response_payload,
    );

    if let Err(e) = state.gateway.send_message(&reply).await {
        warn!(error = %e, msg_id = %msg.id, "failed to send skill result");
    }
}

/// Handle system commands (shutdown, reload, etc.).
async fn handle_system(state: &Arc<AgentState>, msg: &ClawMessage, cmd: &str) {
    info!(msg_id = %msg.id, command = cmd, "handling system command");

    match cmd {
        "shutdown" => {
            info!("received shutdown command — agent will deregister");
            if let Err(e) = state.gateway.deregister(&state.agent_id).await {
                warn!(error = %e, "failed to deregister on shutdown command");
            }
            // The main loop should detect the shutdown and exit.
            // For now, just log it — a real implementation would signal the shutdown channel.
        }
        _ => {
            debug!(command = cmd, "unhandled system command");
        }
    }
}

/// Forward a channel message to the core backend and send the response back via gateway.
async fn handle_channel_message(state: &Arc<AgentState>, msg: &ClawMessage) {
    info!(
        msg_id = %msg.id,
        channel = ?msg.channel,
        session_id = ?msg.session_id,
        "handling channel message"
    );

    let url = format!("{}/channel/message", state.core_url);
    let result = state
        .http_client
        .post(&url)
        .json(&serde_json::json!({
            "channel": msg.channel,
            "session_id": msg.session_id,
            "payload": msg.payload,
        }))
        .send()
        .await;

    let response_payload = match result {
        Ok(resp) => match resp.json::<serde_json::Value>().await {
            Ok(body) => body,
            Err(e) => {
                warn!(error = %e, "failed to parse core response for channel message");
                serde_json::json!({ "error": e.to_string() })
            }
        },
        Err(e) => {
            warn!(error = %e, "failed to forward channel message to core");
            serde_json::json!({ "error": e.to_string() })
        }
    };

    // Send a ChannelResponse back through the gateway.
    let reply = msg.reply(
        state.agent_id.clone(),
        MsgType::ChannelResponse,
        response_payload,
    );

    if let Err(e) = state.gateway.send_message(&reply).await {
        warn!(error = %e, msg_id = %msg.id, "failed to send channel response");
    }
}
