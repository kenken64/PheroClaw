use std::collections::HashMap;
use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use fred::prelude::*;
use serde::{Deserialize, Serialize};
use tracing::{debug, error, warn};

use pheroclaw_messaging as msg;
use pheroclaw_messaging::acl::{AclCache, AclVerdict};

use crate::auth::AuthenticatedIdentity;

// ---------------------------------------------------------------------------
// Shared state
// ---------------------------------------------------------------------------

pub struct GatewayState {
    pub redis: Client,
    /// API key -> agent_id mapping for identity binding.
    pub api_keys: HashMap<String, String>,
    pub acl_cache: AclCache,
}

// ---------------------------------------------------------------------------
// Request / Response types
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
pub struct RegisterRequest {
    pub agent_id: String,
    pub meta: Option<msg::AgentMeta>,
    pub groups: Option<Vec<String>>,
}

#[derive(Deserialize)]
pub struct HeartbeatRequest {
    pub agent_id: String,
}

#[derive(Deserialize)]
pub struct DeregisterRequest {
    pub agent_id: String,
}

#[derive(Deserialize)]
pub struct PollRequest {
    pub agent_id: String,
    pub group: String,
}

#[derive(Serialize)]
pub struct PollMessage {
    pub stream: String,
    pub entry_id: String,
    pub raw: String,
}

#[derive(Deserialize)]
pub struct AckRequest {
    pub stream: String,
    pub group: String,
    pub entry_id: String,
}

#[derive(Serialize)]
pub struct AgentEntry {
    pub agent_id: String,
    pub meta: Option<msg::AgentMeta>,
}

#[derive(Deserialize)]
pub struct BroadcastGroupRequest {
    pub group: String,
    pub message: msg::ClawMessage,
}

// ---------------------------------------------------------------------------
// Error helper
// ---------------------------------------------------------------------------

pub(crate) struct AppError(anyhow::Error);

impl IntoResponse for AppError {
    fn into_response(self) -> axum::response::Response {
        error!(err = %self.0, "request failed");
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": self.0.to_string() })),
        )
            .into_response()
    }
}

impl<E: Into<anyhow::Error>> From<E> for AppError {
    fn from(err: E) -> Self {
        Self(err.into())
    }
}

// ---------------------------------------------------------------------------
// Identity extraction helper
// ---------------------------------------------------------------------------

fn extract_identity(
    extensions: &axum::http::Extensions,
) -> Result<AuthenticatedIdentity, AppError> {
    extensions
        .get::<AuthenticatedIdentity>()
        .cloned()
        .ok_or_else(|| AppError(anyhow::anyhow!("missing authenticated identity")))
}

// ---------------------------------------------------------------------------
// GET /health
// ---------------------------------------------------------------------------

pub async fn health() -> Json<serde_json::Value> {
    Json(serde_json::json!({ "status": "ok" }))
}

// ---------------------------------------------------------------------------
// POST /api/v1/send
// ---------------------------------------------------------------------------

pub async fn send_message(
    State(state): State<Arc<GatewayState>>,
    req: axum::http::Request<axum::body::Body>,
) -> Result<impl IntoResponse, AppError> {
    let identity = extract_identity(req.extensions())?;

    let body = axum::body::to_bytes(req.into_body(), 1024 * 1024).await?;
    let mut message: msg::ClawMessage = serde_json::from_slice(&body)?;

    // Anti-spoof: overwrite msg.from with verified identity.
    message.from = identity.agent_id.clone();

    match &message.to {
        msg::MessageTarget::Broadcast => {
            // Only the orchestrator can broadcast.
            if identity.agent_id != "orchestrator" {
                return Ok((
                    StatusCode::FORBIDDEN,
                    Json(serde_json::json!({ "error": "only orchestrator can broadcast" })),
                )
                    .into_response());
            }
            msg::broadcast(&state.redis, &message).await?;
        }
        msg::MessageTarget::Agent(target_id) => {
            // For P2P messages, enforce ACL.
            if matches!(message.msg_type, msg::MsgType::P2P) {
                let verdict = state
                    .acl_cache
                    .check_p2p(&state.redis, &identity.agent_id, target_id)
                    .await?;
                if let AclVerdict::Deny(reason) = verdict {
                    warn!(
                        sender = %identity.agent_id,
                        target = %target_id,
                        reason = %reason,
                        "P2P denied by ACL"
                    );
                    return Ok((
                        StatusCode::FORBIDDEN,
                        Json(serde_json::json!({ "error": "P2P denied", "reason": reason })),
                    )
                        .into_response());
                }
            }
            msg::send_to_agent(&state.redis, target_id, &message).await?;
        }
        msg::MessageTarget::Orchestrator => {
            msg::send_to_orchestrator(&state.redis, &message.from, &message).await?;
        }
    }

    debug!(msg_id = %message.id, from = %identity.agent_id, "message routed");
    Ok((StatusCode::OK, Json(serde_json::json!({ "sent": true }))).into_response())
}

// ---------------------------------------------------------------------------
// POST /api/v1/register
// ---------------------------------------------------------------------------

pub async fn register(
    State(state): State<Arc<GatewayState>>,
    req: axum::http::Request<axum::body::Body>,
) -> Result<impl IntoResponse, AppError> {
    let identity = extract_identity(req.extensions())?;

    let body = axum::body::to_bytes(req.into_body(), 1024 * 1024).await?;
    let register_req: RegisterRequest = serde_json::from_slice(&body)?;

    // Verify the agent_id in the request matches the authenticated identity.
    if register_req.agent_id != identity.agent_id {
        return Ok((
            StatusCode::FORBIDDEN,
            Json(serde_json::json!({ "error": "agent_id does not match API key identity" })),
        ));
    }

    match register_req.meta {
        Some(mut meta) => {
            // Merge groups from top-level request into meta if provided.
            if let Some(groups) = register_req.groups {
                meta.groups = groups;
            }
            msg::register_agent_with_meta(&state.redis, &register_req.agent_id, &meta).await?;
        }
        None => {
            msg::register_agent(&state.redis, &register_req.agent_id).await?;
            // If groups were provided without meta, set them directly.
            if let Some(groups) = register_req.groups {
                msg::acl::set_agent_groups(&state.redis, &register_req.agent_id, &groups).await?;
            }
        }
    }

    debug!(agent_id = %register_req.agent_id, "agent registered via gateway");
    Ok((
        StatusCode::OK,
        Json(serde_json::json!({ "registered": true })),
    ))
}

// ---------------------------------------------------------------------------
// POST /api/v1/heartbeat
// ---------------------------------------------------------------------------

pub async fn heartbeat(
    State(state): State<Arc<GatewayState>>,
    req: axum::http::Request<axum::body::Body>,
) -> Result<impl IntoResponse, AppError> {
    let identity = extract_identity(req.extensions())?;

    let body = axum::body::to_bytes(req.into_body(), 1024 * 1024).await?;
    let hb_req: HeartbeatRequest = serde_json::from_slice(&body)?;

    if hb_req.agent_id != identity.agent_id {
        return Ok((
            StatusCode::FORBIDDEN,
            Json(serde_json::json!({ "error": "agent_id does not match API key identity" })),
        ));
    }

    msg::refresh_heartbeat(&state.redis, &hb_req.agent_id).await?;

    Ok((
        StatusCode::OK,
        Json(serde_json::json!({ "heartbeat": true })),
    ))
}

// ---------------------------------------------------------------------------
// POST /api/v1/deregister
// ---------------------------------------------------------------------------

pub async fn deregister(
    State(state): State<Arc<GatewayState>>,
    req: axum::http::Request<axum::body::Body>,
) -> Result<impl IntoResponse, AppError> {
    let identity = extract_identity(req.extensions())?;

    let body = axum::body::to_bytes(req.into_body(), 1024 * 1024).await?;
    let dereg_req: DeregisterRequest = serde_json::from_slice(&body)?;

    if dereg_req.agent_id != identity.agent_id {
        return Ok((
            StatusCode::FORBIDDEN,
            Json(serde_json::json!({ "error": "agent_id does not match API key identity" })),
        ));
    }

    msg::deregister_agent(&state.redis, &dereg_req.agent_id).await?;

    debug!(agent_id = %dereg_req.agent_id, "agent deregistered via gateway");
    Ok((
        StatusCode::OK,
        Json(serde_json::json!({ "deregistered": true })),
    ))
}

// ---------------------------------------------------------------------------
// POST /api/v1/poll
// ---------------------------------------------------------------------------

pub async fn poll(
    State(state): State<Arc<GatewayState>>,
    req: axum::http::Request<axum::body::Body>,
) -> Result<impl IntoResponse, AppError> {
    let identity = extract_identity(req.extensions())?;

    let body = axum::body::to_bytes(req.into_body(), 1024 * 1024).await?;
    let poll_req: PollRequest = serde_json::from_slice(&body)?;

    if poll_req.agent_id != identity.agent_id {
        return Ok((
            StatusCode::FORBIDDEN,
            Json(serde_json::json!({ "error": "agent_id does not match API key identity" })),
        )
            .into_response());
    }

    let inbox = msg::agent_inbox(&poll_req.agent_id);

    // Ensure consumer groups exist before reading.
    msg::ensure_consumer_group(&state.redis, &inbox, &poll_req.group, "0").await?;
    msg::ensure_consumer_group(&state.redis, msg::BROADCAST_STREAM, &poll_req.group, "0").await?;

    #[allow(clippy::type_complexity)]
    let result: HashMap<String, Vec<(String, HashMap<String, String>)>> = state
        .redis
        .xreadgroup_map::<String, String, String, String, _, _, _, _>(
            &poll_req.group,
            &poll_req.agent_id,
            Some(10),
            Some(5_000),
            false,
            vec![inbox.as_str(), msg::BROADCAST_STREAM],
            vec![">", ">"],
        )
        .await
        .unwrap_or_default();

    let mut messages: Vec<PollMessage> = Vec::new();

    for (stream, entries) in result {
        for (entry_id, fields) in entries {
            if let Some(raw) = fields.get("msg") {
                messages.push(PollMessage {
                    stream: stream.clone(),
                    entry_id,
                    raw: raw.clone(),
                });
            }
        }
    }

    Ok((StatusCode::OK, Json(messages)).into_response())
}

// ---------------------------------------------------------------------------
// POST /api/v1/ack
// ---------------------------------------------------------------------------

pub async fn ack(
    State(state): State<Arc<GatewayState>>,
    Json(req): Json<AckRequest>,
) -> Result<impl IntoResponse, AppError> {
    state
        .redis
        .xack::<i64, _, _, _>(&req.stream, &req.group, &req.entry_id)
        .await?;

    Ok((StatusCode::OK, Json(serde_json::json!({ "acked": true }))))
}

// ---------------------------------------------------------------------------
// GET /api/v1/roster
// ---------------------------------------------------------------------------

pub async fn roster(
    State(state): State<Arc<GatewayState>>,
) -> Result<impl IntoResponse, AppError> {
    let agents = msg::discover_all_agents(&state.redis).await?;

    let entries: Vec<AgentEntry> = agents
        .into_iter()
        .map(|(agent_id, meta)| AgentEntry { agent_id, meta })
        .collect();

    Ok((StatusCode::OK, Json(entries)))
}

// ---------------------------------------------------------------------------
// POST /api/v1/broadcast-group
// ---------------------------------------------------------------------------

pub async fn broadcast_group(
    State(state): State<Arc<GatewayState>>,
    req: axum::http::Request<axum::body::Body>,
) -> Result<impl IntoResponse, AppError> {
    let identity = extract_identity(req.extensions())?;

    // Only the orchestrator can broadcast to groups.
    if identity.agent_id != "orchestrator" {
        return Ok((
            StatusCode::FORBIDDEN,
            Json(serde_json::json!({ "error": "only orchestrator can broadcast to groups" })),
        )
            .into_response());
    }

    let body = axum::body::to_bytes(req.into_body(), 1024 * 1024).await?;
    let bg_req: BroadcastGroupRequest = serde_json::from_slice(&body)?;

    let mut message = bg_req.message;
    message.from = identity.agent_id;

    let reached = msg::broadcast_to_group(&state.redis, &bg_req.group, &message).await?;

    debug!(group = %bg_req.group, reached = reached.len(), "broadcast to group");
    Ok((
        StatusCode::OK,
        Json(serde_json::json!({ "broadcast": true, "reached": reached })),
    )
        .into_response())
}
