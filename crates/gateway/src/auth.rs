use std::sync::Arc;

use axum::{
    body::Body,
    extract::State,
    http::{Request, StatusCode},
    middleware::Next,
    response::Response,
};

use crate::routes::GatewayState;

/// Identity extracted from the API key mapping.
#[derive(Debug, Clone)]
pub struct AuthenticatedIdentity {
    pub agent_id: String,
}

/// Middleware that validates the `X-API-Key` header against the configured map.
/// On success, inserts `AuthenticatedIdentity` into request extensions.
/// Returns 401 if the header is missing or the key is not recognized.
pub async fn require_api_key(
    State(state): State<Arc<GatewayState>>,
    mut req: Request<Body>,
    next: Next,
) -> Result<Response, StatusCode> {
    let key = req
        .headers()
        .get("X-API-Key")
        .and_then(|v| v.to_str().ok());

    match key {
        Some(k) => match state.api_keys.get(k) {
            Some(agent_id) => {
                req.extensions_mut().insert(AuthenticatedIdentity {
                    agent_id: agent_id.clone(),
                });
                Ok(next.run(req).await)
            }
            None => Err(StatusCode::UNAUTHORIZED),
        },
        None => Err(StatusCode::UNAUTHORIZED),
    }
}
