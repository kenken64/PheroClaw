use anyhow::{Context, Result};
use reqwest::Client;
use serde::Deserialize;
use tracing::debug;

use pheroclaw_messaging::{AgentMeta, ClawMessage};

/// HTTP client wrapper that talks to the gateway.
/// The agent NEVER connects to Redis directly — all communication goes through this client.
pub struct GatewayClient {
    http: Client,
    gateway_url: String,
    api_key: String,
}

/// A single message returned by the gateway's poll endpoint.
#[derive(Deserialize, Debug)]
pub struct PollMessage {
    pub stream: String,
    pub entry_id: String,
    pub raw: String,
}

impl GatewayClient {
    pub fn new(gateway_url: String, api_key: String) -> Self {
        let http = Client::new();
        Self {
            http,
            gateway_url,
            api_key,
        }
    }

    /// Register this agent with the gateway roster.
    pub async fn register(&self, agent_id: &str, meta: Option<AgentMeta>) -> Result<()> {
        let url = format!("{}/api/v1/register", self.gateway_url);
        let body = serde_json::json!({
            "agent_id": agent_id,
            "meta": meta,
        });

        let resp = self
            .http
            .post(&url)
            .header("X-API-Key", &self.api_key)
            .json(&body)
            .send()
            .await
            .context("register request failed")?;

        let status = resp.status();
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            anyhow::bail!("register failed ({status}): {text}");
        }

        debug!(agent_id, "registered with gateway");
        Ok(())
    }

    /// Send a heartbeat to keep the agent alive in the roster.
    pub async fn heartbeat(&self, agent_id: &str) -> Result<()> {
        let url = format!("{}/api/v1/heartbeat", self.gateway_url);
        let body = serde_json::json!({ "agent_id": agent_id });

        let resp = self
            .http
            .post(&url)
            .header("X-API-Key", &self.api_key)
            .json(&body)
            .send()
            .await
            .context("heartbeat request failed")?;

        let status = resp.status();
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            anyhow::bail!("heartbeat failed ({status}): {text}");
        }

        Ok(())
    }

    /// Remove this agent from the roster (graceful shutdown).
    pub async fn deregister(&self, agent_id: &str) -> Result<()> {
        let url = format!("{}/api/v1/deregister", self.gateway_url);
        let body = serde_json::json!({ "agent_id": agent_id });

        let resp = self
            .http
            .post(&url)
            .header("X-API-Key", &self.api_key)
            .json(&body)
            .send()
            .await
            .context("deregister request failed")?;

        let status = resp.status();
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            anyhow::bail!("deregister failed ({status}): {text}");
        }

        debug!(agent_id, "deregistered from gateway");
        Ok(())
    }

    /// Send a message through the gateway for routing.
    pub async fn send_message(&self, msg: &ClawMessage) -> Result<()> {
        let url = format!("{}/api/v1/send", self.gateway_url);

        let resp = self
            .http
            .post(&url)
            .header("X-API-Key", &self.api_key)
            .json(msg)
            .send()
            .await
            .context("send_message request failed")?;

        let status = resp.status();
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            anyhow::bail!("send_message failed ({status}): {text}");
        }

        debug!(msg_id = %msg.id, "message sent via gateway");
        Ok(())
    }

    /// Poll the agent's inbox and broadcast stream for new messages.
    pub async fn poll_inbox(&self, agent_id: &str, group: &str) -> Result<Vec<PollMessage>> {
        let url = format!("{}/api/v1/poll", self.gateway_url);
        let body = serde_json::json!({
            "agent_id": agent_id,
            "group": group,
        });

        let resp = self
            .http
            .post(&url)
            .header("X-API-Key", &self.api_key)
            .json(&body)
            .send()
            .await
            .context("poll request failed")?;

        let status = resp.status();
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            anyhow::bail!("poll failed ({status}): {text}");
        }

        let messages: Vec<PollMessage> = resp.json().await.context("failed to parse poll response")?;
        Ok(messages)
    }

    /// Acknowledge a message so it won't be redelivered.
    pub async fn ack(&self, stream: &str, group: &str, entry_id: &str) -> Result<()> {
        let url = format!("{}/api/v1/ack", self.gateway_url);
        let body = serde_json::json!({
            "stream": stream,
            "group": group,
            "entry_id": entry_id,
        });

        let resp = self
            .http
            .post(&url)
            .header("X-API-Key", &self.api_key)
            .json(&body)
            .send()
            .await
            .context("ack request failed")?;

        let status = resp.status();
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            anyhow::bail!("ack failed ({status}): {text}");
        }

        Ok(())
    }
}
