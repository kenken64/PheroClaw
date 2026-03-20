use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Unique identifier for an agent or the orchestrator.
pub type AgentId = String;

/// Every message on the wire uses this envelope.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ClawMessage {
    /// Globally unique message ID.
    pub id: Uuid,
    /// Sender — either "orchestrator", an agent UUID, or "telegram:{chat_id}".
    pub from: AgentId,
    /// Routing target.
    pub to: MessageTarget,
    /// What kind of message this is.
    pub msg_type: MsgType,
    /// Arbitrary JSON payload — keeps the envelope extensible.
    pub payload: serde_json::Value,
    /// For request-reply: the original request's ID.
    pub correlation_id: Option<Uuid>,
    /// Which channel this message entered the system through.
    pub channel: MessageChannel,
    /// External session identifier (Telegram chat_id, webchat session, etc.).
    pub session_id: Option<String>,
    /// Unix milliseconds timestamp.
    pub ts: i64,
    /// Retry counter — incremented on each DLQ re-queue.
    pub retry_count: u32,
}

impl ClawMessage {
    pub fn new(
        from: AgentId,
        to: MessageTarget,
        msg_type: MsgType,
        payload: serde_json::Value,
    ) -> Self {
        Self {
            id: Uuid::new_v4(),
            from,
            to,
            msg_type,
            payload,
            correlation_id: None,
            channel: MessageChannel::Internal,
            session_id: None,
            ts: chrono::Utc::now().timestamp_millis(),
            retry_count: 0,
        }
    }

    /// Create a message originating from a human channel.
    pub fn from_channel(
        from: AgentId,
        to: MessageTarget,
        channel: MessageChannel,
        session_id: String,
        payload: serde_json::Value,
    ) -> Self {
        Self {
            id: Uuid::new_v4(),
            from,
            to,
            msg_type: MsgType::ChannelMessage,
            payload,
            correlation_id: None,
            channel,
            session_id: Some(session_id),
            ts: chrono::Utc::now().timestamp_millis(),
            retry_count: 0,
        }
    }

    /// Create a reply to an existing message, preserving correlation and channel.
    pub fn reply(&self, from: AgentId, msg_type: MsgType, payload: serde_json::Value) -> Self {
        Self {
            id: Uuid::new_v4(),
            from,
            to: MessageTarget::Agent(self.from.clone()),
            msg_type,
            payload,
            correlation_id: self.correlation_id.or(Some(self.id)),
            channel: self.channel.clone(),
            session_id: self.session_id.clone(),
            ts: chrono::Utc::now().timestamp_millis(),
            retry_count: 0,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "snake_case")]
pub enum MessageTarget {
    /// Fan-out to every agent.
    Broadcast,
    /// Deliver to a specific agent's inbox.
    Agent(String),
    /// Return to the orchestrator.
    Orchestrator,
}

/// Which external channel a message entered or will exit through.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum MessageChannel {
    /// Internal Redis-only message (orchestrator commands, inter-agent).
    Internal,
    /// Human talking via Telegram. session_id = chat_id.
    Telegram,
    /// Human talking via OpenClaw webchat. session_id = webchat session token.
    Webchat,
    /// Human talking via WhatsApp. session_id = phone number.
    Whatsapp,
    /// Human talking via Microsoft Teams. session_id = conversation_id.
    Teams,
    /// Agent-to-agent via the A2A protocol (not Redis-native).
    A2A,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "snake_case")]
pub enum MsgType {
    /// Orchestrator assigns a task to an agent.
    Task,
    /// Agent returns a task result.
    TaskResult,
    /// Agent-to-agent direct message.
    P2P,
    /// Liveness signal.
    Heartbeat,
    /// OpenClaw skill invocation — carries the skill name.
    Skill(String),
    /// System commands: reload_skills, shutdown, etc.
    System(String),
    /// Human -> agent conversation message (from any channel).
    ChannelMessage,
    /// Agent's response back to a human channel.
    ChannelResponse,
}
