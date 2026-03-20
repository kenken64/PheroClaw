// Centralise all Redis key patterns in one place.
// Changing a key format here updates all binaries.

// --- Internal messaging streams ---
pub const BROADCAST_STREAM: &str = "openclaw:orch:broadcast";
pub const ROSTER_KEY: &str = "openclaw:agent:roster";
pub const DLQ_STREAM: &str = "openclaw:dlq";

pub fn agent_inbox(agent_id: &str) -> String {
    format!("openclaw:agent:{agent_id}:inbox")
}

pub fn agent_outbox(agent_id: &str) -> String {
    format!("openclaw:agent:{agent_id}:outbox")
}

pub fn agent_heartbeat(agent_id: &str) -> String {
    format!("openclaw:agent:{agent_id}:heartbeat")
}

pub fn consumer_group(agent_id: &str) -> String {
    format!("cg-{agent_id}")
}

// --- ACL ---
pub const P2P_RULES_KEY: &str = "openclaw:acl:p2p-rules";
pub const ACL_STANCE_KEY: &str = "openclaw:acl:default-stance";

// --- Agent discovery metadata ---

pub fn agent_meta(agent_id: &str) -> String {
    format!("openclaw:agent:{agent_id}:meta")
}

// --- Human channel streams ---

/// Human -> Agent messages arriving via an external channel.
pub fn channel_inbound(agent_id: &str, channel: &str) -> String {
    format!("openclaw:channel:{agent_id}:{channel}:in")
}

/// Agent -> Human responses going out via an external channel.
pub fn channel_outbound(agent_id: &str, channel: &str) -> String {
    format!("openclaw:channel:{agent_id}:{channel}:out")
}

/// List all channel stream keys for a given agent.
pub fn all_channel_streams(agent_id: &str) -> Vec<String> {
    let channels = ["telegram", "webchat", "whatsapp", "teams"];
    channels
        .iter()
        .flat_map(|ch| {
            vec![
                channel_inbound(agent_id, ch),
                channel_outbound(agent_id, ch),
            ]
        })
        .collect()
}
