use fred::prelude::Client;
use pheroclaw_messaging as msg;
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::writer::TaskEvent;

/// Record an outbound message in the task lifecycle table.
#[allow(dead_code)]
pub fn record_outbound(
    writer: &Option<mpsc::Sender<TaskEvent>>,
    claw_msg: &msg::ClawMessage,
    target_agent: &str,
) {
    let Some(tx) = writer.as_ref() else { return };

    let msg_type = match &claw_msg.msg_type {
        msg::MsgType::Task => "task".into(),
        msg::MsgType::Skill(name) => format!("skill:{name}"),
        other => format!("{other:?}"),
    };

    let channel = match &claw_msg.channel {
        msg::MessageChannel::Internal => "internal",
        msg::MessageChannel::Telegram => "telegram",
        msg::MessageChannel::Webchat => "webchat",
        msg::MessageChannel::Whatsapp => "whatsapp",
        msg::MessageChannel::Teams => "teams",
        msg::MessageChannel::A2A => "a2a",
    };

    let event = TaskEvent {
        id: Uuid::new_v4(),
        correlation_id: claw_msg.correlation_id.or(Some(claw_msg.id)),
        direction: "outbound".into(),
        agent_id: target_agent.into(),
        msg_type,
        channel: channel.into(),
        session_id: claw_msg.session_id.clone(),
        payload: claw_msg.payload.clone(),
        status: "recorded".into(),
        msg_ts: claw_msg.ts,
    };

    if let Err(e) = tx.try_send(event) {
        tracing::warn!(error = %e, "task lifecycle writer buffer full, dropping outbound event");
    }
}

/// Assign a task to a specific agent.
#[allow(dead_code)]
pub async fn assign_task(
    redis: &Client,
    target_agent: &str,
    payload: serde_json::Value,
    writer: &Option<mpsc::Sender<TaskEvent>>,
) -> anyhow::Result<()> {
    let task = msg::ClawMessage::new(
        "orchestrator".into(),
        msg::MessageTarget::Agent(target_agent.into()),
        msg::MsgType::Task,
        payload,
    );
    record_outbound(writer, &task, target_agent);
    msg::send_to_agent(redis, target_agent, &task).await
}

/// Broadcast a system command to all agents.
#[allow(dead_code)]
pub async fn broadcast_system(
    redis: &Client,
    command: &str,
    payload: serde_json::Value,
) -> anyhow::Result<()> {
    let msg = msg::ClawMessage::new(
        "orchestrator".into(),
        msg::MessageTarget::Broadcast,
        msg::MsgType::System(command.into()),
        payload,
    );
    msg::broadcast(redis, &msg).await
}

/// Broadcast a system command to a specific group of agents.
#[allow(dead_code)]
pub async fn broadcast_to_group(
    redis: &Client,
    group: &str,
    command: &str,
    payload: serde_json::Value,
) -> anyhow::Result<Vec<String>> {
    let message = msg::ClawMessage::new(
        "orchestrator".into(),
        msg::MessageTarget::Broadcast,
        msg::MsgType::System(command.into()),
        payload,
    );
    msg::broadcast_to_group(redis, group, &message).await
}

/// Invoke a skill on a specific agent.
#[allow(dead_code)]
pub async fn invoke_skill(
    redis: &Client,
    target_agent: &str,
    skill_name: &str,
    payload: serde_json::Value,
    writer: &Option<mpsc::Sender<TaskEvent>>,
) -> anyhow::Result<()> {
    let msg = msg::ClawMessage::new(
        "orchestrator".into(),
        msg::MessageTarget::Agent(target_agent.into()),
        msg::MsgType::Skill(skill_name.into()),
        payload,
    );
    record_outbound(writer, &msg, target_agent);
    msg::send_to_agent(redis, target_agent, &msg).await
}
