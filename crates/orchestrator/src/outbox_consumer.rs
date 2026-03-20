use fred::prelude::*;
use pheroclaw_messaging as msg;
use std::collections::HashMap;
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::writer::TaskEvent;

const ORCH_GROUP: &str = "cg-orchestrator";
const ORCH_CONSUMER: &str = "orchestrator-0";

/// Consume results from all agent outboxes.
pub async fn outbox_loop(redis: Client, writer: Option<mpsc::Sender<TaskEvent>>) {
    loop {
        if let Err(e) = consume_outboxes(&redis, &writer).await {
            tracing::warn!(error = %e, "outbox consumer error");
        }
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    }
}

/// Returns true for message types worth persisting in the task lifecycle table.
fn should_persist(msg_type: &msg::MsgType) -> bool {
    matches!(
        msg_type,
        msg::MsgType::TaskResult
            | msg::MsgType::ChannelResponse
            | msg::MsgType::P2P
            | msg::MsgType::Skill(_)
    )
}

fn msg_type_label(msg_type: &msg::MsgType) -> String {
    match msg_type {
        msg::MsgType::Task => "task".into(),
        msg::MsgType::TaskResult => "task_result".into(),
        msg::MsgType::P2P => "p2p".into(),
        msg::MsgType::Heartbeat => "heartbeat".into(),
        msg::MsgType::Skill(name) => format!("skill:{name}"),
        msg::MsgType::System(cmd) => format!("system:{cmd}"),
        msg::MsgType::ChannelMessage => "channel_message".into(),
        msg::MsgType::ChannelResponse => "channel_response".into(),
    }
}

fn channel_label(channel: &msg::MessageChannel) -> &'static str {
    match channel {
        msg::MessageChannel::Internal => "internal",
        msg::MessageChannel::Telegram => "telegram",
        msg::MessageChannel::Webchat => "webchat",
        msg::MessageChannel::Whatsapp => "whatsapp",
        msg::MessageChannel::Teams => "teams",
        msg::MessageChannel::A2A => "a2a",
    }
}

async fn consume_outboxes(
    redis: &Client,
    writer: &Option<mpsc::Sender<TaskEvent>>,
) -> anyhow::Result<()> {
    let roster = msg::get_roster(redis).await?;
    if roster.is_empty() {
        return Ok(());
    }

    // Ensure consumer groups exist for each outbox
    let mut keys = Vec::new();
    let mut ids = Vec::new();
    for agent_id in &roster {
        let outbox = msg::agent_outbox(agent_id);
        msg::ensure_consumer_group(redis, &outbox, ORCH_GROUP, "0").await?;
        keys.push(outbox);
        ids.push(">".to_string());
    }

    #[allow(clippy::type_complexity)]
    let result: HashMap<String, Vec<(String, HashMap<String, String>)>> = redis
        .xreadgroup_map(
            ORCH_GROUP,
            ORCH_CONSUMER,
            Some(50),
            Some(2_000),
            false,
            keys.clone(),
            ids,
        )
        .await
        .unwrap_or_default();

    for (stream, entries) in &result {
        for (entry_id, fields) in entries {
            if let Some(raw) = fields.get("msg") {
                match msg::decode_message(raw) {
                    Ok(claw_msg) => {
                        tracing::info!(
                            from = %claw_msg.from,
                            msg_type = ?claw_msg.msg_type,
                            msg_id = %claw_msg.id,
                            "received from agent outbox"
                        );

                        if let Some(tx) = writer.as_ref() {
                            if should_persist(&claw_msg.msg_type) {
                                let event = TaskEvent {
                                    id: Uuid::new_v4(),
                                    correlation_id: claw_msg.correlation_id,
                                    direction: "inbound".into(),
                                    agent_id: claw_msg.from.clone(),
                                    msg_type: msg_type_label(&claw_msg.msg_type),
                                    channel: channel_label(&claw_msg.channel).into(),
                                    session_id: claw_msg.session_id.clone(),
                                    payload: claw_msg.payload.clone(),
                                    status: "recorded".into(),
                                    msg_ts: claw_msg.ts,
                                };
                                if let Err(e) = tx.try_send(event) {
                                    tracing::warn!(error = %e, "task lifecycle writer buffer full, dropping event");
                                }
                            }
                        }
                    }
                    Err(e) => tracing::warn!(error = %e, "failed to decode outbox message"),
                }
            }
            redis
                .xack::<i64, _, _, _>(stream.as_str(), ORCH_GROUP, entry_id.as_str())
                .await?;
        }
    }

    Ok(())
}
