use fred::prelude::*;
use pheroclaw_messaging as msg;
use std::collections::HashMap;

/// Watch an agent's inbox + outbox in real-time.
pub async fn watch_agent(redis: &Client, agent_id: &str) -> anyhow::Result<()> {
    let inbox = msg::agent_inbox(agent_id);
    let outbox = msg::agent_outbox(agent_id);
    let group = format!("cg-watch-{}", uuid::Uuid::new_v4());
    let consumer = "watcher";

    // Ensure consumer groups (start from latest)
    msg::ensure_consumer_group(redis, &inbox, &group, "$").await?;
    msg::ensure_consumer_group(redis, &outbox, &group, "$").await?;

    tracing::info!(agent_id, "watching agent streams (Ctrl+C to stop)");

    loop {
        #[allow(clippy::type_complexity)]
        let result: HashMap<String, Vec<(String, HashMap<String, String>)>> = redis
            .xreadgroup_map(
                &group,
                consumer,
                Some(10),
                Some(2_000),
                false,
                vec![inbox.as_str(), outbox.as_str()],
                vec![">", ">"],
            )
            .await
            .unwrap_or_default();

        for (stream, entries) in &result {
            for (entry_id, fields) in entries {
                if let Some(raw) = fields.get("msg") {
                    match msg::decode_message(raw) {
                        Ok(claw_msg) => {
                            let direction =
                                if stream.contains("inbox") { "IN " } else { "OUT" };
                            println!(
                                "[{}] {} | from={} type={:?} id={}",
                                direction, entry_id, claw_msg.from, claw_msg.msg_type, claw_msg.id
                            );
                        }
                        Err(_) => println!("[???] {entry_id} | raw={raw}"),
                    }
                }
                redis
                    .xack::<i64, _, _, _>(stream.as_str(), group.as_str(), entry_id.as_str())
                    .await?;
            }
        }
    }
}
