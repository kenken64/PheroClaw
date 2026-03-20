use fred::prelude::*;
use fred::types::Value;
use pheroclaw_messaging as msg;

/// Run DLQ sweep loop every 60 seconds.
pub async fn dlq_loop(redis: Client, max_retries: u32, pending_timeout_secs: u64) {
    let min_idle_ms = pending_timeout_secs * 1000;
    loop {
        if let Err(e) = sweep_pending(&redis, max_retries, min_idle_ms).await {
            tracing::warn!(error = %e, "DLQ sweep error");
        }
        tokio::time::sleep(std::time::Duration::from_secs(60)).await;
    }
}

async fn sweep_pending(redis: &Client, max_retries: u32, min_idle_ms: u64) -> anyhow::Result<()> {
    let roster = msg::get_roster(redis).await?;

    for agent_id in &roster {
        let inbox = msg::agent_inbox(agent_id);
        let group = msg::consumer_group(agent_id);

        // Check if stream exists before scanning PEL
        let exists: bool = redis.exists(&inbox).await?;
        if !exists {
            continue;
        }

        // Get pending entries
        let pending: Value = redis
            .xpending(&inbox, &group, ("-", "+", 50u64))
            .await
            .unwrap_or(Value::Null);

        // Parse pending entries - each is [entry_id, consumer, idle_ms, delivery_count]
        if let Value::Array(entries) = pending {
            for entry in entries {
                if let Value::Array(ref fields) = entry {
                    if fields.len() >= 4 {
                        let entry_id = field_to_string(&fields[0]);
                        let idle: u64 = field_to_u64(&fields[2]);
                        let deliveries: u32 = field_to_u64(&fields[3]) as u32;

                        if idle < min_idle_ms {
                            continue;
                        }

                        if deliveries > max_retries {
                            // Move to DLQ
                            move_to_dlq(redis, &inbox, &group, &entry_id, agent_id).await?;
                        } else {
                            // Re-claim for reprocessing
                            let _: Value = redis
                                .xclaim(
                                    &inbox, &group, agent_id, min_idle_ms, &entry_id, None, None,
                                    None, false, false,
                                )
                                .await?;
                            tracing::debug!(
                                stream = %inbox,
                                entry_id,
                                deliveries,
                                "reclaimed pending entry"
                            );
                        }
                    }
                }
            }
        }
    }

    Ok(())
}

async fn move_to_dlq(
    redis: &Client,
    stream: &str,
    group: &str,
    entry_id: &str,
    agent_id: &str,
) -> anyhow::Result<()> {
    // Read the original message
    let original: Value = redis.xrange(stream, entry_id, entry_id, Some(1)).await?;

    // Write to DLQ stream with metadata
    let dlq_payload = serde_json::json!({
        "original_stream": stream,
        "original_entry_id": entry_id,
        "agent_id": agent_id,
        "original": format!("{:?}", original),
        "moved_at": chrono::Utc::now().to_rfc3339(),
    });

    redis
        .xadd::<String, _, _, _, _>(
            msg::DLQ_STREAM,
            false,
            ("MAXLEN", "~", msg::MAX_STREAM_LEN),
            "*",
            vec![("msg", dlq_payload.to_string().as_str())],
        )
        .await?;

    // ACK the original to remove from PEL
    redis
        .xack::<i64, _, _, _>(stream, group, entry_id)
        .await?;
    tracing::warn!(stream, entry_id, agent_id, "moved to DLQ");

    Ok(())
}

fn field_to_string(v: &Value) -> String {
    match v {
        Value::String(s) => s.to_string(),
        Value::Bytes(b) => String::from_utf8_lossy(b).into_owned(),
        _ => format!("{v:?}"),
    }
}

fn field_to_u64(v: &Value) -> u64 {
    match v {
        Value::String(s) => s.parse().unwrap_or(0),
        Value::Integer(i) => *i as u64,
        _ => 0,
    }
}
