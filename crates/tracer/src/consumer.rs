use std::collections::HashMap;

use fred::prelude::*;
use pheroclaw_messaging as msg;
use tokio::sync::mpsc;

use crate::writer::TraceEvent;

const TRACER_GROUP: &str = "cg-tracer";
const TRACER_CONSUMER: &str = "tracer-0";

/// Ensure consumer group exists on all given streams.
pub async fn ensure_tracer_groups(redis: &Client, streams: &[String]) -> anyhow::Result<()> {
    for stream in streams {
        msg::ensure_consumer_group(redis, stream, TRACER_GROUP, "0").await?;
    }
    Ok(())
}

/// Main consumer loop: discovers streams, reads, sends to writer.
pub async fn consume_loop(
    redis: Client,
    tx: mpsc::Sender<TraceEvent>,
    discovery_interval_secs: u64,
) {
    let mut known_streams: Vec<String> = Vec::new();
    let mut last_discovery =
        std::time::Instant::now() - std::time::Duration::from_secs(discovery_interval_secs + 1);

    loop {
        // Re-discover streams periodically
        if last_discovery.elapsed() >= std::time::Duration::from_secs(discovery_interval_secs) {
            match crate::discovery::discover_streams(&redis).await {
                Ok(new_streams) => {
                    let (added, removed) =
                        crate::discovery::diff_streams(&new_streams, &known_streams);
                    if !added.is_empty() {
                        tracing::info!(added = ?added, "discovered new streams");
                        if let Err(e) = ensure_tracer_groups(&redis, &added).await {
                            tracing::warn!(error = %e, "failed to create consumer groups");
                        }
                    }
                    if !removed.is_empty() {
                        tracing::info!(removed = ?removed, "streams removed");
                    }
                    known_streams = new_streams;
                }
                Err(e) => tracing::warn!(error = %e, "stream discovery failed"),
            }
            last_discovery = std::time::Instant::now();
        }

        if known_streams.is_empty() {
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            continue;
        }

        // Read from all known streams
        if let Err(e) = consume_streams(&redis, &known_streams, &tx).await {
            tracing::warn!(error = %e, "consume error");
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }
    }
}

async fn consume_streams(
    redis: &Client,
    streams: &[String],
    tx: &mpsc::Sender<TraceEvent>,
) -> anyhow::Result<()> {
    // Build keys and ids vectors
    let keys: Vec<&str> = streams.iter().map(|s| s.as_str()).collect();
    let ids: Vec<&str> = vec![">"; streams.len()];

    #[allow(clippy::type_complexity)]
    let result: HashMap<String, Vec<(String, HashMap<String, String>)>> = redis
        .xreadgroup_map(
            TRACER_GROUP,
            TRACER_CONSUMER,
            Some(100),
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
                let event = match msg::decode_message(raw) {
                    Ok(claw_msg) => TraceEvent {
                        id: claw_msg.id,
                        stream: stream.clone(),
                        entry_id: entry_id.clone(),
                        msg_from: claw_msg.from.clone(),
                        msg_to: serde_json::to_value(&claw_msg.to)?,
                        msg_type: format!("{:?}", claw_msg.msg_type),
                        payload: claw_msg.payload.clone(),
                        correlation_id: claw_msg.correlation_id,
                        channel: format!("{:?}", claw_msg.channel),
                        session_id: claw_msg.session_id.clone(),
                        ts: claw_msg.ts,
                        retry_count: claw_msg.retry_count as i32,
                    },
                    Err(_) => TraceEvent {
                        id: uuid::Uuid::new_v4(),
                        stream: stream.clone(),
                        entry_id: entry_id.clone(),
                        msg_from: "unknown".into(),
                        msg_to: serde_json::json!("unknown"),
                        msg_type: "unknown".into(),
                        payload: serde_json::json!({"raw": raw}),
                        correlation_id: None,
                        channel: "unknown".into(),
                        session_id: None,
                        ts: chrono::Utc::now().timestamp_millis(),
                        retry_count: 0,
                    },
                };

                // Non-blocking send: if channel is full, message stays in PEL for retry
                if tx.try_send(event).is_err() {
                    tracing::warn!("writer buffer full, leaving message in PEL");
                    continue;
                }
            }

            // ACK the entry
            redis
                .xack::<i64, _, _, _>(stream.as_str(), TRACER_GROUP, entry_id.as_str())
                .await?;
        }
    }

    Ok(())
}
