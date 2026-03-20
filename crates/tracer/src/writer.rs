use sqlx::PgPool;
use tokio::sync::mpsc;
use uuid::Uuid;

/// A trace event ready for database insertion.
#[derive(Debug)]
pub struct TraceEvent {
    pub id: Uuid,
    pub stream: String,
    pub entry_id: String,
    pub msg_from: String,
    pub msg_to: serde_json::Value,
    pub msg_type: String,
    pub payload: serde_json::Value,
    pub correlation_id: Option<Uuid>,
    pub channel: String,
    pub session_id: Option<String>,
    pub ts: i64,
    pub retry_count: i32,
}

/// Spawn the writer task, returns the sender half.
pub fn spawn_writer(pool: PgPool, buffer_size: usize) -> mpsc::Sender<TraceEvent> {
    let (tx, rx) = mpsc::channel(buffer_size);
    tokio::spawn(writer_loop(pool, rx));
    tx
}

async fn writer_loop(pool: PgPool, mut rx: mpsc::Receiver<TraceEvent>) {
    let mut batch: Vec<TraceEvent> = Vec::with_capacity(500);
    let flush_interval = tokio::time::interval(std::time::Duration::from_secs(2));
    tokio::pin!(flush_interval);

    loop {
        tokio::select! {
            maybe_event = rx.recv() => {
                match maybe_event {
                    Some(event) => {
                        batch.push(event);
                        if batch.len() >= 500 {
                            flush_batch(&pool, &mut batch).await;
                        }
                    }
                    None => {
                        // Channel closed, flush remaining and exit
                        if !batch.is_empty() {
                            flush_batch(&pool, &mut batch).await;
                        }
                        tracing::info!("writer channel closed, exiting");
                        return;
                    }
                }
            }
            _ = flush_interval.tick() => {
                if !batch.is_empty() {
                    flush_batch(&pool, &mut batch).await;
                }
            }
        }
    }
}

async fn flush_batch(pool: &PgPool, batch: &mut Vec<TraceEvent>) {
    if batch.is_empty() {
        return;
    }

    let count = batch.len();

    // Use UNNEST for bulk insert
    let ids: Vec<Uuid> = batch.iter().map(|e| e.id).collect();
    let streams: Vec<&str> = batch.iter().map(|e| e.stream.as_str()).collect();
    let entry_ids: Vec<&str> = batch.iter().map(|e| e.entry_id.as_str()).collect();
    let froms: Vec<&str> = batch.iter().map(|e| e.msg_from.as_str()).collect();
    let tos: Vec<serde_json::Value> = batch.iter().map(|e| e.msg_to.clone()).collect();
    let types: Vec<&str> = batch.iter().map(|e| e.msg_type.as_str()).collect();
    let payloads: Vec<serde_json::Value> = batch.iter().map(|e| e.payload.clone()).collect();
    let corr_ids: Vec<Option<Uuid>> = batch.iter().map(|e| e.correlation_id).collect();
    let channels: Vec<&str> = batch.iter().map(|e| e.channel.as_str()).collect();
    let session_ids: Vec<Option<&str>> = batch.iter().map(|e| e.session_id.as_deref()).collect();
    let timestamps: Vec<i64> = batch.iter().map(|e| e.ts).collect();
    let retries: Vec<i32> = batch.iter().map(|e| e.retry_count).collect();

    let result = sqlx::query(
        r#"
        INSERT INTO message_trace (id, stream, entry_id, msg_from, msg_to, msg_type, payload, correlation_id, channel, session_id, ts, retry_count)
        SELECT * FROM UNNEST(
            $1::uuid[], $2::text[], $3::text[], $4::text[], $5::jsonb[], $6::text[],
            $7::jsonb[], $8::uuid[], $9::text[], $10::text[], $11::bigint[], $12::integer[]
        )
        ON CONFLICT (id) DO NOTHING
        "#,
    )
    .bind(&ids)
    .bind(&streams)
    .bind(&entry_ids)
    .bind(&froms)
    .bind(&tos)
    .bind(&types)
    .bind(&payloads)
    .bind(&corr_ids)
    .bind(&channels)
    .bind(&session_ids)
    .bind(&timestamps)
    .bind(&retries)
    .execute(pool)
    .await;

    match result {
        Ok(_) => tracing::debug!(count, "flushed trace batch"),
        Err(e) => tracing::error!(error = %e, count, "failed to flush trace batch"),
    }

    batch.clear();
}
