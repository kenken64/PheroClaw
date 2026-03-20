use sqlx::PgPool;
use tokio::sync::mpsc;
use uuid::Uuid;

/// A task lifecycle event ready for database insertion.
#[derive(Debug)]
pub struct TaskEvent {
    pub id: Uuid,
    pub correlation_id: Option<Uuid>,
    pub direction: String,
    pub agent_id: String,
    pub msg_type: String,
    pub channel: String,
    pub session_id: Option<String>,
    pub payload: serde_json::Value,
    pub status: String,
    pub msg_ts: i64,
}

/// Spawn the writer task, returns the sender half.
pub fn spawn_writer(pool: PgPool, buffer_size: usize) -> mpsc::Sender<TaskEvent> {
    let (tx, rx) = mpsc::channel(buffer_size);
    tokio::spawn(writer_loop(pool, rx));
    tx
}

async fn writer_loop(pool: PgPool, mut rx: mpsc::Receiver<TaskEvent>) {
    let mut batch: Vec<TaskEvent> = Vec::with_capacity(500);
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
                        if !batch.is_empty() {
                            flush_batch(&pool, &mut batch).await;
                        }
                        tracing::info!("task lifecycle writer channel closed, exiting");
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

async fn flush_batch(pool: &PgPool, batch: &mut Vec<TaskEvent>) {
    if batch.is_empty() {
        return;
    }

    let count = batch.len();

    let ids: Vec<Uuid> = batch.iter().map(|e| e.id).collect();
    let corr_ids: Vec<Option<Uuid>> = batch.iter().map(|e| e.correlation_id).collect();
    let directions: Vec<&str> = batch.iter().map(|e| e.direction.as_str()).collect();
    let agent_ids: Vec<&str> = batch.iter().map(|e| e.agent_id.as_str()).collect();
    let msg_types: Vec<&str> = batch.iter().map(|e| e.msg_type.as_str()).collect();
    let channels: Vec<&str> = batch.iter().map(|e| e.channel.as_str()).collect();
    let session_ids: Vec<Option<&str>> = batch.iter().map(|e| e.session_id.as_deref()).collect();
    let payloads: Vec<serde_json::Value> = batch.iter().map(|e| e.payload.clone()).collect();
    let statuses: Vec<&str> = batch.iter().map(|e| e.status.as_str()).collect();
    let timestamps: Vec<i64> = batch.iter().map(|e| e.msg_ts).collect();

    let result = sqlx::query(
        r#"
        INSERT INTO task_lifecycle (id, correlation_id, direction, agent_id, msg_type, channel, session_id, payload, status, msg_ts)
        SELECT * FROM UNNEST(
            $1::uuid[], $2::uuid[], $3::text[], $4::text[], $5::text[],
            $6::text[], $7::text[], $8::jsonb[], $9::text[], $10::bigint[]
        )
        ON CONFLICT (id) DO NOTHING
        "#,
    )
    .bind(&ids)
    .bind(&corr_ids)
    .bind(&directions)
    .bind(&agent_ids)
    .bind(&msg_types)
    .bind(&channels)
    .bind(&session_ids)
    .bind(&payloads)
    .bind(&statuses)
    .bind(&timestamps)
    .execute(pool)
    .await;

    match result {
        Ok(_) => tracing::debug!(count, "flushed task lifecycle batch"),
        Err(e) => tracing::error!(error = %e, count, "failed to flush task lifecycle batch"),
    }

    batch.clear();
}
