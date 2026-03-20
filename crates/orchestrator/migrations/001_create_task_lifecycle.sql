CREATE TABLE IF NOT EXISTS task_lifecycle (
    id              UUID PRIMARY KEY,
    correlation_id  UUID,
    direction       TEXT NOT NULL,
    agent_id        TEXT NOT NULL,
    msg_type        TEXT NOT NULL,
    channel         TEXT NOT NULL,
    session_id      TEXT,
    payload         JSONB NOT NULL,
    status          TEXT NOT NULL DEFAULT 'recorded',
    msg_ts          BIGINT NOT NULL,
    recorded_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_task_lifecycle_correlation
    ON task_lifecycle (correlation_id) WHERE correlation_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_task_lifecycle_agent
    ON task_lifecycle (agent_id);
CREATE INDEX IF NOT EXISTS idx_task_lifecycle_direction
    ON task_lifecycle (direction);
CREATE INDEX IF NOT EXISTS idx_task_lifecycle_msg_type
    ON task_lifecycle (msg_type);
CREATE INDEX IF NOT EXISTS idx_task_lifecycle_msg_ts
    ON task_lifecycle (msg_ts);
CREATE INDEX IF NOT EXISTS idx_task_lifecycle_status
    ON task_lifecycle (status);
