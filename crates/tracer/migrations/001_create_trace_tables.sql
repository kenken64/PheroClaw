CREATE TABLE IF NOT EXISTS message_trace (
    id          UUID PRIMARY KEY,
    stream      TEXT NOT NULL,
    entry_id    TEXT NOT NULL,
    msg_from    TEXT NOT NULL,
    msg_to      JSONB NOT NULL,
    msg_type    TEXT NOT NULL,
    payload     JSONB NOT NULL,
    correlation_id UUID,
    channel     TEXT NOT NULL,
    session_id  TEXT,
    ts          BIGINT NOT NULL,
    retry_count INTEGER NOT NULL DEFAULT 0,
    traced_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_message_trace_ts ON message_trace (ts);
CREATE INDEX IF NOT EXISTS idx_message_trace_from ON message_trace (msg_from);
CREATE INDEX IF NOT EXISTS idx_message_trace_correlation ON message_trace (correlation_id) WHERE correlation_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_message_trace_stream ON message_trace (stream);

CREATE TABLE IF NOT EXISTS agent_events (
    id          BIGSERIAL PRIMARY KEY,
    agent_id    TEXT NOT NULL,
    event_type  TEXT NOT NULL,
    details     JSONB,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_agent_events_agent ON agent_events (agent_id);
CREATE INDEX IF NOT EXISTS idx_agent_events_type ON agent_events (event_type);

CREATE TABLE IF NOT EXISTS dlq_audit (
    id              BIGSERIAL PRIMARY KEY,
    original_stream TEXT NOT NULL,
    entry_id        TEXT NOT NULL,
    agent_id        TEXT,
    raw_message     JSONB,
    moved_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_dlq_audit_stream ON dlq_audit (original_stream);
