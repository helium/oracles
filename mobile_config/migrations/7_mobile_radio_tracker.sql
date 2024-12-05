CREATE TABLE IF NOT EXISTS mobile_radio_tracker (
    entity_key BYTEA NOT NULL,
    hash TEXT NOT NULL,
    last_changed_at TIMESTAMPTZ NOT NULL,
    last_checked_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (entity_key)
);
