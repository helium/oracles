CREATE TABLE pending_data_transfer_sessions (
       pub_key TEXT NOT NULL,
       payer TEXT NOT NULL,
       event_id TEXT NOT NULL,
       uploaded_bytes BIGINT NOT NULL,
       downloaded_bytes BIGINT NOT NULL,
       rewardable_bytes BIGINT NOT NULL,
       -- TODO: do we want both timestamps?
       first_timestamp TIMESTAMPTZ NOT NULL,
       last_timestamp TIMESTAMPTZ NOT NULL,
       PRIMARY KEY(pub_key, payer)
);
