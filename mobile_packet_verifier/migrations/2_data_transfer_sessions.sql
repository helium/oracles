CREATE TABLE data_transfer_sessions (
       pub_key TEXT NOT NULL,
       payer TEXT NOT NULL,
       uploaded_bytes BIGINT NOT NULL,
       downloaded_bytes BIGINT NOT NULL,
       first_timestamp TIMESTAMPTZ NOT NULL,
       last_timestamp TIMESTAMPTZ NOT NULL,
       PRIMARY KEY(pub_key, payer)
);
