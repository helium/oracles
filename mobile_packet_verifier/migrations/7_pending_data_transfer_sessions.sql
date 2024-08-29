CREATE TABLE pending_data_transfer_sessions (
       pub_key              TEXT NOT NULL,
       payer                TEXT NOT NULL,
       event_id             TEXT NOT NULL,
       uploaded_bytes       BIGINT NOT NULL,
       downloaded_bytes     BIGINT NOT NULL,
       rewardable_bytes     BIGINT NOT NULL,
       recv_timestamp       TIMESTAMPTZ NOT NULL,
       inserted_at          TIMESTAMPTZ NOT NULL,
       PRIMARY KEY(pub_key, payer, recv_timestamp)
);
