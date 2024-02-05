CREATE TABLE pending_txns AS (
       signature TEXT PRIMARY KEY,
       pub_key TEXT NOT NULL,
       payer TEXT NOT NULL,
       num_dcs BIGINT NOT NULL,
       uploaded_bytes BIGINT NOT NULL,
       downloaded_bytes BIGINT NOT NULL,
       rewardable_bytes BIGINT NOT NULL,
       first_timestamp TIMESTAMPTZ NOT NULL,
       last_timestmap TIMESTAMPTZ NOT NULL
);

CREATE TABLE data_transfer_sessions_by_row (
       pub_key TEXT NOT NULL,
       payer TEXT NOT NULL,
       uploaded_bytes BIGINT NOT NULL,
       downloaded_bytes BIGINT NOT NULL,
       rewardable_bytes BIGINT NOT NULL,
       session_timestamp TIMESTAMPTZ NOT NULL,
       PRIMARY KEY(pub_key, payer, session_timestamp)
);

INSERT INTO data_transfer_sessions_by_row
       (pub_key, payer, uploaded_bytes, downloaded_bytes, rewardable_bytes, session_timestamp)
SELECT pub_key, payer, uploaded_bytes, downloaded_bytes, rewardable_bytes, last_timestamp as session_timestamp FROM data_transfer_sessions;

ALTER TABLE data_transfer_sessions RENAME TO old_data_transfer_sessions;
ALTER TABLE data_transfer_sessions_by_row to data_transfer_sessions;
DROP TABLE data_transfer_sessions;
