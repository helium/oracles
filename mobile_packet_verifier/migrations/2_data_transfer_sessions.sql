CREATE TABLE data_transfer_sesions (
       pub_key TEXT NOT NULL,
       payer TEXT NOT NULL,
       uploaded_bytes BIG INTEGER NOT NULL,
       downloaded_bytes BIG INTEGER NOT NULL,
       first_timestamp TIMESTAMPTZ NOT NULL,
       last_timestamp TIMESTAMPTZ NOT NULL,
       PRIMARY KEY(pub_key, payer)
);
