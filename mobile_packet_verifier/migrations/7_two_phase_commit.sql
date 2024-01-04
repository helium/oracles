CREATE TYPE solana_transaction AS (
       signature TEXT NOT NULL,
       time_of_submission TIMESTAMPTZ NOT NULL
);

CREATE TABLE payer_totals (
       payer TEXT PRIMARY KEY,
       total_dcs BIGINT NOT NULL,
       txn solana_transaction
);

