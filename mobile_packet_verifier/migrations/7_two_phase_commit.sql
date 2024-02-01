CREATE TABLE pending_txns AS (
       signature TEXT PRIMARY KEY,
       payer TEXT NOT NULL,
       amount BIGINT NOT NULL,
       time_of_submission TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE payer_totals (
       payer TEXT PRIMARY KEY,
       total_dcs BIGINT NOT NULL,
       txn solana_transaction
);

