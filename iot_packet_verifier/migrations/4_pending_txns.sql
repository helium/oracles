CREATE TABLE pending_txns (
       signature TEXT PRIMARY KEY,
       payer TEXT NOT NULL,
       amount BIGINT NOT NULL,
       time_of_submission TIMESTAMPTZ NOT NULL
);
