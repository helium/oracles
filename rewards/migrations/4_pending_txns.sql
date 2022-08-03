-- migrations/4_pending_txns.sql
-- :up

CREATE TYPE status as ENUM (
       'cleared',
       'pending',
       'failed'
);

CREATE TABLE pending_txns (
       created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
       updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
       hash TEXT NOT NULL,
       address TEXT NOT NULL,
       status status NOT NULL,
       failed_reason TEXT,
       data BYTEA NOT NULL,

       PRIMARY KEY (hash)
);

CREATE INDEX pending_txn_created_idx ON pending_txns(created_at);
