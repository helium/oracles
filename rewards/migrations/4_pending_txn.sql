-- migrations/4_pending_txns.sql
-- :up
CREATE TYPE status AS ENUM (
    'cleared',
    'pending',
    'failed'
);

CREATE TABLE pending_txn (
    created_at timestamptz NOT NULL DEFAULT NOW(),
    updated_at timestamptz NOT NULL DEFAULT NOW(),
    hash text NOT NULL,
    address text NOT NULL,
    status status NOT NULL,
    failed_reason text,
    data bytea NOT NULL,
    PRIMARY KEY (hash)
);

CREATE INDEX pending_txn_created_idx ON pending_txn (created_at);
