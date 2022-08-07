-- migrations/4_pending_txns.sql
-- :up
CREATE TYPE status AS ENUM (
    'cleared',
    'pending',
    'failed'
);

CREATE TABLE pending_txn (
    hash text PRIMARY KEY NOT NULL,
    address text NOT NULL,
    status status NOT NULL,
    failed_reason text,
    created_at timestamptz DEFAULT NOW(),
    updated_at timestamptz DEFAULT NOW()
);

CREATE INDEX pending_txn_created_idx ON pending_txn (created_at);
