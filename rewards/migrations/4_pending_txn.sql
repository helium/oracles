-- migrations/4_pending_txns.sql
-- :up
CREATE TYPE status AS ENUM (
    'cleared',
    'created',
    'failed',
    'pending'
);

CREATE TABLE pending_txn (
    hash text PRIMARY KEY NOT NULL,
    status status DEFAULT 'pending' NOT NULL,
    failed_reason text,
    created_at timestamptz DEFAULT NOW() NOT NULL,
    updated_at timestamptz DEFAULT NOW() NOT NULL,
);

CREATE INDEX pending_txn_created_idx ON pending_txn (created_at);
