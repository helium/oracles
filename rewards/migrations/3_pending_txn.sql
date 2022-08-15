-- migrations/4_pending_txns.sql

CREATE TYPE status AS ENUM (
    'created',
    'pending',
    'cleared',
    'failed'
);

CREATE TABLE pending_txn (
    hash text PRIMARY KEY NOT NULL,
    status status DEFAULT 'created' NOT NULL,

    submitted_at timestamptz,
    completed_at timestamptz

    created_at timestamptz DEFAULT NOW() NOT NULL,
    updated_at timestamptz DEFAULT NOW() NOT NULL,
);

CREATE INDEX pending_txn_created_idx ON pending_txn (created_at);
select trigger_updated_at('pending_txn');