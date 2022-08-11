-- migrations/3_pending_txns.sql

create type status AS enum (
    'created',
    'pending',
    'cleared',
    'failed'
);

create table pending_txn (
    hash text primary key not null,
    txn_bin bytea not null,
    status status default 'created' not null,

    submitted_at timestamptz,
    completed_at timestamptz,

    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create index pending_txn_created_idx on pending_txn (created_at);
select trigger_updated_at('pending_txn');
