CREATE TYPE onchain_status AS ENUM (
       'queued',
       'pending',
       'success',
       'failed',
       'cancelled'
);

create table activated_hexes (
    location bigint primary key not null,
    activation_ts timestamptz not null,
    boosted_hex_pubkey text not null,
    boost_config_pubkey text not null,
    status onchain_status not null,
    txn_id text,
    retries integer not null default 0,
    inserted_at timestamptz default now(),
    updated_at timestamptz default now()
);

