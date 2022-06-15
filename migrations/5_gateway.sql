create table gateway (
    pubkey text primary key not null,
    owner text not null,
    payer text not null,
    height bigint,
    txn_hash text,
    timestamp timestamptz not null
);

create index gateway_height_idx on gateway(height);
create index gateway_timestamp_idx on gateway(timestamp);