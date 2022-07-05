create table gateway (
    pubkey text primary key not null,
    owner text,
    payer text,
    height bigint,
    txn_hash text,
    block_timestamp timestamptz,

    last_heartbeat timestamptz,
    last_speedtest timestamptz,
    last_attach timestamptz
);

create index gateway_height_idx on gateway(height);
create index gateway_block_timestamp_idx on gateway(block_timestamp);