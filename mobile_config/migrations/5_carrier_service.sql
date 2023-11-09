create table carrier_keys (
    pubkey text not null,
    entity_key text not null,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    PRIMARY KEY(pubkey)
);
