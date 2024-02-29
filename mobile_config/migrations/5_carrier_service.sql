create table carrier_keys (
    pubkey text primary key not null,
    entity_key text not null,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);
