create type key_type as enum (
    'administrator',
    'packet_router',
    'oracle'
);

create table registered_keys (
    pubkey text not null unique,
    key_type key_type not null,

    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

select trigger_updated_at('registered_keys');
