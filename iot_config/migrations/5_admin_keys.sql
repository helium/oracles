create type key_type as enum (
    'administrator',
    'packet_router'
);

create table admin_keys (
    pubkey text not null unique,
    key_type key_type not null,
    
    inserted_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

select trigger_updated_at('admin_keys');
