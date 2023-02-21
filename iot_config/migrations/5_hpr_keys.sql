create table hpr_keys (
    pubkey text not null unique,
    
    inserted_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

select trigger_updated_at('hpr_keys');
