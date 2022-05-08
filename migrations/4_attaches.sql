create table attaches (
    id uuid primary key not null default uuid_generate_v1mc(),
    pubkey text,
    timestamp timestamptz not null,

    created_at timestamptz default now(),
    updated_at timestamptz
);

SELECT trigger_updated_at('attaches');