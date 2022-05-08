

create table uplinks (
    id uuid primary key not null default uuid_generate_v1mc(),
    batch_id BIGINT,
    pubkey TEXT,
    console_device_id TEXT,
    console_device_name TEXT,
    fcnt INTEGER,
    fport INTEGER,

    timestamp timestamptz not null,

    created_at timestamptz default now(),
    updated_at timestamptz
);

SELECT trigger_updated_at('uplinks');
