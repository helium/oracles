create table gateway (
    address text primary key not null,
    owner text not null,
    location text,

    last_heartbeat timestamptz,
    last_speedtest timestamptz,
    last_attach timestamptz,

    created_at timestamptz default now()
);
