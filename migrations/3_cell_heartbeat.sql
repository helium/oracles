create table cell_heartbeat (
    id uuid primary key not null default uuid_generate_v1mc(),

    pubkey text,    
    hotspot_type text,
    cell_id integer,
    timestamp timestamptz not null,
    lon float,
    lat float,
    operation_mode boolean,
    cbsd_category text,

    created_at timestamptz default now()
);

create index cell_heartbeat_pubkey_idx on cell_heartbeat(pubkey);
create index cell_heartbeat_timestamp_idx on cell_heartbeat(timestamp);