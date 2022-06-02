create table cell_attach_event (
    id uuid primary key not null default uuid_generate_v1mc(),
    imsi text,
    pubkey text,    
    timestamp timestamptz not null,

    created_at timestamptz default now()
);

