create table maker (
    pubkey text primary key not null,
    description text,

    created_at timestamptz default now()
);

