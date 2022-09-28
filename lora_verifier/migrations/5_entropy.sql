
create table entropy (
    id bytea primary key not null,
    data bytea not null,
    timestamp timestamptz default now() not null,
    created_at timestamptz default now()
);
