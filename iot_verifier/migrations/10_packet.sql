create table packet (
    id serial primary key,
    payload_size bigint not null,
    gateway bytea not null,
    payload_hash bytea not null,
    timestamp timestamptz default now()
);
