
create table entropy (
    id bytea primary key not null,
    data bytea not null,
    timestamp timestamptz default now() not null,
    version integer not null,
    created_at timestamptz default now()
);

CREATE INDEX idx_entropy_id
ON entropy(id);

CREATE INDEX idx_entropy_timestamp
ON entropy(timestamp);
