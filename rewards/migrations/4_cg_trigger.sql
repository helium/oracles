create table cg_trigger (
    block_height bigint primary key not null,
    block_timestamp bigint not null,

    created_at timestamptz default now()
);
