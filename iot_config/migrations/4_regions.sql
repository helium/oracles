create table regions (
    region text primary key not null,
    params bytea not null,
    indexes bytea,

    inserted_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

select trigger_updated_at('regions');
