create table organization_locks (
    organization varchar(255) primary key not null,
    locked bool default false,

    inserted_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

select trigger_updated_at('organization_locks');
