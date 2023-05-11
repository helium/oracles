create table helium_used_devaddrs (
    devaddr int primary key not null,
    inserted_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

select trigger_updated_at('helium_used_devaddrs');
