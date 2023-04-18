drop table session_key_filters;

create table route_session_key_filters (
    route_id uuid not null references routes(id) on delete cascade,
    devaddr int not null,
    session_key text not null,

    inserted_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),

    primary key (route_id, devaddr, session_key)
);

create index skf_devaddr_idx on route_session_key_filters (devaddr);

select trigger_updated_at('route_session_key_filters');
