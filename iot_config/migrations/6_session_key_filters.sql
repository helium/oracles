create table session_key_filters (
    oui bigint not null references organizations(oui) on delete cascade,
    devaddr int not null,
    session_key text not null,

    inserted_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),

    primary key (oui, devaddr, session_key)
);

create index skf_devaddr_idx on session_key_filters (devaddr);

select trigger_updated_at('session_key_filters');
