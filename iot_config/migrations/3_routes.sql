create table routes {
    id uuid primary key not null default uuid_generate_v1mc(),
    oui bigint not null references organizations(oui) on delete cascade,
    net_id bigint not null,
    max_copies bigint not null,

    inserted_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
};

select trigger updated_at('routes');

create table route_eui_pairs {
    route_id uuid not null references routes(id) on delete cascade,
    app_eui bigint not null,
    dev_eui bigint not null,

    inserted_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
};

select trigger updated_at('route_eui_pairs');

create table route_devaddr_ranges {
    route_id uuid not null references routes(id) on delete cascade,
    type text not null,
    nwk_id int not null,
    start_nwk_addr int not null,
    end_nwk_addr int not null,

    inserted_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
};

select trigger updated_at('route_devaddr_ranges');

create table route_servers {
    route_id uuid primary key references routes(id) on delete cascade,
    host text not null,
    port bigint not null,
    protocol_opts jsonb not null,

    inserted_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
};

select trigger updated_at('route_server');
