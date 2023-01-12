create table routes (
    id uuid primary key not null default uuid_generate_v1mc(),
    oui bigint not null references organizations(oui) on delete cascade,
    net_id bigint not null,
    max_copies int not null,
    server_host text not null,
    server_port int not null,
    server_protocol_opts jsonb not null,

    inserted_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

select trigger_updated_at('routes');

create index route_oui_idx on routes (oui);

create table route_eui_pairs (
    route_id uuid not null references routes(id) on delete cascade,
    app_eui bigint not null,
    dev_eui bigint not null,

    inserted_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

select trigger_updated_at('route_eui_pairs');

create index eui_pair_route_idx on route_eui_pairs (route_id);

create table route_devaddr_ranges (
    route_id uuid not null references routes(id) on delete cascade,
    start_addr bigint not null,
    end_addr bigint not null,

    inserted_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

select trigger_updated_at('route_devaddr_ranges');

create index devaddr_range_route_idx on route_devaddr_ranges (route_id);
