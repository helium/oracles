create type org_status as enum (
    'enabled',
    'disabled'
);

create table organizations (
    oui bigserial primary key not null,
    owner_pubkey text not null unique,
    payer_pubkey text not null,
    delegate_keys text[],
    status org_status not null default 'enabled',

    inserted_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

select trigger_updated_at('organizations');

create table organization_devaddr_constraints (
    oui bigint not null references organizations(oui) on delete cascade,
    net_id bigint not null,
    start_addr bigint not null,
    end_addr bigint not null,

    inserted_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

select trigger_updated_at('organization_devaddr_constraints');
