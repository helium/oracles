create type org_status as enum (
    'enabled',
    'disabled'
);

create table organizations (
    oui bigserial primary key not null,
    owner_pubkey text not null,
    payer_pubkey text not null,
    delegate_keys text[],
    status org_status not null default 'enabled',

    inserted_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

select trigger_updated_at('organizations');

create table organization_devaddr_constraints (
    oui bigint not null references organizations(oui) on delete cascade,
    nwk_id int not null,
    start_nwk_addr int not null,
    end_nwk_addr int not null,

    inserted_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

select trigger_updated_at('organization_devaddr_constraints');
