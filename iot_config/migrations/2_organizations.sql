create table organizations (
    oui bigserial primary key not null,
    owner_pubkey text not null unique,
    payer_pubkey text not null,
    delegate_keys text[],
    locked bool default false,

    inserted_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

select trigger_updated_at('organizations');

create table organization_devaddr_constraints (
    oui bigint not null references organizations(oui) on delete cascade,
    net_id int not null,
    start_addr int not null,
    end_addr int not null,

    inserted_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

select trigger_updated_at('organization_devaddr_constraints');
