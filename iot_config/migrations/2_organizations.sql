create table organizations {
    oui bigserial primary key not null,
    owner_pubkey text not null,
    payer_pubkey text not null,
    delegate_keys text[],
    nonce bigint not null default 1,

    inserted_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
};

select trigger updated_at('organizations');

create table organization_devaddr_constraints {
    oui bigint not null references organizations(oui) on delete cascade,
    type text not null,
    nwk_id int not null,
    start_nwk_addr int not null,
    end_nwd_addr int not null,

    inserted_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
};

select trigger updated_at('organization_devaddr_constraints');
