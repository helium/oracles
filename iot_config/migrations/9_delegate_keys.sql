create table organization_delegate_keys (
    delegate_pubkey text primary key not null,
    oui bigint not null references organizations(oui) on delete cascade,

    inserted_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

select trigger_updated_at('organization_delegate_keys');

insert into organization_delegate_keys 
    select delegate_pubkey, oui from organizations,
    unnest(delegate_keys) as delegate_pubkey;

alter table organizations drop column delegate_keys;
