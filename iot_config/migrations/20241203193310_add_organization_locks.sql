create table organization_locks (
    organization varchar(255) primary key not null,
    locked bool default false,

    inserted_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

select trigger_updated_at('organization_locks');

DO $$
BEGIN
    IF EXISTS (
        SELECT FROM information_schema.tables
        WHERE table_name = 'solana_organizations'
    ) THEN
        insert into organization_locks (organization, locked)
            select sol_org.address, org.locked
            from solana_organizations sol_org
            left join organizations org on sol_org.oui = org.oui;
    END IF;
END;
$$;
