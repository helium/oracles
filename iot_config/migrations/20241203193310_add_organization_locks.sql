create table organization_locks (
    organization TEXT PRIMARY KEY NOT NULL,
    locked BOOL DEFAULT false,
    inserted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

select trigger_updated_at('organization_locks');

DO $$
BEGIN
    IF EXISTS (
        SELECT FROM information_schema.tables
        WHERE table_name = 'solana_organizations'
    ) THEN
        INSERT INTO organization_locks (organization, locked)
            SELECT sol_org.address, org.locked
            FROM solana_organizations sol_org
            LEFT JOIN organizations org ON sol_org.oui = org.oui;
    END IF;
END;
$$;
