-- Migration here solely for testing purposes
-- An instance of account-postgres-sink
-- Will alter this table depending on an on-chain struct
CREATE TABLE IF NOT EXISTS solana_organizations (
    address TEXT PRIMARY KEY,
    net_id TEXT NOT NULL,
    authority TEXT NOT NULL,
    oui BIGINT NOT NULL,
    escrow_key TEXT NOT NULL,
    approved BOOLEAN NOT NULL
);

CREATE OR REPLACE FUNCTION delete_routes_on_solana_organizations_delete() RETURNS trigger AS $$
BEGIN
    DELETE FROM routes WHERE routes.oui = OLD.oui;
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER delete_routes_on_solana_organizations_delete
AFTER DELETE ON solana_organizations
FOR EACH ROW
EXECUTE FUNCTION delete_routes_on_solana_organizations_delete();

CREATE OR REPLACE FUNCTION add_lock_record_on_solana_organizations_insert() RETURNS trigger AS $$
BEGIN
    INSERT INTO organization_locks (organization, locked)
    SELECT sol_org.address, COALESCE(org.locked, TRUE)
    FROM solana_organizations sol_org
    LEFT JOIN organizations org ON sol_org.oui = org.oui
    WHERE sol_org.address = NEW.address;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER add_lock_record_on_solana_organizations_insert
AFTER INSERT ON solana_organizations
FOR EACH ROW
EXECUTE FUNCTION add_lock_record_on_solana_organizations_insert();