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