-- Migration here solely for testing purposes
-- An instance of account-postgres-sink
-- Will alter this table depending on on-chain struct
CREATE TABLE IF NOT EXISTS solana_organization_devaddr_constraints (
    address TEXT PRIMARY KEY,
    net_id TEXT NOT NULL,
    organization TEXT NOT NULL,
    start_addr NUMERIC NOT NULL,
    end_addr NUMERIC NOT NULL
)