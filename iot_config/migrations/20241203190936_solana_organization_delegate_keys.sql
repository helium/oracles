-- Migration here solely for testing purposes
-- An instance of account-postgres-sink
-- Will alter this table depending on an on-chain struct
CREATE TABLE IF NOT EXISTS solana_organization_delegate_keys (
    address TEXT PRIMARY KEY,
    organization TEXT NOT NULL,
    delegate TEXT NOT NULL
);