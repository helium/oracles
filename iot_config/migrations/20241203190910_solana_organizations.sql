-- Migration here solely for testing purposes
-- An instance of account-postgres-sink
-- Will alter this table depending on on-chain struct
CREATE TABLE IF NOT EXISTS solana_organizations (
    address TEXT PRIMARY KEY,
    net_id TEXT NOT NULL,
    authority TEXT NOT NULL,
    oui BIGINT NOT NULL,
    escrow_key TEXT NOT NULL,
    approved BOOLEAN NOT NULL
);
