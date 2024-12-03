-- Migration here solely for testing purposes
-- An instance of account-postgres-sink
-- Will alter this table depending on on-chain struct
CREATE TABLE IF NOT EXISTS solana_net_ids (
    address TEXT PRIMARY KEY,
    id INTEGER NOT NULL,
    authority TEXT NOT NULL,
    current_addr_offset NUMERIC NOT NULL
);