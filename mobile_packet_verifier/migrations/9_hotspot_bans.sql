CREATE TABLE IF NOT EXISTS hotspot_bans (
    hotspot_pubkey TEXT NOT NULL,
    received_timestamp TIMESTAMPTZ NOT NULL,
    expiration_timestamp TIMESTAMPTZ,
    ban_type TEXT NOT NULL,
    PRIMARY KEY (hotspot_pubkey, received_timestamp)
);
