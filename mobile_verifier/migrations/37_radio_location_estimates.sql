CREATE TABLE IF NOT EXISTS radio_location_estimates (
    hashed_key TEXT NOT NULL,
    radio_id TEXT NOT NULL,
    received_timestamp TIMESTAMPTZ NOT NULL,
    radius DECIMAL NOT NULL,
    lat DECIMAL NOT NULL,
    long DECIMAL NOT NULL,
    confidence DECIMAL NOT NULL,
    is_valid BOOLEAN NOT NULL,
    inserted_at TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (hashed_key)
);