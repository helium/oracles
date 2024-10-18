CREATE TABLE IF NOT EXISTS radio_location_estimates (
    hashed_key TEXT NOT NULL,
    radio_type radio_type NOT NULL,
    radio_key TEXT NOT NULL,
    received_timestamp TIMESTAMPTZ NOT NULL,
    hex BIGINT NOT NULL,
    grid_distance BIGINT NOT NULL,
    confidence DECIMAL NOT NULL,
    invalidated_at TIMESTAMPTZ DEFAULT NULL,
    inserted_at TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (hashed_key)
);

CREATE INDEX IF NOT EXISTS idx_radio_key_confidence_invalided_at_radius ON radio_location_estimates (radio_key, confidence, invalidated_at, grid_distance DESC);
