CREATE TABLE IF NOT EXISTS gateways (
    address BYTEA PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL,
    elevation INTEGER,
    gain INTEGER,
    hash TEXT,
    is_active BOOLEAN,
    is_full_hotspot BOOLEAN,
    last_changed_at TIMESTAMPTZ NOT NULL,
    location BIGINT,
    location_asserts INTEGER,
    location_changed_at TIMESTAMPTZ,
    refreshed_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS gateways_last_changed_idx ON gateways (last_changed_at DESC);

CREATE INDEX IF NOT EXISTS gateways_location_changed_idx ON gateways (location_changed_at DESC)
WHERE
    location IS NOT NULL;