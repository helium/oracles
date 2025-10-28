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