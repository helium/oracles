CREATE TYPE gateway_type AS ENUM (
    'wifiIndoor',
    'wifiOutdoor',
    'wifiDataOnly'
);

CREATE TABLE IF NOT EXISTS gateways (
    address BYTEA PRIMARY KEY,
    gateway_type gateway_type NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    refreshed_at TIMESTAMPTZ NOT NULL,
    last_changed_at TIMESTAMPTZ NOT NULL,
    hash TEXT,
    antenna BIGINT,
    elevation BIGINT,
    azimuth BIGINT,
    location BIGINT,
    location_changed_at TIMESTAMPTZ,
    location_asserts BIGINT
);