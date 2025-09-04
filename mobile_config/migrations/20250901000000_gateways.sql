CREATE TYPE gateway_type AS ENUM (
    'wifiIndoor',
    'wifiOutdoor',
    'wifiDataOnly'
);

CREATE TABLE gateways (
    address BYTEA NOT NULL,
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