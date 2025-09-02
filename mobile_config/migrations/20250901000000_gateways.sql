CREATE TYPE device_type AS ENUM (
    'cbrs',
    'wifiIndoor',
    'wifiOutdoor',
    'wifiDataOnly'
);

CREATE TABLE gateways (
    address BYTEA NOT NULL,
    device_type device_type NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    refreshed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    antenna BIGINT,
    elevation BIGINT,
    azimuth BIGINT,
    radio_id TEXT,
    location BIGINT,
    location_changed_at TIMESTAMPTZ,
    location_asserts BIGINT
);