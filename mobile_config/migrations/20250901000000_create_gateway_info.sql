CREATE TYPE device_type AS ENUM (
    'cbrs',
    'wifiIndoor',
    'wifiOutdoor',
    'wifiDataOnly'
);

CREATE TYPE deployment_info_v1 AS (
    antena BIGINT,
    elevation BIGINT,
    azimuth BIGINT,
    radio_id TEXT
);

CREATE TYPE location_info_v1 AS (
    location BIGINT,
    location_changed_at TIMESTAMPTZ,
    location_asserts BIGINT
);

CREATE TABLE gateway_info (
    address BYTEA NOT NULL,
    device_type device_type NOT NULL,
    location_info location_info_v1,
    deployment_info deployment_info_v1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    refreshed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);