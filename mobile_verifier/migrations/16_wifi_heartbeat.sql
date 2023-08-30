ALTER TYPE cell_type ADD VALUE 'celltypenone' AFTER 'sercommoutdoor';
ALTER TYPE cell_type ADD VALUE 'novagenericwifiindoor' AFTER 'celltypenone';

CREATE TABLE wifi_heartbeats (
       hotspot_key TEXT NOT NULL,
       cell_type cell_type NOT NULL,
       truncated_timestamp TIMESTAMPTZ NOT NULL CHECK (truncated_timestamp = date_trunc('hour', truncated_timestamp)),
       latest_timestamp TIMESTAMPTZ NOT NULL,
       location_validation_timestamp TIMESTAMPTZ,
       distance_to_asserted BIGINT,
       PRIMARY KEY(hotspot_key, truncated_timestamp)
);

ALTER TABLE heartbeats RENAME TO cbrs_heartbeats;
