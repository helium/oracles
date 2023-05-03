DROP TABLE heartbeats;

CREATE TYPE cell_type AS ENUM (
       'nova436h',
       'nova430i',
       'neutrino430',
       'sercommindoor',
       'sercommoutdoor'
);

CREATE TABLE heartbeats (
       cbsd_id TEXT NOT NULL,
       hotspot_key TEXT NOT NULL,
       cell_type cell_type NOT NULL,
       latest_timestamp TIMESTAMPTZ NOT NULL,
       truncated_timestamp TIMESTAMPTZ NOT NULL,
       PRIMARY KEY(cbsd_id, truncated_timestamp)
);
