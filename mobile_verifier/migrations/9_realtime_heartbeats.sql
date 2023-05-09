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

DROP TABLE speedtests;
DROP TYPE speedtest;

CREATE TYPE speedtest AS (
       timestamp timestamptz,
       upload_speed bigint,
       download_speed bigint,
       latency integer
);

CREATE TABLE speedtests (
       id text primary key not null,
       speedtests speedtest[] not null,
       latest_timestamp timestamptz not null
);
