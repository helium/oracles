CREATE TYPE signal_level as ENUM (
       'none',
       'low',
       'medium',
       'high'
);

CREATE TABLE hex_coverage (
       uuid UUID NOT NULL,
       hex INTEGER NOT NULL,
       indoor BOOLEAN NOT NULL,
       cbsd_id TEXT NOT NULL,
       signal_level signal_level NOT NULL,
       coverage_claim_time TIMESTAMPTZ NOT NULL
       PRIMARY KEY (uuid, hex)
);

CREATE TABLE coverage_claim_time (
       cbsd_id TEXT PRIMARY KEY,
       uuid UUID NOT NULL,
       last_heartbeat TIMESTAMPTZ NOT NULL,
       coverage_claim_time TIMESTAMPTZ NOT NULL
);

ALTER TABLE heartbeats ADD COLUMN coverage_object UUID;
