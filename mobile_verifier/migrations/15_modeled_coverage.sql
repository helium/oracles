CREATE TYPE signal_level AS ENUM (
       'none',
       'low',
       'medium',
       'high'
);

CREATE TABLE hex_coverage (
       uuid UUID NOT NULL,
       hex BIGINT NOT NULL,
       indoor BOOLEAN NOT NULL,
       cbsd_id TEXT NOT NULL,
       signal_level signal_level NOT NULL,
       coverage_claim_time TIMESTAMPTZ NOT NULL,
       inserted_at TIMESTAMPTZ NOT NULL,
       PRIMARY KEY (uuid, hex)
);

CREATE TABLE seniority (
       cbsd_id TEXT NOT NULL,
       seniority_ts TIMESTAMPTZ NOT NULL,
       last_heartbeat TIMESTAMPTZ NOT NULL,
       uuid UUID NOT NULL,
       PRIMARY KEY (cbsd_id, seniority_ts)
);

ALTER TABLE heartbeats ADD COLUMN coverage_object UUID;
