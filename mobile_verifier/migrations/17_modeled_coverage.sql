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
       radio_key TEXT NOT NULL,
       signal_level signal_level NOT NULL,
       coverage_claim_time TIMESTAMPTZ NOT NULL,
       inserted_at TIMESTAMPTZ NOT NULL,
       radio_type INT NOT NULL,
       PRIMARY KEY (uuid, hex)
);

CREATE TABLE seniority (
       radio_key TEXT NOT NULL,
       seniority_ts TIMESTAMPTZ NOT NULL,
       last_heartbeat TIMESTAMPTZ NOT NULL,
       uuid UUID NOT NULL,
       update_reason INT NOT NULL,
       inserted_at TIMESTAMPTZ NOT NULL,
       radio_type INT NOT NULL,
       PRIMARY KEY (radio_key, radio_type, seniority_ts)
);

ALTER TABLE wifi_heartbeats ADD COLUMN coverage_object UUID;
ALTER TABLE cbrs_heartbeats ADD COLUMN coverage_object UUID;
