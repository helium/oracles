CREATE TABLE coverage_objects (
    uuid UUID NOT NULL,
    radio_type radio_type NOT NULL,
    radio_key TEXT NOT NULL,
    indoor BOOLEAN NOT NULL,
    coverage_claim_time TIMESTAMPTZ NOT NULL,
    trust_score INTEGER,
    inserted_at TIMESTAMPTZ NOT NULL,
    invalidated_at TIMESTAMPTZ,
    PRIMARY KEY (uuid)
);

CREATE TABLE hexes (
    uuid UUID NOT NULL REFERENCES coverage_objects(uuid),
    hex BIGINT NOT NULL,
    signal_level signal_level NOT NULL,
    signal_power INTEGER,
    PRIMARY KEY (uuid, hex)
);

INSERT INTO coverage_objects(uuid, radio_type, radio_key, indoor, coverage_claim_time, inserted_at, invalidated_at)
SELECT DISTINCT uuid, radio_type, radio_key, indoor, coverage_claim_time, inserted_at, invalidated_at
FROM hex_coverage;

INSERT INTO hexes(uuid, hex, signal_level, signal_power)
SELECT uuid, hex, signal_level, signal_power
FROM hex_coverage;

ALTER TABLE hex_coverage RENAME TO old_hex_coverage;

ALTER TYPE cell_type RENAME VALUE 'novagenericwifiindoor' TO 'novagenericwifi';

