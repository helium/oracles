CREATE TABLE coverage_insertion_time (
       uuid UUID NOT NULL,
       radio_key TEXT NOT NULL,
       inserted_at TIMESTAMPTZ NOT NULL,
       invalidated_at TIMESTAMPTZ
);

INSERT INTO coverage_insertion_time SELECT DISTINCT uuid, radio_key, inserted_at FROM hex_coverage;
