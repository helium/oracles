DO $$ BEGIN
CREATE TYPE data_set_status AS enum (
       'pending',
       'downloaded',
       'processed'
);
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

DO $$ BEGIN
CREATE TYPE data_set_type AS enum (
       'urbanization',
       'footfall',
       'landtype'
);
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

CREATE TABLE IF NOT EXISTS hex_assignment_data_set_status (
       filename TEXT PRIMARY KEY,
       data_set data_set_type NOT NULL,
       time_to_use TIMESTAMPTZ NOT NULL,
       status data_set_status NOT NULL
);
