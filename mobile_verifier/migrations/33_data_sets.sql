DROP TYPE IF EXISTS data_set_status;
CREATE TYPE data_set_status AS enum (
       'pending',
       'downloaded',
       'processed'
);

DROP TYPE IF EXISTS data_set_type;
CREATE TYPE data_set_type AS enum (
       'urbanization',
       'footfall',
       'landtype'
);

CREATE TABLE IF NOT EXISTS hex_assignment_data_set_status (
       filename TEXT PRIMARY KEY,
       data_set data_set_type NOT NULL,
       time_to_use TIMESTAMPTZ NOT NULL,
       status data_set_status NOT NULL
);
