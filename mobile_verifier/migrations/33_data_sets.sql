CREATE TYPE data_set_status AS enum (
       'pending',
       'downloaded',
       'processed'
);

CREATE TYPE data_set_type AS enum (
       'urbanization',
       'footfall',
       'landtype'
);

CREATE TABLE data_sets (
       filename TEXT PRIMARY KEY,
       data_set data_set_type NOT NULL,
       time_to_use TIMESTAMPTZ NOT NULL,
       status data_set_status NOT NULL
);