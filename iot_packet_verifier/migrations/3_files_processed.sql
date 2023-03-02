CREATE TABLE files_processed (
	file_name VARCHAR PRIMARY KEY,
	file_type VARCHAR NOT NULL,
	file_timestamp TIMESTAMPTZ NOT NULL,
	processed_at TIMESTAMPTZ NOT NULL
);
