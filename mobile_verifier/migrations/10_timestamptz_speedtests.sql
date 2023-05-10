DROP TYPE speedtest;
DROP TABLE speedtests;

CREATE TYPE speedtest AS (
       timestamp TIMESTAMPTZ,
       upload_speed BIGINT,
       download_speed BIGINT,
       latency INTEGER
);

CREATE TABLE speedtests (
       id TEXT PRIMARY KEY NOT NULL,
       speedtests speedtest[] NOT NULL,
       latest_timestamp TIMESTAMPTZ NOT NULL
);
