CREATE TYPE speedtest AS (
       timestamp timestamp,
       upload_speed bigint,
       download_speed bigint,
       latency integer
);

CREATE TABLE speedtests (
       id text primary key not null,
       speedtests speedtest[] not null
);
