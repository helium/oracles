CREATE TYPE speedtest AS (
       timestamp timestamp not null,
       upload_speed bigint not null,
       download_speed bigint not null,
       latency integer not null
);

CREATE TABLE speedtests (
       id test primary key not null,
       speedtests speedtest[] not null
);
