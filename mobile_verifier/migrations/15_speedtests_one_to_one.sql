
CREATE TABLE speedtests_migration (
       id TEXT NOT NULL,
       upload_speed bigint,
       download_speed bigint,
       latency integer,
       timestamp TIMESTAMP NOT NULL
);

insert into speedtests_migration (id, upload_speed, download_speed, latency, timestamp)
select id, (st).upload_speed, (st).download_speed, (st).latency, (st).timestamp
from (select id, unnest(speedtests) as st from speedtests) as tmp;

ALTER TABLE speedtests RENAME TO speedtests_old;
ALTER TABLE speedtests_migration RENAME TO speedtests;
