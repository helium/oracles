CREATE TYPE speedtest_migration AS (
       timestamp TIMESTAMPTZ,
       upload_speed BIGINT,
       download_speed BIGINT,
       latency INTEGER
);

CREATE FUNCTION tomigration(speedtest)
       RETURNS speedtest_migration
       STRICT IMMUTABLE LANGUAGE SQL AS
$$ SELECT CAST ( ROW ( ($1).timestamp at time zone 'UTC', ($1).upload_speed, ($1).download_speed, ($1).latency ) AS speedtest_migration ); $$ ;

CREATE CAST ( speedtest as speedtest_migration ) WITH FUNCTION tomigration(speedtest) AS IMPLICIT;

ALTER TABLE speedtests ALTER COLUMN speedtests TYPE speedtest_migration [] ;
ALTER TABLE speedtests ALTER COLUMN latest_timestamp TYPE TIMESTAMPTZ USING latest_timestamp at time zone 'UTC' ;

DROP CAST ( speedtest as speedtest_migration );
DROP FUNCTION tomigration(speedtest);
DROP TYPE speedtest;
ALTER TYPE speedtest_migration RENAME TO speedtest;
