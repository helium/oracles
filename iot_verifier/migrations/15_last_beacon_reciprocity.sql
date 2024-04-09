create table last_beacon_recip (
    id bytea primary key not null,
    timestamp timestamptz not null
);
-- seed beacon_recip with timestamps from last_beacon
insert into last_beacon_recip (id, timestamp)
select id, timestamp from last_beacon
where timestamp > now() - interval '7 day';
