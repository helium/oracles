create table last_witness (
    id bytea primary key not null,
    timestamp timestamptz not null
);
-- seed last_witness with timestamps from last_beacon
insert into last_witness (id, timestamp)
select id, timestamp from last_beacon
where timestamp > now() - interval '7 day';
