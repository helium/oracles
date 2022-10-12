create table heartbeats (
       id bytea primary key not null,
       weight decimal not null,
       timestamp timestamptz not null
);
