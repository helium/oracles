create type lorastatus AS enum (
    'pending',
    'valid',
    'invalid'
);

create type reporttype  AS enum (
    'witness',
    'beacon'
);

create table poc_report (
    id bytea primary key not null,
    packet_data bytea not null,
    report_data bytea not null,
    report_type reporttype,
    status lorastatus default 'pending' not null,
    attempts integer default 0,
    report_timestamp timestamptz not null,
    last_processed timestamptz default now() not null,
    created_at timestamptz default now()
);
