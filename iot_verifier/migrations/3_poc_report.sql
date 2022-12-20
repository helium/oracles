create type iotstatus AS enum (
    'pending',
    'ready',
    'valid',
    'invalid'
);

create type reporttype  AS enum (
    'witness',
    'beacon'
);

create table poc_report (
    id bytea primary key not null,
    -- remote_entropy: allow nulls as only beacon reports will populate this
    remote_entropy bytea,
    packet_data bytea not null,
    report_data bytea not null,
    report_type reporttype,
    status iotstatus default 'pending' not null,
    attempts integer default 0,
    report_timestamp timestamptz not null,
    last_processed timestamptz default now() not null,
    created_at timestamptz default now()
);

CREATE INDEX idx_poc_report_packet_data
ON poc_report(packet_data);

CREATE INDEX idx_poc_report_report_type
ON poc_report(report_type);

CREATE INDEX idx_poc_report_status
ON poc_report(status);

CREATE INDEX idx_poc_report_created_at
ON poc_report(created_at);

CREATE INDEX idx_poc_report_attempts
ON poc_report(attempts);

CREATE INDEX idx_poc_report_report_type_status_created_at
ON poc_report (report_type,status,created_at);


