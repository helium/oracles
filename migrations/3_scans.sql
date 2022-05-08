create table scans (
    id uuid primary key not null default uuid_generate_v1mc(),
    uplink uuid references uplinks on delete set null,
    cid integer,
    freq integer,
    lte boolean default false,
    mcc integer,
    mnc integer,
    rsrp integer,
    rsrq integer,

    created_at timestamptz default now(),
    updated_at timestamptz
);

SELECT trigger_updated_at('scans');