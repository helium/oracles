create table cell_speedtest (
    id uuid primary key not null default uuid_generate_v1mc(),

    pubkey text,    
    serial text,
    timestamp timestamptz not null,
    upload_speed bigint,
    download_speed bigint,
    latency integer,

    created_at timestamptz default now()
);

create index cell_speedtest_pubkey_idx on cell_speedtest(pubkey);
create index cell_speedtest_timestamp_idx on cell_speedtest(timestamp);