create table hotspot_data_transfer_sessions (
    pub_key TEXT NOT NULL,
    payer TEXT NOT NULL,
    upload_bytes BIGINT NOT NULL,
    download_bytes BIGINT NOT NULL,
    num_dcs BIGINT NOT NULL,
    received_timestamp TIMESTAMPTZ not null,
    created_at TIMESTAMPTZ default now()
);
