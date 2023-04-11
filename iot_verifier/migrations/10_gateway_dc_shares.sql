create table gateway_dc_shares (
    hotspot_key text not null,
    reward_timestamp timestamptz not null,
    num_dcs bigint default 0,
    -- id of the associated valid poc report
    id bytea primary key not null
);

create index idx_gds_hotspot_key on gateway_dc_shares (hotspot_key);