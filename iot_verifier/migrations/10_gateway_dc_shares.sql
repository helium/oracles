create table gateway_dc_shares (
    hotspot_key text not null,
    reward_timestamp timestamptz not null,
    num_dcs bigint default 0,
    -- id of the associated valid poc report
    id bytea primary key not null
);

create index idx_hotspot_key on gateway_dc_shares (hotspot_key);

create index idx_reward_type on gateway_dc_shares (reward_type);
