create table gateway_dc_shares (
    hotspot_key text not null,
    reward_type reporttype not null,
    reward_timestamp timestamptz not null,
    num_dcs integer default 0,
    -- id of the associated valid poc report
    id bytea not null,
    primary key(id)
);

create index idx_hotspot_key on gateway_dc_shares (hotspot_key);

create index idx_reward_type on gateway_dc_shares (reward_type);
