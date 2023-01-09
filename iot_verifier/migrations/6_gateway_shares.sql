create table gateway_shares (
    hotspot_key text not null,
    reward_type reporttype not null,
    reward_timestamp timestamptz not null,
    hex_scale decimal not null,
    reward_unit decimal not null,
    -- id of the associated valid poc report
    poc_id bytea not null,
    primary key(hotspot_key, poc_id)
);

create index idx_hotspot_key on gateway_shares (hotspot_key);

create index idx_reward_type on gateway_shares (reward_type);
