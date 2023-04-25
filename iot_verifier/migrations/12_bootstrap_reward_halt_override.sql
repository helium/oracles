CREATE TYPE reward_type as enum('beacon', 'witness', 'packet');

insert into meta
    (key, value)
values
    ('halt_rewards_override', 'false')
on conflict
    (key)
do nothing;

CREATE TABLE rewards_history (
    reward_type reward_type,
    count bigint NOT NULL,
    epoch_ts timestamptz NOT NULL,
    created_at timestamptz  NOT NULL default now()
);

