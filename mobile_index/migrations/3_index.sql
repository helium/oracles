create table reward_index (
    address bytea primary key not null,
    rewards bigint not null default 0,
    last_reward timestamptz
);
