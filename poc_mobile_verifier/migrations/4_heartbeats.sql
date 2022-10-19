drop table heartbeats;

create table heartbeats (
  hotspot_key bytea not null,
  cbsd_id text not null,
  reward_weight not null,
  timestamp timestamp not null,
  primary key(pub_key, cbsd_id)
);
