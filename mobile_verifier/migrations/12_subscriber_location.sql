create table subscriber_loc (
  subscriber_id TEXT not null,
  date_bucket TIMESTAMPTZ not null,
  hour_bucket DECIMAL not null,
  reward_timestamp TIMESTAMPTZ not null,
  created_at TIMESTAMPTZ default now(),
  primary key(subscriber_id, date_bucket, hour_bucket)
);
