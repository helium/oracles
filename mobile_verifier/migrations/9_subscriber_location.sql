create table subscriber_loc (
  subscriber_id text not null,
  date_bucket timestampz not null,
  hour_bucket decimal not null,
  reward_timestamp timestampz not null,
  created_at timestamptz default now(),
  primary key(subscriber_id, date_bucket, hour_bucket)
);
