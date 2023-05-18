create table subscriber_loc_verified (
  subscriber_id BYTEA not null,
  received_timestamp TIMESTAMPTZ not null,
  created_at TIMESTAMPTZ default now()
);
