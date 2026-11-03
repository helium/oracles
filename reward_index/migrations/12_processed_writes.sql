create table processed_writes (
	write_id varchar primary key,
	processed_at timestamptz not null default now()
);
