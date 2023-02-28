create table files_processed (
	file_name varchar primary key,
	file_type varchar not null,
	file_timestamp timestamptz not null,
	processed_at timestamptz not null
);

insert into files_processed (file_name, file_type, file_timestamp, processed_at)
select 'migration', 'reward_manifest', to_timestamp(value::bigint / 1000) + interval '1801 seconds', NOW()
from meta
where key = 'last_reward_manifest';
