create table files_processed (
	file_name varchar primary key,
	file_type varchar,
	file_timestamp timestamptz,
	processed_at timestamptz
);

insert into files_processed (file_name, file_type, file_timestamp, processed_at)
select 'migration', 'reward_manifest', to_timestamp(value::int) + interval '30 minutes', NOW()
from meta
where key = 'last_reward_manifest';
