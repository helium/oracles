create table files_processed (
	file_name varchar primary key,
	file_type varchar,
	file_timestamp timestamptz,
	processed_at timestamptz
);

insert into files_processed (file_name, file_type, file_timestamp, processed_at)
select 'migration', 'iot_poc', to_timestamp(value::int) + interval '10 minutes', NOW()
from meta
where key = 'last_reward_end_time';
