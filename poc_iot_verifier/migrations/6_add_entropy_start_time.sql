alter table poc_report add entropy_start_time timestamptz default now() not null;
