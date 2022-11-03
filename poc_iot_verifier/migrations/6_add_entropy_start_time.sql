-- :up

alter table poc_report add entropy_start_time timestamptz default now() not null;

-- :down

alter table poc_report drop entropy_start_time;
