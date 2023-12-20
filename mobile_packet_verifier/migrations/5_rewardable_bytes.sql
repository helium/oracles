
alter table data_transfer_sessions add rewardable_bytes bigint;

update data_transfer_sessions
set rewardable_bytes = uploaded_bytes + downloaded_bytes;

alter table data_transfer_sessions alter column rewardable_bytes set not null;
