-- Add migration script here
alter table routes add column deleted bool not null default false;
alter table route_eui_pairs add column deleted bool not null default false;
alter table route_devaddr_ranges add column deleted bool not null default false;
alter table route_session_key_filters add column deleted bool not null default false;
