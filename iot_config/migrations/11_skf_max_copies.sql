alter table route_session_key_filters add column max_copies int;

update route_session_key_filters set max_copies = 0;

alter table route_session_key_filters alter column max_copies set not null;
