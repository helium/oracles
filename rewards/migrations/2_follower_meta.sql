create table follower_meta (
    key text primary key not null,
    value text
);

insert into follower_meta (key, value)
values ('last_height', '1431670')
