create table meta (
    key text primary key not null,
    value text
);

insert into meta (key, value)
values ('last_height', '995041')