insert into meta
    (key, value)
values
    ('next_rewarded_end_time', '1671586200000')
on conflict
    (key)
do nothing;
