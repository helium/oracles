insert into meta
    (key, value)
values
    ('last_rewarded_end_time', '1671499800'),
    ('next_rewarded_end_time', '1671586200')
on conflict
    (key)
do nothing;
