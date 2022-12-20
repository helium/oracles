insert into meta
    (key, value)
values
    ('last_rewarded_end_time', '1671499800000')
on conflict
    (key)
do nothing;
