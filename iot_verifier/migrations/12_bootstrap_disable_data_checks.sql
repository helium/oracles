insert into meta
    (key, value)
values
    ('disable_complete_data_checks_until', '0')
on conflict
    (key)
do nothing;
