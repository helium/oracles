alter table routes add column ignore_empty_skf bool;
update routes set ignore_empty_skf = 'f';
alter table routes alter column ignore_empty_skf set default false;
alter table routes alter column ignore_empty_skf set not null;
