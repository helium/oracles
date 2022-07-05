create table maker (
    pubkey text primary key not null,
    description text,

    created_at timestamptz default now()
);

insert into maker(pubkey, description)
values ('13y2EqUUzyQhQGtDSoXktz8m5jHNSiwAKLTYnHNxZq2uH5GGGym', 'FreedomFi');