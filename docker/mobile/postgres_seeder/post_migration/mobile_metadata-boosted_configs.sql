CREATE TABLE public.boost_configs (
    address character varying(255) NOT NULL,
    price_oracle character varying(255) NULL,
    payment_mint character varying(255) NULL,
    sub_dao character varying(255) NULL,
    rent_reclaim_authority character varying(255) NULL,
    boost_price numeric NULL,
    period_length integer NULL,
    minimum_periods integer NULL,
    bump_seed integer NULL,
    start_authority character varying(255) NULL,
    refreshed_at timestamp with time zone NULL,
    created_at timestamp with time zone NOT NULL
);

ALTER TABLE
    public.boost_configs
ADD
    CONSTRAINT boost_configs_pkey PRIMARY KEY (address)