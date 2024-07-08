CREATE TABLE public.boosted_hexes (
    address character varying(255) NOT NULL,
    boost_config character varying(255) NULL,
    location numeric NULL,
    start_ts numeric NULL,
    reserved numeric [] NULL,
    bump_seed integer NULL,
    boosts_by_period bytea NULL,
    version integer NULL,
    refreshed_at timestamp with time zone NULL,
    created_at timestamp with time zone NOT NULL
);

ALTER TABLE
    public.boosted_hexes
ADD
    CONSTRAINT boosted_hexes_pkey PRIMARY KEY (address)