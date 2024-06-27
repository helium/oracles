CREATE TABLE IF NOT EXISTS public.key_to_assets (
    address character varying(255) NOT NULL,
    dao character varying(255) NULL,
    asset character varying(255) NULL,
    entity_key bytea NULL,
    bump_seed integer NULL,
    refreshed_at timestamp with time zone NULL,
    created_at timestamp with time zone NOT NULL,
    key_serialization jsonb NULL
);

ALTER TABLE
    public.key_to_assets
ADD
    CONSTRAINT key_to_assets_pkey PRIMARY KEY (address)