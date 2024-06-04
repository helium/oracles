CREATE TABLE IF NOT EXISTS public.mobile_hotspot_infos (
    address character varying(255) NOT NULL,
    asset character varying(255) NULL,
    bump_seed integer NULL,
    location numeric NULL,
    is_full_hotspot boolean NULL,
    num_location_asserts integer NULL,
    refreshed_at timestamp with time zone NULL,
    created_at timestamp with time zone NOT NULL,
    is_active boolean NULL,
    dc_onboarding_fee_paid numeric NULL,
    device_type jsonb NOT NULL
);

ALTER TABLE
    public.mobile_hotspot_infos
ADD
    CONSTRAINT mobile_hotspot_infos_pkey PRIMARY KEY (address)