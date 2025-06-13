CREATE TABLE public.key_to_assets (
    address character varying(255) NOT NULL,
    dao character varying(255),
    asset character varying(255),
    entity_key bytea,
    bump_seed integer,
    refreshed_at timestamp with time zone,
    created_at timestamp with time zone NOT NULL,
    key_serialization jsonb
);

INSERT INTO "key_to_assets" ("address", "dao", "asset", "entity_key", "bump_seed", "refreshed_at", "created_at", "key_serialization") VALUES
('4TCw73EqhtXbDp19D4WY72vWZo9QWY6gcphRFswzKBNk',	'BQ3MCuTT5zVBhNfQ4SjMh3NPVhFy73MPV8rjfq5d1zie',	'HJtATvtga22LQPViQGoSdwqoHMS8uxirNNsRyGpQK1Nc',	'Helium Mobile Mapping Rewards',	254,	'2025-06-12 02:24:01.305+00',	'2025-06-12 02:24:01.305+00',	'"utf8"');

INSERT INTO "key_to_assets" ("address", "dao", "asset", "entity_key", "bump_seed", "refreshed_at", "created_at", "key_serialization") VALUES
('4RsbdRtGNiMEUPPJrmkVpSUswYXLKtZLUKaiyaGFxyd5',	'BQ3MCuTT5zVBhNfQ4SjMh3NPVhFy73MPV8rjfq5d1zie',	'4xpWF7KjbcShMt3LnEJmPkbDVyEZ19zrwtJwS1e1e827',	decode('0000d5a568ab7b418ba68eabe61b5b042f84afcfa86f1cdfcdba644401b6bcb65c84af622936', 'hex'),	253,	'2025-04-30 16:49:26.499+00',	'2023-05-04 14:36:16.429+00',	'"b58"');
