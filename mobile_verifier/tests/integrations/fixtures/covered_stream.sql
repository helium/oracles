-- CBRS
INSERT INTO "coverage_objects" ("uuid", "radio_type", "radio_key", "indoor", "coverage_claim_time", "trust_score", "inserted_at", "invalidated_at") VALUES
('0194afb2-05af-7682-bd9b-14684b92e419',	'cbrs',	'P27-SCE4255W2202CW5001441',	't',	'2025-01-29 01:34:27+00',	1000,	'2025-01-29 01:50:43.093896+00',	NULL);

INSERT INTO "hexes" ("uuid", "hex", "signal_level", "signal_power", "urbanized", "footfall", "landtype", "service_provider_override") VALUES
('0194afb2-05af-7682-bd9b-14684b92e419',	631243921458086911,	'low',	-1140,	'a',	'a',	'a',	'c');

INSERT INTO "seniority" ("radio_key", "seniority_ts", "last_heartbeat", "uuid", "update_reason", "inserted_at", "radio_type") VALUES
('P27-SCE4255W2202CW5001441',	'2025-02-27 12:41:58.851816+00',	'2024-11-30 23:00:33.818+00',	'01937c0e-11f5-7751-a2cc-553d8532061f',	2,	'2025-02-27 12:41:58.851827+00',	'cbrs');


-- WIFI
INSERT INTO "coverage_objects" ("uuid", "radio_type", "radio_key", "indoor", "coverage_claim_time", "trust_score", "inserted_at", "invalidated_at") VALUES
('019516a5-2a8c-7480-b319-8b4f801ebe6c',	'wifi',	'1trSuseow771kqR8Muvj8rK3SbM26jN3o8GuDEjuUMEWZp7WzvexMtZwNP1jH7BMvaUgpb2fWQCxBgCm4UCFbHn6x6ApFzXoaUTb6SMSYYc6uwUQiHsa9vFC8LpPEwo6bv7rjKddgSxxtRhNojuck5dAXkAuWaxW9fW1vxwSqAq7WKEMnRMfjMzbpC1yKVA9iBd3m7s6V9KqLLCBaG4BdYszS3cbsQY92d9BkTapkLfbFrVEaLTeF5ETT7eewTGYQwY2h8knk9x9e84idnNVUKTiJs34AvSaAXkbRehzJpAjQ2skHXb1PtS7FU6TVgmQpW1tykJ9qJkDzDf9JWiHSvupkxvmK6MT2Aqkvc1owy2Q7i',	't',	'2025-02-18 01:21:17+00',	1000,	'2025-02-18 01:37:29.412982+00',	NULL);
INSERT INTO "hexes" ("uuid", "hex", "signal_level", "signal_power", "urbanized", "footfall", "landtype", "service_provider_override") VALUES
('019516a5-2a8c-7480-b319-8b4f801ebe6c',	631210990370874367,	'high',	-640,	'a',	'a',	'a',	'c');
INSERT INTO "seniority" ("radio_key", "seniority_ts", "last_heartbeat", "uuid", "update_reason", "inserted_at", "radio_type") VALUES
('1trSuseow771kqR8Muvj8rK3SbM26jN3o8GuDEjuUMEWZp7WzvexMtZwNP1jH7BMvaUgpb2fWQCxBgCm4UCFbHn6x6ApFzXoaUTb6SMSYYc6uwUQiHsa9vFC8LpPEwo6bv7rjKddgSxxtRhNojuck5dAXkAuWaxW9fW1vxwSqAq7WKEMnRMfjMzbpC1yKVA9iBd3m7s6V9KqLLCBaG4BdYszS3cbsQY92d9BkTapkLfbFrVEaLTeF5ETT7eewTGYQwY2h8knk9x9e84idnNVUKTiJs34AvSaAXkbRehzJpAjQ2skHXb1PtS7FU6TVgmQpW1tykJ9qJkDzDf9JWiHSvupkxvmK6MT2Aqkvc1owy2Q7i',	'2025-03-06 12:53:47.452988+00',	'2025-03-06 14:46:00.713+00',	'019516a5-2a8c-7480-b319-8b4f801ebe6c',	2,	'2025-03-06 12:53:47.453303+00',	'wifi');

