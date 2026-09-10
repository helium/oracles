-- Test data for local development. NOT used by production and NOT a schema
-- definition -- just rows.
--
-- Idempotent by construction: every table is emptied before it is filled. That
-- is not tidiness, it is the whole point. `docker compose down` WITHOUT `-v`
-- leaves the Iceberg data in the volume, so re-running an insert-only fixture
-- script against a warehouse you thought was fresh duplicates every row -- and
-- the first thing you see is the source's uniqueness test failing, which reads
-- like a code defect rather than stale local state.
--
-- Only `hotspots.enabled_carriers_history` is covered, because it is the only
-- source this project declares. It is also the one table in this catalog with
-- no Rust row type: if an oracle-written table ever needs fixtures, write them
-- through its own row structs rather than as INSERT statements here. SQL
-- fixtures re-encode column order and types, which is exactly the duplication
-- the rest of this project avoids; it is acceptable here only because there is
-- no definition to reuse.

DELETE FROM "${CATALOG}".hotspots.enabled_carriers_history;

INSERT INTO "${CATALOG}".hotspots.enabled_carriers_history VALUES
  -- hs_a reports twice. The later device `timestamp` must win, so the mart
  -- should show att+tmo on firmware 2.19.0 and never the 2.18.0 row.
  (1757000000, 0, TIMESTAMP '2026-09-07 10:00:00 UTC', 'hs_a',
   ARRAY['att'], NULL, '2.18.0',
   TIMESTAMP '2026-09-07 09:59:00 UTC', 'signer_1'),
  (1757000000, 1, TIMESTAMP '2026-09-07 10:05:00 UTC', 'hs_a',
   ARRAY['att', 'tmo'], ARRAY['orion'], '2.19.0',
   TIMESTAMP '2026-09-07 10:04:00 UTC', 'signer_1'),

  -- hs_b: one report, and a null sampling list. Null is not an empty array
  -- here, so this is the row that shows the difference downstream.
  (1757000000, 2, TIMESTAMP '2026-09-07 10:06:00 UTC', 'hs_b',
   ARRAY['tmo'], NULL, '2.18.0',
   TIMESTAMP '2026-09-07 10:05:00 UTC', 'signer_1'),

  -- hs_c: device clock set to 2099. Keeps the failure mode that decided this
  -- model's watermark permanently reproducible: a watermark over `timestamp`
  -- lands here and never admits another row, for any hotspot.
  (1757000000, 3, TIMESTAMP '2026-09-07 10:07:00 UTC', 'hs_c',
   ARRAY['att'], NULL, '2.18.0',
   TIMESTAMP '2099-01-01 00:00:00 UTC', 'signer_1'),

  -- hs_d: received AFTER hs_c, honest timestamp far behind hs_c's bogus one.
  -- Invisible forever under a `timestamp` watermark; picked up under
  -- `received_timestamp`. Deleting this row removes the regression guard.
  (1757100000, 0, TIMESTAMP '2026-09-07 10:20:00 UTC', 'hs_d',
   ARRAY['tmo', 'att'], NULL, '2.18.0',
   TIMESTAMP '2026-09-07 10:19:00 UTC', 'signer_1');
