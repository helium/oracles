-- HIP-150: accumulate sessions per file, so each row has one timestamp.
--
-- The burn prices a row by the multiplier in force at its timestamp, which needs
-- the row to have a single timestamp rather than a range. Every report in an
-- ingest file carries that file's timestamp, so keying on it gives exactly that:
-- one row is one hotspot in one file, at one instant.
--
-- Reports within a file still merge, so a row still collects everything a
-- hotspot did in that file. It just no longer reaches across files.
--
-- Bytes are still converted to data credits in bulk. The burn groups these rows
-- by the multiplier it resolves for each, and converts once per group, so a
-- hotspot whose multiplier did not change is billed exactly as before.
--
-- The cost is row count: one row per hotspot per file, for as long as the burn
-- window holds them.
--
-- Existing rows are unique on (pub_key, payer), so widening the key cannot
-- collide.
ALTER TABLE data_transfer_sessions
    DROP CONSTRAINT data_transfer_sessions_pkey;

ALTER TABLE data_transfer_sessions
    ADD PRIMARY KEY (pub_key, payer, last_timestamp);
