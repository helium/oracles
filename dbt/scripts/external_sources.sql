-- Mirror of the mobile-catalog tables this repo does NOT produce.
--
-- Every other source in this project is created in CI from its own
-- `table_definition()` in Rust, so the schema has exactly one definition and
-- dbt is checked against it. These tables are written by a pipeline outside
-- this repo, so there is no definition here to derive them from and this file
-- is a hand-maintained COPY. Treat it that way:
--
--   * It is not authoritative. The upstream table is.
--   * When upstream changes a column, this file and models/sources.yml both
--     have to follow. CI will fail on the mismatch -- as a missing or wrongly
--     typed column, not as a quietly wrong mart -- which is the best guarantee
--     available for a table we do not own.
--   * Refresh it with `SHOW CREATE TABLE mobile.hotspots.<table>` against the
--     production cluster, then re-apply the local edits noted below.
--
-- Local edits applied to the production DDL, and why:
--
--   * `location` dropped. The production DDL points at
--     s3://mobileoracles-mainnet/...; letting the local catalog assign a
--     location under its own warehouse is the whole point of a local stack, and
--     naming the real bucket here would aim test writes at production storage.
--   * Catalog name parameterised. `mobile` in production, `iceberg` locally
--     (see dbt/README.md on why local Trino has no `mobile` catalog).
--
-- Column names, types, nullability, partitioning and sort order are otherwise
-- verbatim. Those are the parts that drift.

CREATE SCHEMA IF NOT EXISTS "${CATALOG}".hotspots;

CREATE TABLE IF NOT EXISTS "${CATALOG}".hotspots.enabled_carriers_history (
   file_ts bigint NOT NULL,
   record_index bigint NOT NULL,
   received_timestamp timestamp(6) with time zone NOT NULL,
   hotspot_pubkey varchar NOT NULL,
   enabled_carriers array(varchar),
   sampling_enabled_carriers array(varchar),
   firmware_version varchar,
   timestamp timestamp(6) with time zone NOT NULL,
   signer varchar NOT NULL
)
WITH (
   compression_codec = 'ZSTD',
   format = 'PARQUET',
   format_version = 2,
   partitioning = ARRAY['day(received_timestamp)'],
   sorted_by = ARRAY['hotspot_pubkey ASC NULLS FIRST','received_timestamp ASC NULLS FIRST']
);
