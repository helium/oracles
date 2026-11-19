{{
  config(
    materialized='table',
    on_table_exists='replace',
    schema='chain',
    tags=['1h'],
    properties={
      'format': "'PARQUET'",
      'sorted_by': "ARRAY['pub_key', 'valid_from']"
    }
  )
}}

-- One row per assertion period: where each mobile hotspot was asserted, and
-- from when until when, in chain time. Any model can then ask where a hotspot
-- was asserted at time T with a plain range join:
--
--   valid_from <= T and (valid_to is null or T < valid_to)
--
-- Built from `chain.mobile_hotspot_history`, the event log icicle writes: one
-- row per chain update to a hotspot, with the asserted cell's center already
-- computed (Trino has no H3 functions). Written to `chain` beside it.
--
-- A NEW PERIOD STARTS ONLY WHEN `asserted_hex` OR `device_type` CHANGES. Rows
-- that change something else -- the serial, the asset, the name -- are folded
-- into the period they fall in (about 1 in 10 history rows as of 2026-11). Comparisons are
-- `is distinct from`, so going from asserted to unasserted (a null hex) or back
-- also starts a period.
--
-- `valid_to` IS THE NEXT PERIOD'S `valid_from`, and null for the period in
-- effect now. Periods for one hotspot therefore tile its history with no gaps
-- and no overlaps. The first period starts at the hotspot's first history row:
-- the history opens with a snapshot of every hotspot on 2025-10-30, so a
-- hotspot asserted before then starts there, not at its real assertion time.
--
-- ORDER IS THE CHAIN'S `timestamp`, TIES BROKEN BY `block`, `file_ts` AND
-- `record_index`. This model is the one place ties are broken, so everything
-- downstream inherits one answer. When updates share a chain timestamp, the
-- last one's period stands and the ones before it cover no time.
-- `received_timestamp` is when ingest saw the update and plays no part.
--
-- DUPLICATES NEED NO DEDUPE STEP. icicle delivers at least once: a crash
-- between an Iceberg commit and its cleanup re-commits the batch, so an update
-- can appear twice with the same `(file_ts, record_index)`. A second copy sorts
-- beside the first and repeats its hex and device type, so it never starts a
-- period; it is folded in like any other unchanged row.
--
-- A FULL REBUILD EVERY RUN, not incremental. The history is about 70k rows,
-- so rebuilding takes seconds and avoids an incremental model's hard case: a
-- late update landing inside an existing period, which would have to split it
-- and rewrite the following period's `valid_from`. `on_table_exists='replace'`
-- swaps the table in one Iceberg commit (CREATE OR REPLACE), so a reader never
-- sees it empty or half-built, and earlier versions stay in the table's
-- snapshot history until they are expired.

with ordered as (

    select
        pub_key,
        "timestamp",
        block,
        file_ts,
        record_index,
        asserted_hex,
        asserted_lat,
        asserted_lon,
        device_type,
        row_number() over chain_order as event_number,
        lag(asserted_hex) over chain_order as previous_hex,
        lag(device_type) over chain_order as previous_device_type
    from {{ source('chain', 'mobile_hotspot_history') }}
    window chain_order as (
        partition by pub_key
        order by "timestamp", block, file_ts, record_index
    )

),

period_starts as (

    select *
    from ordered
    where
        event_number = 1
        or asserted_hex is distinct from previous_hex
        or device_type is distinct from previous_device_type

),

periods as (

    select
        pub_key,
        asserted_hex,
        asserted_lat,
        asserted_lon,
        device_type,
        "timestamp" as valid_from,
        lead("timestamp") over (
            partition by pub_key
            order by "timestamp", block, file_ts, record_index
        ) as valid_to
    from period_starts

)

-- A period that ends the moment it begins was replaced in the same instant
-- (two updates at one chain timestamp, ordered by block); it covers no time,
-- so it is dropped and the later one stands. That keeps (pub_key, valid_from)
-- unique.
select *
from periods
where valid_to is null or valid_to > valid_from
