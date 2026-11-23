{{
  config(
    materialized='incremental',
    incremental_strategy='append',
    on_schema_change='fail',
    schema='chain',
    tags=['1h'],
    properties={
      'format': "'PARQUET'",
      'sorted_by': "ARRAY['pub_key', 'valid_from']"
    }
  )
}}

-- One row per assertion of each mobile hotspot: the moment, in chain time, it
-- was asserted at a new cell (or changed device type), and where. Each row
-- holds until the hotspot's next row, so the period it starts ends at the next
-- row's `valid_from`:
--
--   lead(valid_from) over (
--       partition by pub_key
--       order by valid_from, block, file_ts, record_index
--   ) as valid_to                                   -- null: still in effect
--
-- and where a hotspot was asserted at time T is then a range join,
-- `valid_from <= T and (valid_to is null or T < valid_to)`. `valid_to` is not
-- stored because storing it would mean rewriting a hotspot's previous row
-- whenever it reasserts; see APPEND-ONLY below.
--
-- Built from `chain.mobile_hotspot_history`, the event log icicle writes: one
-- row per chain update to a hotspot, with the asserted cell's center already
-- computed (Trino has no H3 functions). Written to `chain` beside it.
--
-- A ROW IS WRITTEN ONLY WHEN `asserted_hex` OR `device_type` CHANGES. A
-- history row that changes only something else -- the serial, the asset, the
-- name -- writes no row here; the hotspot's current assertion just continues
-- (about 1 in 10 history rows as of 2026-11). Comparisons are `is distinct
-- from`, so going from asserted to unasserted (a null hex) or back also
-- counts. A hotspot's first history row always writes one; the history
-- opens with a snapshot of every hotspot on 2025-10-30, so a hotspot asserted
-- before then starts there, not at its real assertion time.
--
-- ORDER IS THE CHAIN'S `timestamp`, TIES BROKEN BY `block`, `file_ts` AND
-- `record_index`, all kept on the row so readers order by exactly the same key.
-- Two updates at one chain timestamp both get rows; under that order the first
-- one's period ends the instant it begins and matches no time.
-- `received_timestamp` is when ingest saw the update and is only the
-- incremental watermark.
--
-- APPEND-ONLY, SO THE TABLE CAN BE STREAMED. Every run only inserts, and only
-- rows that are new, so each run commits an Iceberg `append` snapshot holding
-- exactly the new assertions -- what helium-iceberg's stream poller and Spark
-- Structured Streaming follow (they skip the `overwrite` snapshots a `merge`
-- commits, and a full rebuild would append the whole table every run). A run
-- with nothing new commits no snapshot.
--
-- INCREMENTAL SHAPE:
--
--   1. NEW HISTORY -- rows received since the last run. The watermark is
--      `max(received_timestamp)` of this table: the newest update that started
--      an assertion, so the window also re-reads any later updates that did
--      not, which is harmless. The lookback (`incremental_lookback_minutes`)
--      covers ingest commit skew.
--   2. EACH HOTSPOT'S LATEST ROW HERE seeds the comparison, so new history is
--      compared against where the hotspot is already recorded as asserted.
--      Only history AFTER that row, in chain order, is considered: the lookback
--      re-reads updates already accounted for, and they must not start a
--      second row.
--   3. Of the history after the seed, rows whose hex or device type differs
--      from the row before them are appended.
--
-- DUPLICATES NEED NO DEDUPE STEP. icicle delivers at least once, so an update
-- can appear twice with the same `(file_ts, record_index)`. A second copy sorts
-- beside the first and repeats its hex and device type, so it never starts a
-- row; a copy redelivered in a later run sorts at or before the seed and is not
-- considered.
--
-- WHAT APPEND-ONLY GIVES UP. A chain update that reaches the history AFTER a
-- later update for the same hotspot has been processed -- out of chain order
-- -- sorts before the seed and is ignored, because placing it would mean
-- rewriting rows already written. icicle reads ingest's files in order, so this
-- needs an out-of-order chain report; `--full-refresh` places it correctly.
--
-- THE WATERMARK IS A LITERAL, read once at compile time from the table's
-- `$partitions` metadata, as in `speedtest_averages`. Unit tests set
-- `mobile_hotspot_assertions_watermark` instead.

{%- set lookback_minutes = var('incremental_lookback_minutes') -%}

{%- set watermark = none -%}
{%- if is_incremental() -%}
  {%- set watermark = var('mobile_hotspot_assertions_watermark', none) -%}
  {%- if watermark is none and execute -%}
    {%- set watermark_query -%}
      select cast(max(data.received_timestamp.max) as varchar)
      from {{ this.database }}.{{ this.schema }}."{{ this.identifier }}$partitions"
    {%- endset -%}
    {%- set watermark = run_query(watermark_query).columns[0].values()[0] -%}
  {%- endif -%}
{%- endif %}

with new_history as (

    -- No watermark means a first run, a full refresh, or an empty table: take
    -- everything.
    select
        pub_key,
        "timestamp",
        block,
        file_ts,
        record_index,
        received_timestamp,
        asserted_hex,
        asserted_lat,
        asserted_lon,
        device_type
    from {{ source('chain', 'mobile_hotspot_history') }}
    {%- if watermark is not none %}
    where received_timestamp
        >= timestamp '{{ watermark }}' - interval '{{ lookback_minutes }}' minute
    {%- endif %}

),

{%- if is_incremental() %}

seeds as (

    -- Each hotspot's latest row already written, for hotspots with new history.
    select
        pub_key,
        "timestamp",
        block,
        file_ts,
        record_index,
        received_timestamp,
        asserted_hex,
        asserted_lat,
        asserted_lon,
        device_type
    from (
        select
            pub_key,
            valid_from as "timestamp",
            block,
            file_ts,
            record_index,
            received_timestamp,
            asserted_hex,
            asserted_lat,
            asserted_lon,
            device_type,
            row_number() over (
                partition by pub_key
                order by valid_from desc, block desc, file_ts desc, record_index desc
            ) as recency_rank
        from {{ this }}
        where pub_key in (select pub_key from new_history)
    )
    where recency_rank = 1

),

{%- endif %}

chain_sequence as (

    select n.*, false as is_written
    from new_history as n
    {%- if is_incremental() %}
    left join seeds as s
        on n.pub_key = s.pub_key
    where
        s.pub_key is null
        or row(n."timestamp", n.block, n.file_ts, n.record_index)
        > row(s."timestamp", s.block, s.file_ts, s.record_index)

    union all

    select s.*, true as is_written
    from seeds as s
    {%- endif %}

),

ordered as (

    select
        *,
        row_number() over chain_order as position,
        lag(asserted_hex) over chain_order as previous_hex,
        lag(device_type) over chain_order as previous_device_type
    from chain_sequence
    window chain_order as (
        partition by pub_key
        order by "timestamp", block, file_ts, record_index
    )

)

select
    pub_key,
    asserted_hex,
    asserted_lat,
    asserted_lon,
    device_type,
    "timestamp" as valid_from,
    block,
    file_ts,
    record_index,
    received_timestamp
from ordered
where
    not is_written
    and (
        position = 1
        or asserted_hex is distinct from previous_hex
        or device_type is distinct from previous_device_type
    )
