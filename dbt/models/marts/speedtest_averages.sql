{{
  config(
    materialized='incremental',
    incremental_strategy='append',
    on_schema_change='fail',
    schema='hotspots',
    tags=['1h'],
    properties={
      'format': "'PARQUET'",
      'partitioning': "ARRAY['day(timestamp)']",
      'sorted_by': "ARRAY['hotspot_pubkey', 'timestamp']"
    }
  )
}}

-- Rolling speedtest average per hotspot, one row per speedtest report.
--
-- A row says: as of `timestamp` -- the device-reported time of one report in
-- `hotspots.speedtests` -- this hotspot's last (up to) `max_samples` speedtests
-- within the preceding `window_hours` averaged to these values. The report is
-- always the newest sample in its own window.
--
-- FACTS ONLY. No tier, multiplier or validity: what counts as a good or bad
-- average is the reader's policy, and lives with the reader. `sample_count` is
-- here so a reader can tell an average of one sample from an average of six.
-- The window itself (6 samples / 48 hours, project vars) is the one piece of
-- policy that does live here.
--
-- Runs alongside the per-report average mobile-verifier computes in memory and
-- writes to `hotspots.speedtest_avgs` / `invalid_speedtest_avgs`. The oracle
-- keeps writing those for a transition period so their readers can move over at
-- their own pace; this table is their replacement, built from the `speedtests`
-- history rather than from them. Differences from the oracle, all deliberate:
--
--   * `timestamp` is the report's time, not when the average was calculated.
--   * Future-dated reports (device `timestamp` after `received_timestamp`) are
--     dropped entirely -- neither averaged nor used as a sample. They are
--     dropped rather than tolerated because they would also break the
--     watermark below.
--   * A replayed report is averaged once.
--
-- APPEND-ONLY, SO THE TABLE CAN BE STREAMED. Every run only inserts, and only
-- averages for reports that do not have one yet, so each run commits an Iceberg
-- `append` snapshot holding exactly the new rows. That is what streaming
-- readers follow -- helium-iceberg's stream poller and Spark Structured
-- Streaming read the rows an `append` added and skip `overwrite` snapshots,
-- which is what a `merge` commits whenever it touches an existing row. A row is
-- never rewritten after it is written. The consequence: a late report (one
-- received after reports the device timed later) gets its own average, from
-- the samples before it, but the later averages it would have belonged in keep
-- the values they were written with -- as in the oracle.
--
-- STARTS AT `speedtest_averages_start` (2026-11-01). Only reports from that
-- date get an average; earlier averages live in mobile-verifier's
-- `speedtest_avgs` / `invalid_speedtest_avgs`, which are kept (moved to a
-- deprecated schema) rather than deleted. Samples are not cut off: a report
-- just after the date averages its full window, including speedtests before
-- it. The date also bounds the first build, and any `--full-refresh`, to one
-- query over the reports since then; move it forward if that ever grows too
-- large for one run.
--
-- INCREMENTAL SHAPE. Three ranges:
--
--   1. CANDIDATE REPORTS -- received since the last run. The watermark is
--      `max(timestamp)` of this table, the device clock. That is safe only
--      because future-dated reports are filtered: every row here has
--      `timestamp <= received_timestamp <= previous run`, so anything received
--      after the previous run has `received_timestamp` above the watermark.
--      The lookback (`incremental_lookback_minutes`) covers ingest commit
--      skew, as in the `incremental_window` macro's header.
--   2. NEW REPORTS -- candidates without a row here yet. The lookback re-reads
--      reports the previous run already averaged; they are dropped here, which
--      is what keeps the run an insert of new rows only.
--   3. SAMPLES -- per hotspot, everything from `window_hours` before its
--      earliest new report up to its latest. The `received_timestamp`
--      predicate on it is for partition pruning only and never excludes a
--      needed row, because `received_timestamp >= timestamp` for every row that
--      survives the future-dated filter.
--
-- THE WATERMARK IS A LITERAL, looked up once at compile time, not the
-- `incremental_window` macro's `(select max(...) from this)`. Trino inlines a
-- CTE at every reference, so a subquery watermark here was evaluated at every
-- reference, each a full scan of this table -- measured against production at
-- 1.7 minutes for a run whose real work took seconds, and growing with the
-- table. The lookup reads Iceberg's `$partitions` metadata (per-file column
-- stats) rather than the data, and as a literal the `received_timestamp`
-- predicate prunes partitions at planning time instead of waiting on a dynamic
-- filter. Rows are never deleted, so file stats cannot overstate the max.
--
-- Unit tests cannot reach `run_query` -- it would read the real table, not the
-- `this` fixture -- so they set `speedtest_averages_watermark` instead.

{%- set window_hours = var('speedtest_average_window_hours') -%}
{%- set max_samples = var('speedtest_average_max_samples') -%}
{%- set lookback_minutes = var('incremental_lookback_minutes') -%}
{%- set start = "timestamp '" ~ var('speedtest_averages_start') ~ " 00:00:00 UTC'" -%}

{%- set watermark = none -%}
{%- if is_incremental() -%}
  {%- set watermark = var('speedtest_averages_watermark', none) -%}
  {%- if watermark is none and execute -%}
    {%- set watermark_query -%}
      select cast(max(data."timestamp".max) as varchar)
      from {{ this.database }}.{{ this.schema }}."{{ this.identifier }}$partitions"
    {%- endset -%}
    {%- set watermark = run_query(watermark_query).columns[0].values()[0] -%}
  {%- endif -%}
{%- endif %}

with speedtests as (

    select
        hotspot_pubkey,
        "timestamp",
        received_timestamp,
        upload_speed,
        download_speed,
        latency
    from {{ source('hotspots', 'speedtests') }}
    where "timestamp" <= received_timestamp

),

candidate_reports as (

    -- Reports from `speedtest_averages_start` on. No watermark means a first
    -- run, a full refresh, or an empty table: take every one of those. The
    -- `received_timestamp` bound prunes partitions and excludes nothing,
    -- since a report is never received before its own time.
    select distinct hotspot_pubkey, "timestamp"
    from speedtests
    where
        "timestamp" >= {{ start }}
        and received_timestamp >= {{ start }}
        {%- if watermark is not none %}
        and received_timestamp
            >= timestamp '{{ watermark }}' - interval '{{ lookback_minutes }}' minute
        {%- endif %}

),

{%- if is_incremental() %}

already_averaged as (

    -- `this` on the probe side of a semi-join, so Trino's dynamic filters on
    -- the candidates' keys prune its partitions rather than reading it all.
    select hotspot_pubkey, "timestamp"
    from {{ this }}
    where
        hotspot_pubkey in (select hotspot_pubkey from candidate_reports)
        and "timestamp" >= (select min("timestamp") from candidate_reports)

),

{%- endif %}

new_reports as (

    select c.hotspot_pubkey, c."timestamp"
    from candidate_reports as c
    {%- if is_incremental() %}
    left join already_averaged as a
        on
            c.hotspot_pubkey = a.hotspot_pubkey
            and c."timestamp" = a."timestamp"
    where a.hotspot_pubkey is null
    {%- endif %}

),

sample_ranges as (

    select
        hotspot_pubkey,
        min("timestamp") - interval '{{ window_hours }}' hour as first_sample,
        max("timestamp") as last_sample
    from new_reports
    group by hotspot_pubkey

),

samples as (

    -- One row per (hotspot, timestamp). mobile-verifier stores a replayed
    -- report again, and the oracle never counted one twice; the earliest
    -- received copy wins, with the measurements as tie-breakers so the pick is
    -- deterministic.
    select hotspot_pubkey, "timestamp", upload_speed, download_speed, latency
    from (
        select
            s.hotspot_pubkey,
            s."timestamp",
            s.upload_speed,
            s.download_speed,
            s.latency,
            row_number() over (
                partition by s.hotspot_pubkey, s."timestamp"
                order by s.received_timestamp, s.upload_speed, s.download_speed, s.latency
            ) as copy_rank
        from speedtests as s
        inner join sample_ranges as r
            on
                s.hotspot_pubkey = r.hotspot_pubkey
                and s."timestamp" >= r.first_sample
                and s."timestamp" <= r.last_sample
        -- Samples may predate the start date: a report just after it still
        -- averages its whole window. Both bounds only prune partitions.
        where
            s.received_timestamp >= {{ start }} - interval '{{ window_hours }}' hour
            {%- if watermark is not none %}
            and s.received_timestamp >= (
                select min(first_sample) from sample_ranges
            )
            {%- endif %}
    )
    where copy_rank = 1

),

windowed as (

    select
        n.hotspot_pubkey,
        n."timestamp",
        s."timestamp" as sample_timestamp,
        s.upload_speed,
        s.download_speed,
        s.latency,
        row_number() over (
            partition by n.hotspot_pubkey, n."timestamp"
            order by s."timestamp" desc
        ) as recency_rank
    from new_reports as n
    inner join samples as s
        on
            s.hotspot_pubkey = n.hotspot_pubkey
            and s."timestamp" <= n."timestamp"
            and s."timestamp" >= n."timestamp" - interval '{{ window_hours }}' hour

)

-- bigint / bigint truncates in Trino, matching the integer division the oracle
-- used, so carried-over readers see the same numbers.
select
    hotspot_pubkey,
    "timestamp",
    sum(upload_speed) / count(*) as upload_speed_avg_bps,
    sum(download_speed) / count(*) as download_speed_avg_bps,
    sum(latency) / count(*) as latency_avg_ms,
    count(*) as sample_count,
    array_agg(
        cast(
            row(upload_speed, download_speed, latency, sample_timestamp)
            as row(
                upload_speed_bps bigint,
                download_speed_bps bigint,
                latency_ms bigint,
                "timestamp" timestamp(6) with time zone
            )
        )
        order by sample_timestamp desc
    ) as speedtests
from windowed
where recency_rank <= {{ max_samples }}
group by hotspot_pubkey, "timestamp"
