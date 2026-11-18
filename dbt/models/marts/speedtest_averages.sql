{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['hotspot_pubkey', 'timestamp'],
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
-- Replaces the per-report average mobile-verifier computed in memory and wrote
-- to `hotspots.speedtest_avgs` / `invalid_speedtest_avgs`. Those tables are no
-- longer written and keep their history; this table starts from the
-- `speedtests` history instead. Differences from the oracle, all deliberate:
--
--   * `timestamp` is the report's time, not when the average was calculated.
--   * Future-dated reports (device `timestamp` after `received_timestamp`) are
--     dropped entirely -- neither averaged nor used as a sample. They are
--     dropped rather than tolerated because they would also break the
--     watermark below.
--   * A late report is folded into every average it belongs to. The oracle
--     averaged a late report with what preceded it but never revisited the
--     later averages that should have included it; this model recomputes them.
--
-- INCREMENTAL SHAPE. Three ranges, easy to conflate:
--
--   1. NEW REPORTS -- received since the last run. Watermark is
--      `max(timestamp)` of this table, the device clock. That is safe only
--      because future-dated reports are filtered: every row here has
--      `timestamp <= received_timestamp <= previous run`, so anything received
--      after the previous run has `received_timestamp` above the watermark.
--      The lookback (`incremental_lookback_minutes`) covers ingest commit
--      skew, as in the macro's header.
--   2. AFFECTED REPORTS -- per hotspot, every report from its earliest new
--      report to `window_hours` after its latest. A new sample can sit in the
--      window of any report up to `window_hours` after it, so those are
--      recomputed and re-merged. Usually that is just the new reports
--      themselves; a late report widens it.
--   3. SAMPLES -- per hotspot, everything from `window_hours` before the
--      earliest affected report. The `received_timestamp` predicate on it is
--      for partition pruning only and never excludes a needed row, because
--      `received_timestamp >= timestamp` for every row that survives the
--      future-dated filter.
--
-- `merge` on (hotspot_pubkey, timestamp) makes the overlap idempotent: a
-- recomputed average replaces its previous row.

{%- set window_hours = var('speedtest_average_window_hours') -%}
{%- set max_samples = var('speedtest_average_max_samples') %}

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

new_reports as (

    select hotspot_pubkey, "timestamp"
    from speedtests
    where {{ incremental_window('received_timestamp', '"timestamp"') }}

),

affected_hotspots as (

    select
        hotspot_pubkey,
        min("timestamp") as first_affected,
        max("timestamp") + interval '{{ window_hours }}' hour as last_affected
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
        inner join affected_hotspots as a
            on
                s.hotspot_pubkey = a.hotspot_pubkey
                and s."timestamp" >= a.first_affected - interval '{{ window_hours }}' hour
                and s."timestamp" <= a.last_affected
        {%- if is_incremental() %}
        where s.received_timestamp >= (
            select min("timestamp") - interval '{{ window_hours }}' hour from new_reports
        )
        {%- endif %}
    )
    where copy_rank = 1

),

reports as (

    select s.hotspot_pubkey, s."timestamp"
    from samples as s
    inner join affected_hotspots as a
        on
            s.hotspot_pubkey = a.hotspot_pubkey
            and s."timestamp" >= a.first_affected
            and s."timestamp" <= a.last_affected

),

windowed as (

    select
        r.hotspot_pubkey,
        r."timestamp",
        s."timestamp" as sample_timestamp,
        s.upload_speed,
        s.download_speed,
        s.latency,
        row_number() over (
            partition by r.hotspot_pubkey, r."timestamp"
            order by s."timestamp" desc
        ) as recency_rank
    from reports as r
    inner join samples as s
        on
            s.hotspot_pubkey = r.hotspot_pubkey
            and s."timestamp" <= r."timestamp"
            and s."timestamp" >= r."timestamp" - interval '{{ window_hours }}' hour

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
