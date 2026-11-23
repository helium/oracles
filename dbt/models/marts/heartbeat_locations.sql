{{
  config(
    materialized='incremental',
    incremental_strategy='append',
    on_schema_change='fail',
    schema='hotspots',
    tags=['1h'],
    properties={
      'format': "'PARQUET'",
      'partitioning': "ARRAY['day(received_timestamp)']",
      'sorted_by': "ARRAY['hotspot_pubkey', 'received_timestamp']"
    }
  )
}}

-- Each valid heartbeat, as the hotspot reported it, beside the assertion in
-- effect when it arrived and the distance between them. One row per
-- heartbeat in `hotspots.heartbeat_reports`.
--
-- FACTS ONLY. No fix-age limit, no 300 m / 400 m bands, no trust score: those
-- are eligibility policy and live in the models that read this one. Rows whose
-- location the device asserted (`location_source = 'asserted'`) are kept;
-- their distance is about 0 by construction, and a reader decides what that
-- is worth.
--
-- THE ASSERTION IS THE ONE IN EFFECT AT `received_timestamp`. Each row of
-- `chain.mobile_hotspot_assertions` holds until the hotspot's next one, so its
-- end is `lead(valid_from)` in the order that model writes them (valid_from,
-- block, file_ts, record_index), and the periods tile each hotspot's history
-- with no gaps or overlaps: a heartbeat matches at most one. It is a LEFT
-- join: a heartbeat with no period -- the hotspot's chain update is not in the
-- history yet -- is kept with null asserted fields and a null distance, not
-- dropped, so the gap is visible.
--
-- DISTANCE MATCHES mobile-verifier EXACTLY. It is measured from the
-- heartbeat's centered point (the center of its resolution-12 H3 cell, which
-- the verifier records as `centered_lat` / `centered_lon` because Trino has no
-- H3 functions) to the asserted cell's center (from icicle), with h3o's
-- haversine and h3o's earth radius, rounded to whole meters -- the formula
-- behind the verifier's `distance_to_asserted`. Trino's
-- `great_circle_distance` uses a slightly different radius (6371.01 km), which
-- is enough to flip a rounding at the 300 m and 400 m edges.
--
-- APPEND-ONLY, SO THE TABLE CAN BE STREAMED. Every run only inserts, and only
-- heartbeats not measured yet, so each run commits an Iceberg `append`
-- snapshot holding exactly the new rows -- what helium-iceberg's stream poller
-- and Spark Structured Streaming follow; they skip the `overwrite` snapshots a
-- `merge` commits. A run with nothing new commits no snapshot.
--
-- INCREMENTAL ON `received_timestamp`, the oracle's clock and this table's
-- partition key. Each run reads from the watermark minus the shared
-- `incremental_lookback_minutes` (commit skew) and drops heartbeats already
-- here, keyed by (hotspot_pubkey, received_timestamp) -- the verifier's own
-- identity for a heartbeat. Should one ever be stored twice, one copy is kept.
--
-- A HEARTBEAT IS MEASURED ONLY ONCE IT HAS SETTLED: received at least
-- `heartbeat_locations_settle_minutes` (default 60) before the run. A row is
-- never rewritten, so it has to be right when it is written, and icicle can
-- land a reassertion a little after the heartbeats that follow it. Waiting
-- gives the chain history time to catch up. The price is that the table trails
-- ingest by that much. A chain update later than the settle time leaves the
-- heartbeats before it measured against the previous assertion (or none);
-- only `--full-refresh` re-measures them.
--
-- THE WATERMARK IS A LITERAL, read once at compile time from the table's
-- `$partitions` metadata, as in `speedtest_averages` (see its header): a
-- `(select max(...) from this)` subquery would scan this table, and a literal
-- prunes the source's partitions at planning time. Unit tests set
-- `heartbeat_locations_watermark` instead, since `run_query` would read the
-- real table rather than the `this` fixture.
--
-- NO BACKFILL. `heartbeat_reports` starts when the verifier starts writing it;
-- older heartbeats were stored with carried-forward locations in
-- `hotspots.heartbeats` and cannot be recovered as reported.

{%- set lookback_minutes = var('incremental_lookback_minutes') -%}
{#- Unit tests pin "now" with `heartbeat_locations_settled_before`. -#}
{%- set settled_before = var('heartbeat_locations_settled_before', none) -%}
{%- if settled_before is none -%}
  {%- set settled_before_sql = "current_timestamp - interval '" ~ var('heartbeat_locations_settle_minutes') ~ "' minute" -%}
{%- else -%}
  {%- set settled_before_sql = "timestamp '" ~ settled_before ~ "'" -%}
{%- endif -%}
{#- h3o::EARTH_RADIUS_KM -#}
{%- set earth_radius_km = 6371.007180918475 -%}

{%- set watermark = none -%}
{%- if is_incremental() -%}
  {%- set watermark = var('heartbeat_locations_watermark', none) -%}
  {%- if watermark is none and execute -%}
    {%- set watermark_query -%}
      select cast(max(data.received_timestamp.max) as varchar)
      from {{ this.database }}.{{ this.schema }}."{{ this.identifier }}$partitions"
    {%- endset -%}
    {%- set watermark = run_query(watermark_query).columns[0].values()[0] -%}
  {%- endif -%}
{%- endif %}

with reports as (

    select
        hotspot_pubkey,
        received_timestamp,
        heartbeat_timestamp,
        lat,
        lon,
        centered_lat,
        centered_lon,
        location_validation_timestamp,
        location_source,
        row_number() over (
            partition by hotspot_pubkey, received_timestamp
            order by heartbeat_timestamp, lat, lon
        ) as copy_number
    from {{ source('hotspots', 'heartbeat_reports') }}
    where
        received_timestamp < {{ settled_before_sql }}
        -- No watermark means a first run, a full refresh, or an empty table:
        -- take everything that has settled.
        {%- if watermark is not none %}
        and received_timestamp
            >= timestamp '{{ watermark }}' - interval '{{ lookback_minutes }}' minute
        {%- endif %}

),

{%- if is_incremental() %}

already_measured as (

    -- The lookback's overlap: heartbeats a previous run already wrote. The
    -- same literal bound as `reports`, so only the overlap's partitions are
    -- read.
    select hotspot_pubkey, received_timestamp
    from {{ this }}
    {%- if watermark is not none %}
    where received_timestamp
        >= timestamp '{{ watermark }}' - interval '{{ lookback_minutes }}' minute
    {%- endif %}

),

{%- endif %}

new_reports as (

    select r.*
    from reports as r
    {%- if is_incremental() %}
    left join already_measured as m
        on
            r.hotspot_pubkey = m.hotspot_pubkey
            and r.received_timestamp = m.received_timestamp
    {%- endif %}
    where
        r.copy_number = 1
        {%- if is_incremental() %}
        and m.hotspot_pubkey is null
        {%- endif %}

),

periods as (

    select
        pub_key,
        asserted_hex,
        asserted_lat,
        asserted_lon,
        valid_from,
        lead(valid_from) over (
            partition by pub_key
            order by valid_from, block, file_ts, record_index
        ) as valid_to
    from {{ ref('mobile_hotspot_assertions') }}

),

located as (

    select
        r.received_timestamp,
        r.hotspot_pubkey,
        r.heartbeat_timestamp,
        r.lat as heartbeat_lat,
        r.lon as heartbeat_lon,
        r.location_validation_timestamp,
        r.location_source,
        p.asserted_hex,
        p.asserted_lat,
        p.asserted_lon,
        r.centered_lat,
        r.centered_lon
    from new_reports as r
    left join periods as p
        on
            p.pub_key = r.hotspot_pubkey
            and p.valid_from <= r.received_timestamp
            and (p.valid_to is null or r.received_timestamp < p.valid_to)

),

haversine as (

    -- h3o's `LatLng::distance_rads`, from the asserted center to the
    -- heartbeat's centered point: a = sin²(Δlat/2) + cos φ1 · cos φ2 · sin²(Δlon/2).
    -- Null when either point is.
    select
        *,
        power(sin((radians(centered_lat) - radians(asserted_lat)) / 2), 2)
        + cos(radians(asserted_lat)) * cos(radians(centered_lat))
        * power(sin((radians(centered_lon) - radians(asserted_lon)) / 2), 2)
            as haversine_a
    from located

)

select
    received_timestamp,
    hotspot_pubkey,
    heartbeat_timestamp,
    heartbeat_lat,
    heartbeat_lon,
    location_validation_timestamp,
    location_source,
    asserted_hex,
    asserted_lat,
    asserted_lon,
    cast(
        round(
            -- In h3o's order: radians, then km, then meters.
            2 * atan2(sqrt(haversine_a), sqrt(1 - haversine_a))
            * {{ earth_radius_km }} * 1000
        ) as bigint
    ) as distance_to_asserted
from haversine
