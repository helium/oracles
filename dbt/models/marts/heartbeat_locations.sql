{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['hotspot_pubkey', 'received_timestamp'],
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
-- THE ASSERTION IS THE ONE IN EFFECT AT `received_timestamp`, by a range join
-- to `chain.mobile_hotspot_assertions`, whose periods tile each hotspot's
-- history with no gaps or overlaps, so a heartbeat matches at most one. It is a
-- LEFT join: a heartbeat with no period -- the hotspot's chain update is not in
-- the history yet -- is kept with null asserted fields and a null distance,
-- not dropped, so the gap is visible.
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
-- INCREMENTAL ON `received_timestamp`, the oracle's clock and this table's
-- partition key. The lookback is its own var,
-- `heartbeat_locations_lookback_minutes`, hours rather than the shared 30
-- minutes: icicle can land a reassertion after the heartbeats that follow it
-- were already measured, and each run re-measures the lookback against the
-- current periods so those rows are corrected once the chain catches up. A
-- chain update later than the lookback is fixed only by `--full-refresh`.
--
-- `merge` on (hotspot_pubkey, received_timestamp) makes the overlap
-- idempotent. The key is the verifier's own identity for a heartbeat; should
-- one ever be stored twice, one copy is kept so the merge source stays
-- unique.
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

{%- set lookback_minutes = var('heartbeat_locations_lookback_minutes') -%}
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
    -- No watermark means a first run, a full refresh, or an empty table: take
    -- everything.
    {%- if watermark is not none %}
    where received_timestamp
        >= timestamp '{{ watermark }}' - interval '{{ lookback_minutes }}' minute
    {%- endif %}

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
        a.asserted_hex,
        a.asserted_lat,
        a.asserted_lon,
        r.centered_lat,
        r.centered_lon
    from reports as r
    left join {{ ref('mobile_hotspot_assertions') }} as a
        on
            a.pub_key = r.hotspot_pubkey
            and a.valid_from <= r.received_timestamp
            and (a.valid_to is null or r.received_timestamp < a.valid_to)
    where r.copy_number = 1

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
