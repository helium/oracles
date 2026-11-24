{{
  config(
    materialized='table',
    on_table_exists='replace',
    schema='hotspots',
    tags=['1h'],
    properties={
      'format': "'PARQUET'",
      'sorted_by': "ARRAY['hotspot_pubkey']"
    }
  )
}}

-- Current carrier offload eligibility: one row per asserted wifi hotspot, its
-- latest row in `offload_eligibility_history` plus whether its latest speedtest
-- average meets the bar for submitting a new hotspot.
--
-- A CURRENT-STATE TABLE, REBUILT EACH RUN. It is a snapshot to look things up
-- in, not a stream: every run replaces it in one Iceberg commit. Follow
-- `offload_eligibility_history` for changes.
--
-- `meets_new_hotspot_bar` uses the `applies_to = 'new'` floor in effect now and
-- an average from the last 48 hours (`speedtest_average_window_hours`) with at
-- least that floor's `min_sample_count` speedtests. It is a lookup for
-- submissions; nothing removes offload by it.

{%- set window_hours = var('speedtest_average_window_hours') %}

with hotspots as (

    select pub_key as hotspot_pubkey
    from (
        select
            pub_key,
            asserted_hex,
            device_type,
            row_number() over (
                partition by pub_key
                order by valid_from desc, block desc, file_ts desc, record_index desc
            ) as recency_rank
        from {{ ref('mobile_hotspot_assertions') }}
    )
    where
        recency_rank = 1
        and asserted_hex is not null
        and device_type in ('WIFI_INDOOR', 'WIFI_OUTDOOR')

),

latest_status as (

    select *
    from (
        select
            *,
            row_number() over (partition by hotspot_pubkey order by changed_at desc) as recency_rank
        from {{ ref('offload_eligibility_history') }}
    )
    where recency_rank = 1

),

new_hotspot_floor as (

    select
        min_download_mbps * 125000 as min_download_bps,
        min_upload_mbps * 125000 as min_upload_bps,
        max_latency_ms,
        min_sample_count
    from {{ ref('offload_speedtest_floors') }}
    where
        applies_to = 'new'
        and with_timezone(cast(effective_from as timestamp(6)), 'UTC') <= current_timestamp
    order by effective_from desc
    limit 1

),

latest_average as (

    select *
    from (
        select
            hotspot_pubkey,
            sample_count,
            download_speed_avg_bps,
            upload_speed_avg_bps,
            latency_avg_ms,
            row_number() over (partition by hotspot_pubkey order by "timestamp" desc) as recency_rank
        from {{ ref('speedtest_averages') }}
        where "timestamp" > current_timestamp - interval '{{ window_hours }}' hour
    )
    where recency_rank = 1

)

select
    h.hotspot_pubkey,
    s.eligible,
    s.reasons,
    s.changed_at as since,
    -- The history row's other context is as of `since`; only the low run's
    -- start is still true now, and only while location is failing.
    case when contains(s.reasons, 'location') then s.location_low_since end as location_low_since,
    coalesce(
        a.sample_count >= f.min_sample_count
        and a.download_speed_avg_bps >= f.min_download_bps
        and a.upload_speed_avg_bps >= f.min_upload_bps
        and a.latency_avg_ms <= f.max_latency_ms,
        false
    ) as meets_new_hotspot_bar
from hotspots as h
inner join latest_status as s
    on h.hotspot_pubkey = s.hotspot_pubkey
left join latest_average as a
    on h.hotspot_pubkey = a.hotspot_pubkey
left join new_hotspot_floor as f
    on true
