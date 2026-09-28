{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='hotspot_pubkey',
    on_schema_change='append_new_columns',
    schema='hotspots',
    tags=['15m'],
    properties={
      'format': "'PARQUET'"
    }
  )
}}

-- Current carrier enablement per hotspot: the latest report for each
-- hotspot_pubkey, carried through column for column.
--
-- Written to `hotspots`, the same schema as the `enabled_carriers_history` it
-- summarises, so the current-state table sits beside the history a consumer
-- would reach for next. Note the consequence: this project writes into a schema
-- whose raw tables are produced by a pipeline outside this repo, so dbt needs
-- write access somewhere it does not own the inputs.
--
-- Reads the source directly and selects `*`, which is what makes
-- `on_schema_change='append_new_columns'` worth having: a column added upstream
-- reaches this mart on the next run with no edit here. A view enumerating
-- columns in between would defeat that.
--
-- `merge` on hotspot_pubkey. Nothing is deleted and no day is rebuilt: a run
-- upserts one row per hotspot that appears in its window and leaves every other
-- row in the table untouched.
--
-- WHICH MAKES THE WINDOW THE THING THAT SIZES A RUN. The default lookback is 30
-- minutes (`incremental_lookback_minutes`), and it wants to stay in that order
-- of magnitude, because at the `15m` cadence below the window is the only thing
-- deciding how much work happens 96 times a day. 30 minutes means a run reads
-- two cadences of source partitions and upserts only the hotspots that reported
-- in them -- a small slice of the fleet. A days-wide window would instead sweep
-- up nearly every hotspot every quarter hour and rewrite most of this table's
-- data files each time.
--
-- The window only has to cover ingest commit skew, not data lateness: there is
-- no past aggregate here to recompute, only a boundary not to miss a row at.
-- The macro's header spells out why the two are different sizes.
--
-- The `15m` tag is inherited, not measured -- carry it only as long as carrier
-- enablement actually needs quarter-hour freshness, and note that whatever runs
-- this owes the table Iceberg maintenance either way: 96 merges a day is 96
-- snapshots a day, so `expire_snapshots` and `optimize` need to be scheduled
-- against it.
--
-- THE WATERMARK IS `received_timestamp`, THE ORDERING IS `timestamp`. They are
-- answering different questions and the distinction matters:
--
--   * Which rows a run considers is a question about not missing any, so it has
--     to key off the clock the oracle assigns on ingest. `received_timestamp`
--     is monotonic with ingest, so "received since the last run" is exactly the
--     set of new rows -- and it is the table's partition key, so the predicate
--     prunes. Watermarking on the device's `timestamp` instead breaks both
--     halves of that: it prunes nothing (the partition transform is over
--     `received_timestamp`), and because the watermark is global while the
--     grain is per hotspot, one device reporting a far-future timestamp raises
--     it beyond every other hotspot's real timestamps and those hotspots stop
--     being picked up entirely.
--
--   * Which row wins within a hotspot is a question about which report
--     reflects current state, and that is the device's own `timestamp` -- the
--     moment it is describing. Kept as the primary sort for that reason, with
--     `received_timestamp`, `file_ts` and `record_index` behind it so the
--     result is deterministic when a device repeats a timestamp.
--
-- One consequence to know about: a device with a clock set far forward pins its
-- own row, since its bogus timestamp keeps winning the sort. That is a property
-- of ordering on a reported clock, not of the watermark, and it affects that
-- hotspot alone.

with new_records as (

    select *
    from {{ source('hotspots', 'enabled_carriers_history') }}
    where {{ incremental_window('received_timestamp', 'received_timestamp') }}

),

latest_per_key as (

    -- Only the identifying columns, so the row_number does not have to carry
    -- the whole row through the window. `file_ts` + `record_index` identifies a
    -- row uniquely (asserted on the source in sources.yml), which is what makes
    -- joining back for the full record safe rather than a fan-out.
    select
        hotspot_pubkey,
        file_ts,
        record_index,
        row_number() over (
            partition by hotspot_pubkey
            order by "timestamp" desc, received_timestamp desc, file_ts desc, record_index desc
        ) as recency_rank
    from new_records

)

select nr.*
from new_records as nr
inner join latest_per_key as lpk
    on
        nr.hotspot_pubkey = lpk.hotspot_pubkey
        and nr.file_ts = lpk.file_ts
        and nr.record_index = lpk.record_index
        and lpk.recency_rank = 1
