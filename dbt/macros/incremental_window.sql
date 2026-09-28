{#
  Predicate that limits an incremental run to a trailing window of source rows.

  `source_column`    the timestamp column to filter, in the SOURCE.
  `watermark_column` the column in `{{ this }}` holding how far the last run
                     got. Usually the same name; a `date` works as well as a
                     timestamp, which is why the max is cast.

  Three decisions, each avoiding a specific failure:

  1. THE PREDICATE IS OVER THE RAW COLUMN, UNWRAPPED. These tables are
     partitioned on a `day(<timestamp>)` transform, and Trino prunes those
     partitions only for a plain range over the column itself. Wrapping it --
     `cast(ts as date) >= ...`, `date_trunc(...)` -- type-checks, returns
     identical rows, and reads the whole table. Group by a derived day all you
     like; never filter on one.

  2. THE WINDOW IS ANCHORED TO THE TABLE, NOT THE CLOCK. `max(watermark)` from
     the model itself, not `current_date - n`. A run after a two-day outage
     therefore reaches back two days and closes the gap; a clock-anchored window
     would skip it silently and leave a hole nothing ever fills.

  3. IT IS `>=` WITH A LOOKBACK, NOT `>`. A strict `>` on the last value seen
     assumes nothing will ever arrive behind it, which is false for anything
     batch-written: two ingest commits in flight can land slightly out of order,
     and the row on the wrong side of a strict boundary is not late, it is lost.
     The lookback buys margin against that.

  SIZE THE LOOKBACK TO COMMIT SKEW, NOT TO DATA LATENESS, AND CHECK IT AGAINST
  THE MODEL'S CADENCE. The window is how much work every single run does. A
  current-state upsert needs only enough margin to not miss a row at the
  boundary -- minutes -- because it recomputes nothing. A daily rollup is the
  other shape: a late row changes an already-computed day's totals, so it has to
  reach back far enough to rebuild those days, and that is a property of the
  data. Giving the first shape the second shape's window means re-reading and
  re-upserting most of the keyed universe on every run; at a 15-minute cadence a
  3-day window reprocesses each row nearly 300 times. `lookback_minutes`
  defaults from `incremental_lookback_minutes` (dbt_project.yml); a daily model
  should pass its own, e.g. `lookback_minutes=3*24*60`.

  THE CALLER OWES IDEMPOTENCE OVER THE OVERLAP. Decision 3 means every run
  re-reads rows the previous run already processed, so the model has to absorb
  that without double counting. `merge` on a unique key and `delete+insert` on
  whole days both do -- they replace rather than accumulate. `append` does NOT,
  and pairing it with this macro produces duplicates at a rate equal to the
  lookback. The macro cannot check this; it is on you.

  One more property worth knowing: an empty model (first run, or after a manual
  truncate) has no max, the `coalesce` falls through to the epoch, and the run
  reads everything. That is the intent -- rebuild from scratch rather than
  quietly produce nothing -- but it does mean a "small incremental run" can turn
  into a full scan if something emptied the table.
#}
{% macro incremental_window(source_column, watermark_column, lookback_minutes=none) %}
  {%- set minutes = lookback_minutes if lookback_minutes is not none
                    else var('incremental_lookback_minutes') -%}
  {%- if is_incremental() -%}
    {{ source_column }} >= coalesce(
      (select cast(max({{ watermark_column }}) as timestamp(6) with time zone) from {{ this }}),
      timestamp '1970-01-01 00:00:00 UTC'
    ) - interval '{{ minutes }}' minute
  {%- else -%}
    true
  {%- endif -%}
{% endmacro %}


{#
  The UTC calendar day a `timestamp with time zone` falls on.

  Spelled out rather than left to `cast(ts as date)` because that cast resolves
  in the session time zone. The profiles pin it to UTC, but a mart's day
  boundary should not silently depend on a connection property that a
  hand-written query or a future target could set differently.
#}
{% macro utc_day(timestamp_column) %}
  cast({{ timestamp_column }} at time zone 'UTC' as date)
{% endmacro %}
