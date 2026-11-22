{{
  config(
    materialized='incremental',
    incremental_strategy='append',
    on_schema_change='fail',
    schema='hotspots',
    tags=['1h'],
    properties={
      'format': "'PARQUET'",
      'partitioning': "ARRAY['day(changed_at)']",
      'sorted_by': "ARRAY['hotspot_pubkey', 'changed_at']"
    }
  )
}}

-- Carrier offload eligibility over time: one row each time a hotspot's
-- failing reasons change, or a location low run starts or ends.
--
-- A hotspot is eligible at time t unless one of these holds, and every one
-- that holds is listed in `reasons`:
--
--   no_heartbeats        no valid heartbeat in the `max_hours_silent` hours
--                        before t.
--   location             its heartbeats have failed the location check
--                        without a pass for `max_hours_below` hours. A
--                        heartbeat passes when its newest fix is at most
--                        `max_fix_age_hours` old and within `max_distance_m` of
--                        the asserted location. Time without heartbeats counts:
--                        only a passing heartbeat ends a low run.
--   no_recent_speedtest  no speedtest average in the 48 hours before t
--                        (`speedtest_average_window_hours`), or one built from
--                        fewer than `min_sample_count` speedtests.
--   below_floor          the latest average misses the speedtest floor in
--                        effect at t: download or upload below it, or latency
--                        above it. Floors are inclusive.
--   not_covered          the hotspot is no longer an asserted wifi hotspot
--                        (see WHO). Always alone.
--
-- The thresholds come from the seeds `offload_heartbeat_rules` and
-- `offload_speedtest_floors` (`applies_to = 'active'`), each row in effect from
-- 00:00 UTC on its `effective_from` until the next. A moment is judged by the
-- rules in effect then; a heartbeat's fix age and distance by the rules in
-- effect when it arrived. Before the first active floor no speedtest reason
-- applies. A run fails rather than judge with an empty rules table: `dbt seed`
-- reloads a seed by dropping and refilling it, and a run caught in between
-- would otherwise append rows judged without the speedtest floors.
--
-- WHO. Every hotspot whose current assertion is WIFI_INDOOR or WIFI_OUTDOOR
-- with an asserted cell, from `chain.mobile_hotspot_assertions`. A hotspot that
-- never heartbeats is listed as failing `no_heartbeats`, not left out. A
-- hotspot reasserted out of coverage (another device type, or no cell) gets a
-- final `not_covered` row, so a stream reader doesn't keep its last status as
-- current; if it comes back it is judged afresh from its new assertion.
--
-- CARRIED FIXES. Fixes arrive about every 12 hours and heartbeats every 15
-- minutes, so most heartbeats carry no fix of their own. Such a heartbeat is
-- judged by the hotspot's newest earlier fix and that fix's distance, but only
-- a fix made under the same asserted cell: after a reassertion the hotspot
-- needs a fresh fix before it passes. A hotspot that moves away and back to the
-- same cell (A, B, A) can reuse a fix from its first time at A, because
-- `heartbeat_locations` records the cell, not the assertion period.
--
-- STATUS CHANGES BETWEEN DATA POINTS. A hotspot can start failing with no new
-- row arriving: silence runs out, a low run reaches its limit, an average gets
-- too old, or a rule takes effect. So each hotspot is judged at its data points
-- AND at those moments, each limit computed under every rule row it could fall
-- under (a limit is only a moment if it lands while that row is in effect).
--
-- APPEND-ONLY, SO THE TABLE CAN BE STREAMED. Each run judges the interval
-- (watermark, settled_until] and appends only changes:
--
--   * `settled_until` is where both inputs are complete: the newer input is cut
--     back to the older. For heartbeats that is the newest heartbeat location,
--     complete because `heartbeat_locations` waits an hour after ingest
--     (`heartbeat_locations_settle_minutes`), longer than ingest's commit skew.
--   * The watermark is this table's newest `changed_at`. Moments between it and
--     the previous run's `settled_until` are judged again; inputs are
--     append-only, so they give the same answer and write nothing.
--   * Judging a moment needs history before it. Each run judges heartbeats
--     from `history_hours` before the watermark: the longest fix age plus the
--     longest silence or low-run limit in the rules, plus an hour (97 hours
--     with today's rules). Their fixes are read from a further fix age back,
--     so every heartbeat it judges sees the fix it would see in one long run.
--     Speedtest averages are read from 49 hours before the watermark.
--   * What the window cannot hold, each hotspot's latest written row carries:
--     rows are written when a low run starts or ends, not only when reasons
--     change, so the latest row knows whether a low run is open and since
--     when, and when the last heartbeat was. A low run older than the window,
--     or a silence longer than it, continues from that row.
--   * Judging starts `history_hours` after the first heartbeat location, so
--     the first answers don't rest on missing history. Each hotspot is judged
--     from then, or from its first assertion if later, so a hotspot with no
--     data still gets a row, at a moment that doesn't depend on when runs
--     happen.
--
-- WHAT DEPENDS ON RUN TIMING. Changes that only become known after the moment
-- they belong to was judged are recorded at the start of the next run's
-- interval (one millisecond after the watermark), not at their own time: a
-- hotspot whose assertion reaches the assertions table late, a hotspot leaving
-- or rejoining coverage likewise, and a speedtest average that arrives late
-- (its moment's row stands). A rebuild from scratch would place them at their
-- own time, and leaves out hotspots no longer covered.
--
-- COST. Trino inlines a CTE at every reference, so each input is read once:
-- every event family is built from a single pass over its source with
-- `unnest`, and the per-heartbeat windows share one partitioning.
--
-- The watermark, `settled_until`, the first heartbeat and the rules' longest
-- limits are read once at compile time, the first three from Iceberg metadata
-- as in `speedtest_averages`. Unit tests set `offload_eligibility_watermark`,
-- `offload_eligibility_settled_until`, `offload_eligibility_data_start` and
-- `offload_eligibility_rule_hours` instead.

{%- set window_hours = var('speedtest_average_window_hours') -%}
{%- set speedtest_history_hours = window_hours + 1 -%}

{#- The rules' longest fix age, and longest silence or low-run limit, size the
    window. Read from the seed so a new rule row can't silently outgrow it. -#}
{%- set rule_hours = var('offload_eligibility_rule_hours', none) -%}
{%- if rule_hours is none -%}
  {%- if execute -%}
    {%- set rule_hours_query -%}
      select max(max_fix_age_hours), max(greatest(max_hours_below, max_hours_silent))
      from {{ ref('offload_heartbeat_rules') }}
    {%- endset -%}
    {%- set rule_row = run_query(rule_hours_query).rows[0] -%}
    {%- if rule_row[0] is none or rule_row[1] is none -%}
      {{ exceptions.raise_compiler_error("offload_heartbeat_rules is empty; run `dbt seed` before this model") }}
    {%- endif -%}
    {%- set rule_hours = {'max_fix_age': rule_row[0] | int, 'lookback': rule_row[1] | int} -%}
  {%- else -%}
    {%- set rule_hours = {'max_fix_age': 0, 'lookback': 0} -%}
  {%- endif -%}
{%- endif -%}
{%- set fix_age_hours = rule_hours['max_fix_age'] -%}
{%- set history_hours = fix_age_hours + rule_hours['lookback'] + 1 -%}

{%- set ts = 'timestamp(6) with time zone' -%}
{%- set heartbeat_state = 'row(passes boolean, low_since ' ~ ts ~ ')' -%}
{%- set average_state = 'row(at ' ~ ts ~ ', sample_count bigint, download_bps bigint, upload_bps bigint, latency_ms bigint)' -%}
{%- set event = 'row(at ' ~ ts ~ ', heartbeat_at ' ~ ts ~ ', heartbeat ' ~ heartbeat_state ~ ', average ' ~ average_state ~ ')' -%}
{%- set status = 'row(reasons array(varchar), location_low_since ' ~ ts ~ ')' -%}

{%- set locations = ref('heartbeat_locations') -%}
{%- set averages = ref('speedtest_averages') -%}

{%- set watermark = none -%}
{%- if is_incremental() -%}
  {%- set watermark = var('offload_eligibility_watermark', none) -%}
  {%- if watermark is none and execute -%}
    {%- set watermark_query -%}
      select cast(max(data.changed_at.max) as varchar)
      from {{ this.database }}.{{ this.schema }}."{{ this.identifier }}$partitions"
    {%- endset -%}
    {%- set watermark = run_query(watermark_query).columns[0].values()[0] -%}
  {%- endif -%}
{%- endif -%}

{%- set settled_until = var('offload_eligibility_settled_until', none) -%}
{%- set data_start = var('offload_eligibility_data_start', none) -%}
{%- if execute and (settled_until is none or data_start is none) -%}
  {%- set bounds_query -%}
    select
      cast(
        case
          when s.newest is null then h.newest
          else least(h.newest, s.newest)
        end as varchar
      ),
      cast(h.oldest as varchar)
    from (
      select max(data.received_timestamp.max) as newest, min(data.received_timestamp.min) as oldest
      from {{ locations.database }}.{{ locations.schema }}."{{ locations.identifier }}$partitions"
    ) as h
    cross join (
      select max(data."timestamp".max) as newest
      from {{ averages.database }}.{{ averages.schema }}."{{ averages.identifier }}$partitions"
    ) as s
  {%- endset -%}
  {%- set bounds = run_query(bounds_query) -%}
  {%- if settled_until is none -%}
    {%- set settled_until = bounds.columns[0].values()[0] -%}
  {%- endif -%}
  {%- if data_start is none -%}
    {%- set data_start = bounds.columns[1].values()[0] -%}
  {%- endif -%}
{%- endif -%}

{#- The interval this run judges: (zone_start, zone_end], or [zone_start,
    zone_end] when nothing has been written yet. -#}
{%- set resuming = watermark is not none -%}
{%- if resuming -%}
  {%- set zone_start = "timestamp '" ~ watermark ~ "'" -%}
{%- elif data_start is not none -%}
  {%- set zone_start = "timestamp '" ~ data_start ~ "' + interval '" ~ history_hours ~ "' hour" -%}
{%- else -%}
  {%- set zone_start = none -%}
{%- endif -%}
{%- set zone_end = "timestamp '" ~ settled_until ~ "'" if settled_until is not none else none -%}
{%- set has_zone = zone_start is not none and zone_end is not none -%}
{#- The first moment ever judged: every hotspot's status starts there, or at its
    first assertion if later, whichever run covers that moment. -#}
{%- set first_judged = "timestamp '" ~ data_start ~ "' + interval '" ~ history_hours ~ "' hour" if data_start is not none else none -%}
{#- The first moment of a resumed run's interval: where it records what it
    learns late. -#}
{%- set zone_opening = zone_start ~ " + interval '0.001' second" if resuming else none -%}
{%- set incremental = is_incremental() %}

with heartbeat_rules as (

    select
        with_timezone(cast(effective_from as timestamp(6)), 'UTC') as valid_from,
        lead(with_timezone(cast(effective_from as timestamp(6)), 'UTC'))
            over (order by effective_from) as valid_to,
        max_fix_age_hours,
        max_distance_m,
        max_hours_below,
        max_hours_silent
    from {{ ref('offload_heartbeat_rules') }}

),

rule_list as (

    -- Every heartbeat rule row in one array, so a limit can be computed under
    -- each of them without multiplying rows.
    select array_agg(
        cast(
            row(valid_from, valid_to, max_hours_below, max_hours_silent)
            as row(valid_from {{ ts }}, valid_to {{ ts }}, max_hours_below bigint, max_hours_silent bigint)
        )
    ) as rules
    from heartbeat_rules

),

floors as (

    select
        with_timezone(cast(effective_from as timestamp(6)), 'UTC') as valid_from,
        lead(with_timezone(cast(effective_from as timestamp(6)), 'UTC'))
            over (order by effective_from) as valid_to,
        min_download_mbps * 125000 as min_download_bps,
        min_upload_mbps * 125000 as min_upload_bps,
        max_latency_ms,
        min_sample_count
    from {{ ref('offload_speedtest_floors') }}
    where applies_to = 'active'

),

rules_present as (

    -- `dbt seed` drops a seed's table, recreates it and refills it. A run that
    -- reads one in between sees no rows; fail rather than judge without them.
    -- Every judgement joins this, so no row is written unless it held.
    select
        case
            when (select count(*) from {{ ref('offload_heartbeat_rules') }}) > 0
                and (select count(*) from {{ ref('offload_speedtest_floors') }}) > 0
                then true
            else fail('offload eligibility rules tables are empty (a dbt seed reload in progress?)')
        end as ok

),

assertions as (

    -- Each hotspot's current assertion, whether that puts it in scope, and
    -- when it was first asserted.
    select
        pub_key as hotspot_pubkey,
        valid_from as asserted_from,
        first_asserted,
        coalesce(asserted_hex is not null and device_type in ('WIFI_INDOOR', 'WIFI_OUTDOOR'), false) as covered
    from (
        select
            pub_key,
            valid_from,
            asserted_hex,
            device_type,
            min(valid_from) over (partition by pub_key) as first_asserted,
            row_number() over (
                partition by pub_key
                order by valid_from desc, block desc, file_ts desc, record_index desc
            ) as recency_rank
        from {{ ref('mobile_hotspot_assertions') }}
    )
    where recency_rank = 1

),

hotspots as (

    -- Asserted wifi hotspots, by their current assertion.
    select hotspot_pubkey, asserted_from, first_asserted
    from assertions
    where covered

),

{%- if incremental %}

written as (

    -- Each hotspot's latest row: what new judgements are compared against, and
    -- the state a run's window cannot see (an open low run, a last heartbeat
    -- older than the window, having left coverage).
    select
        hotspot_pubkey,
        max(changed_at) as changed_at,
        max_by(cast(row(reasons, location_low_since) as {{ status }}), changed_at) as status,
        max_by(last_heartbeat_at, changed_at) as last_heartbeat_at,
        contains(max_by(reasons, changed_at), 'not_covered') as not_covered
    from {{ this }}
    group by hotspot_pubkey

),

{%- endif %}

judged_heartbeats as (

    -- Each heartbeat in the window, passing or failing the location check by
    -- its newest fix under the same cell and the rules in effect when it came.
    -- Fixes are read from a fix age before the window, so the window's first
    -- heartbeats are judged as they would be with all of history.
    select
        b.hotspot_pubkey,
        b.at,
        coalesce(
            b.fix.distance_m <= r.max_distance_m
            and b.fix.fixed_at >= b.at - r.max_fix_age_hours * interval '1' hour,
            false
        ) as passes
    from (
        select
            l.hotspot_pubkey,
            l.received_timestamp as at,
            last_value(
                case
                    when l.location_validation_timestamp is not null
                        then cast(
                            row(l.location_validation_timestamp, l.distance_to_asserted)
                            as row(fixed_at {{ ts }}, distance_m bigint)
                        )
                end
            ) ignore nulls over (
                partition by l.hotspot_pubkey, l.asserted_hex
                order by l.received_timestamp
                rows between unbounded preceding and current row
            ) as fix
        from {{ locations }} as l
        inner join hotspots as h
            on l.hotspot_pubkey = h.hotspot_pubkey
        where
            {%- if has_zone %}
            l.received_timestamp >= {{ zone_start }} - interval '{{ history_hours + fix_age_hours }}' hour
            and l.received_timestamp <= {{ zone_end }}
            {%- else %}
            false
            {%- endif %}
    ) as b
    inner join heartbeat_rules as r
        on b.at >= r.valid_from and (r.valid_to is null or b.at < r.valid_to)
    {%- if has_zone %}
    where b.at >= {{ zone_start }} - interval '{{ history_hours }}' hour
    {%- endif %}

),

low_runs as (

    -- Where each failing heartbeat's low run began. A run starts at a failure
    -- after a pass, or at the window's first failure; one with no pass before
    -- it in the window may have begun earlier, which the latest written row
    -- records.
    select
        r.hotspot_pubkey,
        r.at,
        r.passes,
        r.next_at,
        r.starts_run,
        case
            {%- if incremental %}
            when not r.passes and not r.passed_before and w.status.location_low_since is not null
                then least(w.status.location_low_since, r.window_run_start)
            {%- endif %}
            when not r.passes then r.window_run_start
        end as low_since
    from (
        select
            *,
            max(case when starts_run then at end) over carried as window_run_start,
            coalesce(bool_or(passes) over carried, false) as passed_before
        from (
            select
                *,
                lead(at) over ordered as next_at,
                not passes and coalesce(lag(passes) over ordered, true) as starts_run
            from judged_heartbeats
            window ordered as (partition by hotspot_pubkey order by at)
        )
        window carried as (
            partition by hotspot_pubkey
            order by at
            rows between unbounded preceding and current row
        )
    ) as r
    {%- if incremental %}
    left join written as w
        on r.hotspot_pubkey = w.hotspot_pubkey
    {%- endif %}

),

heartbeat_events as (

    -- Per heartbeat: the heartbeat itself; silence running out under each rule
    -- when no heartbeat follows in time; and, for a run's first failure in the
    -- window, the run reaching its limit under each rule.
    select l.hotspot_pubkey, e.at, e.heartbeat_at, e.heartbeat, e.average
    from low_runs as l
    cross join rule_list
    cross join unnest(
        concat(
            array[
                cast(row(l.at, l.at, cast(row(l.passes, l.low_since) as {{ heartbeat_state }}), null) as {{ event }})
            ],
            filter(
                transform(
                    rule_list.rules,
                    rule -> case
                        when (l.next_at is null or l.next_at > l.at + rule.max_hours_silent * interval '1' hour)
                            and l.at + rule.max_hours_silent * interval '1' hour >= rule.valid_from
                            and (rule.valid_to is null or l.at + rule.max_hours_silent * interval '1' hour < rule.valid_to)
                            then cast(row(l.at + rule.max_hours_silent * interval '1' hour, null, null, null) as {{ event }})
                    end
                ),
                deadline -> deadline is not null
            ),
            filter(
                transform(
                    rule_list.rules,
                    rule -> case
                        when l.starts_run
                            and l.low_since + rule.max_hours_below * interval '1' hour >= rule.valid_from
                            and (rule.valid_to is null or l.low_since + rule.max_hours_below * interval '1' hour < rule.valid_to)
                            then cast(row(l.low_since + rule.max_hours_below * interval '1' hour, null, null, null) as {{ event }})
                    end
                ),
                deadline -> deadline is not null
            )
        )
    ) as e (at, heartbeat_at, heartbeat, average)

),

average_events as (

    -- Per average: the average itself, and it getting too old when no average
    -- follows in time.
    select a.hotspot_pubkey, e.at, e.heartbeat_at, e.heartbeat, e.average
    from (
        select
            s.hotspot_pubkey,
            s."timestamp" as at,
            cast(
                row(s."timestamp", s.sample_count, s.download_speed_avg_bps, s.upload_speed_avg_bps, s.latency_avg_ms)
                as {{ average_state }}
            ) as average,
            lead(s."timestamp") over (partition by s.hotspot_pubkey order by s."timestamp") as next_at
        from {{ averages }} as s
        inner join hotspots as h
            on s.hotspot_pubkey = h.hotspot_pubkey
        where
            {%- if has_zone %}
            s."timestamp" >= {{ zone_start }} - interval '{{ speedtest_history_hours }}' hour
            and s."timestamp" <= {{ zone_end }}
            {%- else %}
            false
            {%- endif %}
    ) as a
    cross join unnest(
        array[
            cast(row(a.at, null, null, a.average) as {{ event }}),
            case
                when a.next_at is null or a.next_at > a.at + interval '{{ window_hours }}' hour
                    then cast(row(a.at + interval '{{ window_hours }}' hour, null, null, null) as {{ event }})
            end
        ]
    ) as e (at, heartbeat_at, heartbeat, average)
    where e.at is not null

),

{%- if incremental %}

carried_in as (

    -- The latest written row as an event at its own time: a hotspot with no
    -- heartbeat in the window still has its last heartbeat and open low run,
    -- its silence still runs out and its run still reaches its limit, each
    -- under every rule row. A `not_covered` row carries no low run, so a
    -- hotspot back in coverage starts clean. Deadlines a later heartbeat
    -- overtakes are judged and change nothing.
    select w.hotspot_pubkey, e.at, e.heartbeat_at, e.heartbeat, e.average
    from written as w
    inner join hotspots as h
        on w.hotspot_pubkey = h.hotspot_pubkey
    cross join rule_list
    cross join unnest(
        concat(
            array[
                cast(
                    row(
                        w.changed_at,
                        w.last_heartbeat_at,
                        case
                            when w.last_heartbeat_at is not null
                                then cast(row(w.status.location_low_since is null, w.status.location_low_since) as {{ heartbeat_state }})
                        end,
                        null
                    ) as {{ event }}
                )
            ],
            filter(
                transform(
                    rule_list.rules,
                    rule -> case
                        when w.last_heartbeat_at + rule.max_hours_silent * interval '1' hour >= rule.valid_from
                            and (rule.valid_to is null or w.last_heartbeat_at + rule.max_hours_silent * interval '1' hour < rule.valid_to)
                            then cast(row(w.last_heartbeat_at + rule.max_hours_silent * interval '1' hour, null, null, null) as {{ event }})
                    end
                ),
                deadline -> deadline is not null
            ),
            filter(
                transform(
                    rule_list.rules,
                    rule -> case
                        when w.status.location_low_since + rule.max_hours_below * interval '1' hour >= rule.valid_from
                            and (rule.valid_to is null or w.status.location_low_since + rule.max_hours_below * interval '1' hour < rule.valid_to)
                            then cast(row(w.status.location_low_since + rule.max_hours_below * interval '1' hour, null, null, null) as {{ event }})
                    end
                ),
                deadline -> deadline is not null
            )
        )
    ) as e (at, heartbeat_at, heartbeat, average)

),

{%- endif %}

boundary_events as (

    -- Rule starts, and each hotspot's first moment: the first judged moment or
    -- its first assertion, whichever is later. That moment falls in exactly
    -- one run, so a hotspot with no data at all still gets a row, and the same
    -- one however the runs fall. A resumed run also starts the hotspots it has
    -- no live row for -- new to the assertions table since its moment was
    -- judged, or back in coverage -- at their moment or this interval's
    -- opening, whichever is later.
    select h.hotspot_pubkey, s.at, null as heartbeat_at, null as heartbeat, null as average
    from hotspots as h
    cross join (
        select valid_from as at from heartbeat_rules
        union
        select valid_from from floors
    ) as s

    {%- if resuming %}

    union all

    select
        h.hotspot_pubkey,
        greatest(
            case when w.hotspot_pubkey is null then h.first_asserted else h.asserted_from end,
            {{ zone_opening }}
        ),
        null,
        null,
        null
    from hotspots as h
    left join written as w
        on h.hotspot_pubkey = w.hotspot_pubkey
    where w.hotspot_pubkey is null or w.not_covered

    {%- elif first_judged is not none %}

    union all

    select hotspot_pubkey, greatest(first_asserted, {{ first_judged }}), null, null, null
    from hotspots
    {%- endif %}

),

events as (

    select * from heartbeat_events
    union all
    select * from average_events
    {%- if incremental %}
    union all
    select * from carried_in
    {%- endif %}
    union all
    select * from boundary_events

),

states as (

    -- The state after each event, carried forward: the latest heartbeat and
    -- its verdict, the latest average. Within one moment, data sorts after
    -- the moments that carry none, so the moment's last row holds it all.
    select
        hotspot_pubkey,
        at,
        max(heartbeat_at) over carried as last_heartbeat_at,
        last_value(heartbeat) ignore nulls over carried as last_heartbeat,
        last_value(average) ignore nulls over carried as last_average,
        row_number() over (
            partition by hotspot_pubkey, at
            order by heartbeat_at desc nulls last, average.at desc nulls last
        ) as event_rank
    from events
    window carried as (
        partition by hotspot_pubkey
        order by at, heartbeat_at nulls first, average.at nulls first
        rows between unbounded preceding and current row
    )

),

judged as (

    -- One judgement per hotspot per moment in the interval, with the rules in
    -- effect then.
    select
        s.hotspot_pubkey,
        s.at,
        s.last_heartbeat_at,
        -- Only while it still counts: an average older than the window decides
        -- nothing, and whether a run's input reached it depends on run timing.
        case
            when s.last_average.at > s.at - interval '{{ window_hours }}' hour then s.last_average.at
        end as last_average_at,
        case
            when not s.last_heartbeat.passes then s.last_heartbeat.low_since
        end as location_low_since,
        f.valid_from as speedtest_floor_from,
        filter(
            array[
                case
                    when s.last_heartbeat_at is null
                        or s.last_heartbeat_at <= s.at - r.max_hours_silent * interval '1' hour
                        then 'no_heartbeats'
                end,
                case
                    when not s.last_heartbeat.passes
                        and s.last_heartbeat.low_since <= s.at - r.max_hours_below * interval '1' hour
                        then 'location'
                end,
                case
                    when f.valid_from is not null and (
                        s.last_average is null
                        or s.last_average.at <= s.at - interval '{{ window_hours }}' hour
                        or s.last_average.sample_count < f.min_sample_count
                    )
                        then 'no_recent_speedtest'
                end,
                case
                    when f.valid_from is not null
                        and s.last_average.at > s.at - interval '{{ window_hours }}' hour
                        and s.last_average.sample_count >= f.min_sample_count
                        and (
                            s.last_average.download_bps < f.min_download_bps
                            or s.last_average.upload_bps < f.min_upload_bps
                            or s.last_average.latency_ms > f.max_latency_ms
                        )
                        then 'below_floor'
                end
            ],
            reason -> reason is not null
        ) as reasons
    from states as s
    inner join rules_present as g
        on g.ok
    inner join heartbeat_rules as r
        on s.at >= r.valid_from and (r.valid_to is null or s.at < r.valid_to)
    left join floors as f
        on s.at >= f.valid_from and (f.valid_to is null or s.at < f.valid_to)
    where
        s.event_rank = 1
        {%- if has_zone %}
        -- The interval is (zone_start, zone_end], except the first run's, which
        -- includes its start: every hotspot's first moment can be exactly there.
        and s.at {{ '>' if resuming else '>=' }} {{ zone_start }}
        and s.at <= {{ zone_end }}
        {%- else %}
        and false
        {%- endif %}

),

sequenced as (

    select
        *,
        cast(row(reasons, location_low_since) as {{ status }}) as status,
        lag(cast(row(reasons, location_low_since) as {{ status }}))
            over (partition by hotspot_pubkey order by at) as previous_status
    from judged

)

select
    s.hotspot_pubkey,
    s.at as changed_at,
    cardinality(s.reasons) = 0 as eligible,
    s.reasons,
    s.location_low_since,
    s.last_heartbeat_at,
    s.last_average_at,
    s.speedtest_floor_from
from sequenced as s
{%- if incremental %}
left join written as w
    on s.hotspot_pubkey = w.hotspot_pubkey
{%- endif %}
where
    {%- if incremental %}
    coalesce(s.previous_status, w.status) is distinct from s.status
    {%- else %}
    s.previous_status is distinct from s.status
    {%- endif %}

{%- if resuming %}

union all

-- Hotspots that left coverage since their latest row: one final row, at the
-- assertion that took them out or this interval's opening, whichever is later.
select
    w.hotspot_pubkey,
    greatest(coalesce(a.asserted_from, {{ zone_opening }}), {{ zone_opening }}) as changed_at,
    false as eligible,
    cast(array['not_covered'] as array(varchar)) as reasons,
    cast(null as {{ ts }}) as location_low_since,
    w.last_heartbeat_at,
    cast(null as {{ ts }}) as last_average_at,
    cast(null as {{ ts }}) as speedtest_floor_from
from written as w
left join assertions as a
    on w.hotspot_pubkey = a.hotspot_pubkey
where
    not w.not_covered
    and not coalesce(a.covered, false)
    {%- if has_zone %}
    and greatest(coalesce(a.asserted_from, {{ zone_opening }}), {{ zone_opening }}) <= {{ zone_end }}
    {%- else %}
    and false
    {%- endif %}
{%- endif %}
