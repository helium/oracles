use crate::diff::{DiffTable, FloatEq, DEFAULT_F64_EPSILON};
use crate::CommonArgs;
use anyhow::{Context, Result};
use futures::TryStreamExt;
use mobile_verifier::heartbeats::{HeartbeatReward, MINIMUM_HEARTBEAT_COUNT};
use rust_decimal::prelude::ToPrimitive;
use serde::{Deserialize, Serialize};
use sqlx::{Pool, Postgres};
use std::collections::BTreeMap;
use trino_client::{Client as TrinoClient, Statement};
use trino_rust_client::Trino;

/// Per-hotspot summary of valid_radios.sql output, aggregated across cell types.
/// We sum across the (hotspot_key, cell_type) groups produced by the production
/// SQL because cell_type ↔ device_type mapping is lossy (multiple cbrs cell
/// types collapse to a single Trino `device_type='cbrs'`).
#[derive(Debug, Clone, Default, PartialEq)]
struct HeartbeatSummary {
    /// Number of (cell_type / device_type) groups that passed the
    /// `MINIMUM_HEARTBEAT_COUNT` threshold.
    valid_groups: u64,
    /// Total heartbeats counted across those groups.
    heartbeat_count: u64,
    /// Sum of all per-heartbeat trust_score_multipliers.
    multiplier_sum: f64,
}

impl FloatEq for HeartbeatSummary {
    fn float_eq(&self, other: &Self, epsilon: f64) -> bool {
        self.valid_groups == other.valid_groups
            && self.heartbeat_count == other.heartbeat_count
            && (self.multiplier_sum - other.multiplier_sum).abs() <= epsilon
    }
}

#[derive(Debug, Clone, Trino, Serialize, Deserialize)]
struct TrinoHeartbeatRow {
    hotspot_pubkey: String,
    valid_groups: i64,
    heartbeat_count: i64,
    multiplier_sum: f64,
}

const TRINO_SQL: &str = r#"
    WITH hourly AS (
        SELECT
            hotspot_pubkey,
            device_type,
            date_trunc('hour', heartbeat_timestamp) AS hour,
            avg(location_trust_score_multiplier) AS trust
        FROM poc.heartbeats
        WHERE heartbeat_timestamp >= :start
        AND heartbeat_timestamp < :end
        GROUP BY hotspot_pubkey, device_type, date_trunc('hour', heartbeat_timestamp)
    ),
    groups AS (
        SELECT
            hotspot_pubkey,
            device_type,
            count(*) AS n_hours,
            sum(trust) AS trust_sum
        FROM hourly
        GROUP BY hotspot_pubkey, device_type
        HAVING count(*) >= :min_count
    )
    SELECT
        hotspot_pubkey,
        cast(count(*) AS bigint) AS valid_groups,
        cast(sum(n_hours) AS bigint) AS heartbeat_count,
        sum(trust_sum) AS multiplier_sum
    FROM groups
    GROUP BY hotspot_pubkey
"#;

pub async fn run(pg: &Pool<Postgres>, trino: &TrinoClient, args: &CommonArgs) -> Result<()> {
    let epoch = args.epoch();

    let mut pg_rows: BTreeMap<String, HeartbeatSummary> = BTreeMap::new();
    let mut stream = HeartbeatReward::validated(pg, &epoch);
    while let Some(row) = stream
        .try_next()
        .await
        .context("streaming HeartbeatReward::validated")?
    {
        let key = row.hotspot_key.to_string();
        let entry = pg_rows.entry(key).or_default();
        entry.valid_groups += 1;
        entry.heartbeat_count += row.trust_score_multipliers.len() as u64;
        entry.multiplier_sum += row
            .trust_score_multipliers
            .iter()
            .filter_map(|d| d.to_f64())
            .sum::<f64>();
    }

    let stmt = Statement::new(TRINO_SQL)
        .bind("start", epoch.start)
        .bind("end", epoch.end)
        .bind("min_count", MINIMUM_HEARTBEAT_COUNT)
        .typed::<TrinoHeartbeatRow>();
    let trino_rows: Vec<TrinoHeartbeatRow> = trino
        .get_all(stmt)
        .await
        .context("running Trino heartbeats query")?;
    let trino_map: BTreeMap<String, HeartbeatSummary> = trino_rows
        .into_iter()
        .map(|r| {
            (
                r.hotspot_pubkey,
                HeartbeatSummary {
                    valid_groups: r.valid_groups.max(0) as u64,
                    heartbeat_count: r.heartbeat_count.max(0) as u64,
                    multiplier_sum: r.multiplier_sum,
                },
            )
        })
        .collect();

    let mut table = DiffTable::new(
        "heartbeats — valid_radios.sql parity (per hotspot, summed over cell_type groups)",
        "hotspot_pubkey",
        "groups/heartbeats/multiplier_sum",
    )
    .with_note(
        "PG cell_type → Trino device_type is a many-to-one mapping (multiple cbrs cell types collapse to one device_type='cbrs'). \
         Mismatches in `valid_groups` are expected when a hotspot has multiple cbrs cell types in PG.",
    )
    .with_options(args.show_all, args.limit);
    table.fill(
        pg_rows,
        trino_map,
        DEFAULT_F64_EPSILON,
        |k| k.clone(),
        |v| {
            format!(
                "{}/{}/{:.6}",
                v.valid_groups, v.heartbeat_count, v.multiplier_sum
            )
        },
        |p, t| {
            format!(
                "g={:+} hb={:+} mul={:+.6}",
                p.valid_groups as i64 - t.valid_groups as i64,
                p.heartbeat_count as i64 - t.heartbeat_count as i64,
                p.multiplier_sum - t.multiplier_sum,
            )
        },
    );
    table.print();
    Ok(())
}
