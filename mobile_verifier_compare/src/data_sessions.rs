use crate::diff::{DiffTable, FloatEq, DEFAULT_F64_EPSILON};
use crate::CommonArgs;
use anyhow::{Context, Result};
use mobile_verifier::data_session::aggregate_hotspot_data_sessions_to_dc;
use serde::{Deserialize, Serialize};
use sqlx::{Pool, Postgres};
use std::collections::BTreeMap;
use trino_client::{Client as TrinoClient, Statement};
use trino_rust_client::Trino;

#[derive(Debug, Clone, Default, PartialEq)]
struct DcSummary {
    rewardable_bytes: u64,
}

impl FloatEq for DcSummary {
    fn float_eq(&self, other: &Self, _: f64) -> bool {
        self == other
    }
}

#[derive(Debug, Clone, Trino, Serialize, Deserialize)]
struct TrinoDcRow {
    hotspot_key: String,
    rewardable_bytes: i64,
}

/// Compares `rewardable_bytes` aggregated per hotspot.
///
/// PG side reads `hotspot_data_transfer_sessions` rows whose `burn_timestamp`
/// falls in the window — that's the reward pipeline's *input*. Trino side
/// reads `rewards.data_transfer` rows whose `[start_period, end_period)`
/// overlaps the window — that's the pipeline's *output*.
///
/// `rewardable_bytes` is the only column that is apples-to-apples between the
/// two tables — both sides report it in raw bytes. PG's `num_dcs` (DC count,
/// an input to the burn step) and Trino's `dc_transfer_reward` (the paid
/// reward in bones, an output of burn × price) live in different units, so
/// they are deliberately not diffed here.
///
/// Mismatches are expected on sessions that exist in PG but didn't produce a
/// reward (e.g. sub-DC dust, or sessions whose hotspot was banned at reward
/// time). PG totals should be ≥ Trino totals in a normal epoch.
const TRINO_SQL: &str = r#"
    SELECT
        hotspot_key,
        cast(sum(rewardable_bytes) AS bigint) AS rewardable_bytes
    FROM rewards.data_transfer
    WHERE start_period < :end AND end_period > :start
    GROUP BY hotspot_key
"#;

pub async fn run(pg: &Pool<Postgres>, trino: &TrinoClient, args: &CommonArgs) -> Result<()> {
    let epoch = args.epoch();

    let pg_raw = aggregate_hotspot_data_sessions_to_dc(pg, &epoch)
        .await
        .context("running aggregate_hotspot_data_sessions_to_dc")?;
    let pg_rows: BTreeMap<String, DcSummary> = pg_raw
        .into_iter()
        .map(|(k, v)| {
            (
                k.to_string(),
                DcSummary {
                    rewardable_bytes: v.rewardable_bytes,
                },
            )
        })
        .collect();

    let stmt = Statement::new(TRINO_SQL)
        .bind("start", epoch.start)
        .bind("end", epoch.end)
        .typed::<TrinoDcRow>();
    let trino_raw: Vec<TrinoDcRow> = trino
        .get_all(stmt)
        .await
        .context("running Trino rewards.data_transfer query")?;
    let trino_rows: BTreeMap<String, DcSummary> = trino_raw
        .into_iter()
        .map(|r| {
            (
                r.hotspot_key,
                DcSummary {
                    rewardable_bytes: r.rewardable_bytes.max(0) as u64,
                },
            )
        })
        .collect();

    let mut table = DiffTable::new(
        "data-sessions — rewardable_bytes parity (PG input vs Trino reward output)",
        "hotspot_key",
        "rewardable_bytes",
    )
    .with_note(
        "PG sums hotspot_data_transfer_sessions (pipeline input); Trino sums rewards.data_transfer (pipeline output). \
         Only `rewardable_bytes` is comparable across these two tables — DC count and dc_transfer_reward are different units. \
         Mismatches are expected on sessions that didn't produce a reward (sub-DC dust, banned hotspots, etc.).",
    )
    .with_options(args.show_all, args.limit);
    table.fill(
        pg_rows,
        trino_rows,
        DEFAULT_F64_EPSILON,
        |k| k.clone(),
        |v| v.rewardable_bytes.to_string(),
        |p, t| {
            format!(
                "{:+}",
                p.rewardable_bytes as i128 - t.rewardable_bytes as i128,
            )
        },
    );
    table.print();
    Ok(())
}
