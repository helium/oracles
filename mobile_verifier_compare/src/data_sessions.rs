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
    rewardable_dc: u64,
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
    dc_transfer_reward: i64,
}

/// Trino-side: aggregate `rewards.data_transfer` rows whose `[start_period, end_period)`
/// overlaps the requested window. This is the *output* of the production reward
/// pipeline, not raw sessions, so this comparison is an input-vs-output sanity
/// check: PG numbers should be >= Trino numbers in a normal epoch (PG includes
/// all sessions, Trino only includes those that got rewards written).
const TRINO_SQL: &str = r#"
    SELECT
        hotspot_key,
        cast(sum(rewardable_bytes) AS bigint) AS rewardable_bytes,
        cast(sum(dc_transfer_reward) AS bigint) AS dc_transfer_reward
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
                    rewardable_dc: v.rewardable_dc,
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
                    rewardable_dc: r.dc_transfer_reward.max(0) as u64,
                },
            )
        })
        .collect();

    let mut table = DiffTable::new(
        "data-sessions — PG input aggregate vs Trino rewards.data_transfer output",
        "hotspot_key",
        "bytes/dc",
    )
    .with_note(
        "PG side sums raw hotspot_data_transfer_sessions; Trino side sums rewards.data_transfer (already-paid rewards). \
         Mismatches are expected on sessions that didn't make it to a reward (e.g. sub-DC dust).",
    )
    .with_options(args.show_all, args.limit);
    table.fill(
        pg_rows,
        trino_rows,
        DEFAULT_F64_EPSILON,
        |k| k.clone(),
        |v| format!("{}/{}", v.rewardable_bytes, v.rewardable_dc),
        |p, t| {
            format!(
                "bytes={:+} dc={:+}",
                p.rewardable_bytes as i128 - t.rewardable_bytes as i128,
                p.rewardable_dc as i128 - t.rewardable_dc as i128,
            )
        },
    );
    table.print();
    Ok(())
}
