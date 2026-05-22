use crate::diff::{DiffTable, FloatEq, DEFAULT_F64_EPSILON};
use crate::CommonArgs;
use anyhow::{Context, Result};
use mobile_verifier::data_session::sum_data_sessions_to_dc_by_payer;
use serde::{Deserialize, Serialize};
use sqlx::{Pool, Postgres};
use std::collections::BTreeMap;
use trino_client::{Client as TrinoClient, Statement};
use trino_rust_client::Trino;

#[derive(Debug, Clone, Copy, Default, PartialEq)]
struct DcOnly(u64);

impl FloatEq for DcOnly {
    fn float_eq(&self, other: &Self, _: f64) -> bool {
        self == other
    }
}

#[derive(Debug, Clone, Trino, Serialize, Deserialize)]
struct TrinoPayerRow {
    payer: String,
    total_dcs: i64,
}

/// Trino doesn't expose per-payer rollups directly — `rewards.data_transfer` is
/// keyed by hotspot, not payer. There is no payer column on the iceberg side
/// today, so we fall back to a per-hotspot total and bucket it under the
/// pseudo-payer key `__all__`. This still catches gross divergence; per-payer
/// breakdown requires either a payer-keyed iceberg writer or a join through
/// hotspot ownership.
const TRINO_SQL: &str = r#"
    SELECT
        '__all__' AS payer,
        cast(sum(dc_transfer_reward) AS bigint) AS total_dcs
    FROM rewards.data_transfer
    WHERE start_period < :end AND end_period > :start
"#;

pub async fn run(pg: &Pool<Postgres>, trino: &TrinoClient, args: &CommonArgs) -> Result<()> {
    let epoch = args.epoch();

    let pg_raw = sum_data_sessions_to_dc_by_payer(pg, &epoch)
        .await
        .context("running sum_data_sessions_to_dc_by_payer")?;
    let mut pg_rows: BTreeMap<String, DcOnly> =
        pg_raw.into_iter().map(|(k, v)| (k, DcOnly(v))).collect();
    let pg_total: u64 = pg_rows.values().map(|d| d.0).sum();
    pg_rows.insert("__all__".to_string(), DcOnly(pg_total));

    let stmt = Statement::new(TRINO_SQL)
        .bind("start", epoch.start)
        .bind("end", epoch.end)
        .typed::<TrinoPayerRow>();
    let trino_raw: Vec<TrinoPayerRow> = trino
        .get_all(stmt)
        .await
        .context("running Trino payer-burns query")?;
    let trino_rows: BTreeMap<String, DcOnly> = trino_raw
        .into_iter()
        .map(|r| (r.payer, DcOnly(r.total_dcs.max(0) as u64)))
        .collect();

    let mut table = DiffTable::new(
        "payer-burns — PG per-payer DC totals (Trino has no payer column; rolled to __all__)",
        "payer",
        "total_dcs",
    )
    .with_note(
        "Only the __all__ bucket is comparable until a payer-keyed iceberg table exists. \
         Per-payer PG rows are shown for visibility.",
    )
    .with_options(args.show_all, args.limit);
    table.fill(
        pg_rows,
        trino_rows,
        DEFAULT_F64_EPSILON,
        |k| k.clone(),
        |v| v.0.to_string(),
        |p, t| format!("{:+}", p.0 as i128 - t.0 as i128),
    );
    table.print();
    Ok(())
}
