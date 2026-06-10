use crate::diff::{DiffTable, FloatEq, DEFAULT_F64_EPSILON};
use crate::CommonArgs;
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use helium_crypto::PublicKeyBinary;
use mobile_verifier::speedtests::{
    aggregate_epoch_speedtests, get_latest_speedtests_for_pubkey, SPEEDTEST_AVG_MAX_DATA_POINTS,
};
use mobile_verifier::speedtests_average::SPEEDTEST_LAPSE;
use serde::{Deserialize, Serialize};
use sqlx::{Pool, Postgres};
use std::collections::BTreeMap;
use trino_client::{Client as TrinoClient, Statement};
use trino_rust_client::Trino;

#[derive(Debug, Clone, Default, PartialEq)]
struct SpeedtestSummary {
    count: u64,
    upload_sum: u64,
    download_sum: u64,
    latency_sum: u64,
}

impl FloatEq for SpeedtestSummary {
    fn float_eq(&self, other: &Self, _: f64) -> bool {
        self == other
    }
}

#[derive(Debug, Clone, Trino, Serialize, Deserialize)]
struct TrinoSpeedtestRow {
    hotspot_pubkey: String,
    n: i64,
    upload_sum: i64,
    download_sum: i64,
    latency_sum: i64,
}

const TRINO_AGGREGATE_SQL: &str = r#"
    SELECT
        hotspot_pubkey,
        cast(count(*) AS bigint) AS n,
        cast(sum(upload_speed) AS bigint) AS upload_sum,
        cast(sum(download_speed) AS bigint) AS download_sum,
        cast(sum(latency) AS bigint) AS latency_sum
    FROM (
        SELECT
            hotspot_pubkey,
            upload_speed,
            download_speed,
            latency,
            row_number() OVER (PARTITION BY hotspot_pubkey ORDER BY timestamp DESC) AS rn
        FROM poc.speedtests
        WHERE timestamp >= :start AND timestamp < :end
    )
    WHERE rn <= :max_points
    GROUP BY hotspot_pubkey
"#;

/// Trino doesn't allow `LIMIT ?` placeholders, so the limit is interpolated
/// at compile time from `SPEEDTEST_AVG_MAX_DATA_POINTS` rather than bound.
fn trino_latest_sql() -> String {
    format!(
        "SELECT \
            hotspot_pubkey, upload_speed, download_speed, latency, timestamp \
         FROM poc.speedtests \
         WHERE hotspot_pubkey = :pubkey \
         AND timestamp >= :start \
         AND timestamp <= :anchor \
         ORDER BY timestamp DESC \
         LIMIT {}",
        SPEEDTEST_AVG_MAX_DATA_POINTS
    )
}

pub async fn run(pg: &Pool<Postgres>, trino: &TrinoClient, args: &CommonArgs) -> Result<()> {
    let epoch_end = args.end;
    let start = epoch_end - chrono::Duration::hours(SPEEDTEST_LAPSE);

    let pg_raw = aggregate_epoch_speedtests(epoch_end, pg)
        .await
        .context("running aggregate_epoch_speedtests")?;
    let mut pg_rows: BTreeMap<String, SpeedtestSummary> = BTreeMap::new();
    for (pubkey, tests) in pg_raw {
        let mut s = SpeedtestSummary::default();
        for t in &tests {
            s.count += 1;
            s.upload_sum += t.report.upload_speed;
            s.download_sum += t.report.download_speed;
            s.latency_sum += t.report.latency as u64;
        }
        pg_rows.insert(pubkey.to_string(), s);
    }

    let stmt = Statement::new(TRINO_AGGREGATE_SQL)
        .bind("start", start)
        .bind("end", epoch_end)
        .bind("max_points", SPEEDTEST_AVG_MAX_DATA_POINTS as i64)
        .typed::<TrinoSpeedtestRow>();
    let trino_raw: Vec<TrinoSpeedtestRow> = trino
        .get_all(stmt)
        .await
        .context("running Trino speedtests query")?;
    let trino_rows: BTreeMap<String, SpeedtestSummary> = trino_raw
        .into_iter()
        .map(|r| {
            (
                r.hotspot_pubkey,
                SpeedtestSummary {
                    count: r.n.max(0) as u64,
                    upload_sum: r.upload_sum.max(0) as u64,
                    download_sum: r.download_sum.max(0) as u64,
                    latency_sum: r.latency_sum.max(0) as u64,
                },
            )
        })
        .collect();

    let mut table = DiffTable::new(
        format!(
            "speedtests — aggregate_epoch_speedtests parity (window {} → {})",
            start, epoch_end
        ),
        "hotspot_pubkey",
        "n/up_sum/dn_sum/lat_sum",
    )
    .with_note(
        "Window is end-SPEEDTEST_LAPSE..end regardless of --start (matches production aggregate_epoch_speedtests).",
    )
    .with_options(args.show_all, args.limit);
    table.fill(
        pg_rows,
        trino_rows,
        DEFAULT_F64_EPSILON,
        |k| k.clone(),
        |v| {
            format!(
                "{}/{}/{}/{}",
                v.count, v.upload_sum, v.download_sum, v.latency_sum
            )
        },
        |p, t| {
            format!(
                "n={:+} up={:+} dn={:+} lat={:+}",
                p.count as i64 - t.count as i64,
                p.upload_sum as i64 - t.upload_sum as i64,
                p.download_sum as i64 - t.download_sum as i64,
                p.latency_sum as i64 - t.latency_sum as i64,
            )
        },
    );
    table.print();
    Ok(())
}

pub async fn run_latest(
    pg: &Pool<Postgres>,
    trino: &TrinoClient,
    pubkey: &str,
    anchor: Option<DateTime<Utc>>,
    args: &CommonArgs,
) -> Result<()> {
    let anchor = anchor.unwrap_or(args.end);
    let pk: PublicKeyBinary = pubkey
        .parse()
        .context("parsing --pubkey as a base58 PublicKeyBinary")?;

    let mut tx = pg.begin().await.context("begin tx")?;
    let pg_tests = get_latest_speedtests_for_pubkey(&pk, anchor, &mut tx)
        .await
        .context("running get_latest_speedtests_for_pubkey")?;
    tx.rollback().await.ok();

    let start = anchor - chrono::Duration::hours(SPEEDTEST_LAPSE);
    let stmt = Statement::new(trino_latest_sql())
        .bind("pubkey", pubkey)
        .bind("start", start)
        .bind("anchor", anchor)
        .typed::<TrinoLatestRow>();
    let trino_tests: Vec<TrinoLatestRow> = trino
        .get_all(stmt)
        .await
        .context("running Trino latest speedtests query")?;

    let pg_summary = pg_tests
        .iter()
        .fold(SpeedtestSummary::default(), |mut s, t| {
            s.count += 1;
            s.upload_sum += t.report.upload_speed;
            s.download_sum += t.report.download_speed;
            s.latency_sum += t.report.latency as u64;
            s
        });
    let trino_summary = trino_tests
        .iter()
        .fold(SpeedtestSummary::default(), |mut s, t| {
            s.count += 1;
            s.upload_sum += t.upload_speed.max(0) as u64;
            s.download_sum += t.download_speed.max(0) as u64;
            s.latency_sum += t.latency.max(0) as u64;
            s
        });

    let mut pg_map = BTreeMap::new();
    pg_map.insert(pubkey.to_string(), pg_summary);
    let mut trino_map = BTreeMap::new();
    trino_map.insert(pubkey.to_string(), trino_summary);

    let mut table = DiffTable::new(
        format!("latest-speedtests — get_latest_speedtests_for_pubkey (anchor {anchor})"),
        "hotspot_pubkey",
        "n/up_sum/dn_sum/lat_sum",
    )
    .with_options(true, args.limit);
    table.fill(
        pg_map,
        trino_map,
        DEFAULT_F64_EPSILON,
        |k| k.clone(),
        |v| {
            format!(
                "{}/{}/{}/{}",
                v.count, v.upload_sum, v.download_sum, v.latency_sum
            )
        },
        |p, t| {
            format!(
                "n={:+} up={:+} dn={:+} lat={:+}",
                p.count as i64 - t.count as i64,
                p.upload_sum as i64 - t.upload_sum as i64,
                p.download_sum as i64 - t.download_sum as i64,
                p.latency_sum as i64 - t.latency_sum as i64,
            )
        },
    );
    table.print();
    Ok(())
}

#[derive(Debug, Clone, Trino, Serialize, Deserialize)]
struct TrinoLatestRow {
    #[allow(dead_code)]
    hotspot_pubkey: String,
    upload_speed: i64,
    download_speed: i64,
    latency: i64,
    #[allow(dead_code)]
    timestamp: chrono::DateTime<chrono::FixedOffset>,
}
