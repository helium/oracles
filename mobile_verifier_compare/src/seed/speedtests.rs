use super::plan::{Bucket, Plan};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use mobile_verifier::speedtests::SPEEDTEST_AVG_MAX_DATA_POINTS;
use sqlx::PgPool;
use trino_client::Client as TrinoClient;

/// `aggregate_epoch_speedtests` keeps the latest `SPEEDTEST_AVG_MAX_DATA_POINTS`
/// rows per pubkey within the SPEEDTEST_LAPSE window. Seed exactly that many
/// on the match buckets so the per-pubkey sums on both sides are equal.
const SPEEDTESTS_PER_HOTSPOT: usize = SPEEDTEST_AVG_MAX_DATA_POINTS;
/// Constants for the matching-bucket rows. Picked to be obviously synthetic.
const MATCH_UPLOAD: u64 = 10_000_000;
const MATCH_DOWNLOAD: u64 = 100_000_000;
const MATCH_LATENCY: u32 = 25;

pub async fn seed(pg: &PgPool, trino: &TrinoClient, plan: &Plan) -> Result<()> {
    println!("[speedtests] clearing window in PG + Iceberg");
    sqlx::query("DELETE FROM speedtests WHERE timestamp >= $1 AND timestamp < $2")
        .bind(plan.epoch_start)
        .bind(plan.epoch_end)
        .execute(pg)
        .await
        .context("clearing PG speedtests window")?;
    trino
        .execute_raw(format!(
            "DELETE FROM poc.speedtests \
             WHERE timestamp >= TIMESTAMP '{}' AND timestamp < TIMESTAMP '{}'",
            fmt_ts(plan.epoch_start),
            fmt_ts(plan.epoch_end),
        ))
        .await
        .context("clearing iceberg poc.speedtests window")?;

    let mut pg_inserts = 0usize;
    let mut trino_rows: Vec<String> = Vec::new();

    for h in &plan.hotspots {
        // Per-bucket value plan:
        // - match     : same values on both sides
        // - mismatch  : PG upload_speed is doubled so per-hotspot sums diverge
        // - pg_only   : PG inserts, Iceberg skipped
        // - trino_only: Iceberg inserts, PG skipped
        let (pg_upload, trino_upload) = match h.bucket {
            Bucket::Match => (MATCH_UPLOAD, MATCH_UPLOAD),
            Bucket::Mismatch => (MATCH_UPLOAD * 2, MATCH_UPLOAD),
            Bucket::PgOnly => (MATCH_UPLOAD, 0),    // 0 unused
            Bucket::TrinoOnly => (0, MATCH_UPLOAD), // 0 unused
        };
        let pg_enabled = !matches!(h.bucket, Bucket::TrinoOnly);
        let trino_enabled = !matches!(h.bucket, Bucket::PgOnly);

        for i in 0..SPEEDTESTS_PER_HOTSPOT {
            // One row per hour starting at epoch_end-1h going back.
            let ts = plan.epoch_end - chrono::Duration::hours(1 + i as i64);
            if pg_enabled {
                sqlx::query(
                    "INSERT INTO speedtests
                     (pubkey, upload_speed, download_speed, latency, serial_num, timestamp)
                     VALUES ($1, $2, $3, $4, $5, $6)
                     ON CONFLICT (pubkey, timestamp) DO UPDATE SET
                        upload_speed = EXCLUDED.upload_speed,
                        download_speed = EXCLUDED.download_speed,
                        latency = EXCLUDED.latency,
                        serial_num = EXCLUDED.serial_num",
                )
                .bind(&h.pubkey)
                .bind(pg_upload as i64)
                .bind(MATCH_DOWNLOAD as i64)
                .bind(MATCH_LATENCY as i32)
                .bind(serial_for(&h.pubkey))
                .bind(ts)
                .execute(pg)
                .await
                .context("inserting speedtest row")?;
                pg_inserts += 1;
            }
            if trino_enabled {
                let serial = serial_for(&h.pubkey);
                trino_rows.push(format!(
                    "('{pubkey}', '{serial}', TIMESTAMP '{ts}', TIMESTAMP '{ts}', \
                     {up}, {down}, {lat})",
                    pubkey = h.pubkey,
                    ts = fmt_ts(ts),
                    up = trino_upload as i64,
                    down = MATCH_DOWNLOAD as i64,
                    lat = MATCH_LATENCY as i64,
                ));
            }
        }
    }

    if !trino_rows.is_empty() {
        let sql = format!(
            "INSERT INTO poc.speedtests (
                hotspot_pubkey, serial, received_timestamp, timestamp,
                upload_speed, download_speed, latency
             ) VALUES {}",
            trino_rows.join(",\n  ")
        );
        trino
            .execute_raw(sql)
            .await
            .context("inserting poc.speedtests rows")?;
    }
    println!(
        "[speedtests] inserted {pg_inserts} PG rows, {} Iceberg rows",
        trino_rows.len()
    );
    Ok(())
}

fn fmt_ts(ts: DateTime<Utc>) -> String {
    ts.format("%Y-%m-%d %H:%M:%S%.6f UTC").to_string()
}

fn serial_for(pubkey: &str) -> String {
    let n = 8.min(pubkey.len());
    format!("seed-{}", &pubkey[..n])
}
