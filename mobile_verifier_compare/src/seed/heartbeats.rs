use super::plan::{Bucket, Plan};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use sqlx::PgPool;
use trino_client::Client as TrinoClient;
use uuid::Uuid;

/// PG `cell_type` enum value used for every fixture hotspot. We pick the wifi
/// variant since `novagenericwifiindoor` maps cleanly to Trino's `device_type
/// = 'wifi_indoor'`, so the per-hotspot summary the diff produces stays 1:1.
const PG_CELL_TYPE: &str = "novagenericwifiindoor";
const TRINO_DEVICE_TYPE: &str = "wifi_indoor";

/// Number of hours each bucket writes on each side.
///
/// `valid_radios.sql` requires `count(*) >= 12` to keep a (hotspot, cell_type)
/// group, so the match/pg_only/trino_only buckets all write 12. The mismatch
/// bucket writes 14 on PG and 12 on Iceberg so both sides keep the row but
/// the aggregate counts differ.
const PG_HOURS_MATCH: usize = 12;
const TRINO_HOURS_MATCH: usize = 12;
const PG_HOURS_MISMATCH: usize = 14;
const TRINO_HOURS_MISMATCH: usize = 12;

pub async fn seed(pg: &PgPool, trino: &TrinoClient, plan: &Plan) -> Result<()> {
    println!("[heartbeats] clearing window in PG + Iceberg");
    sqlx::query(
        "DELETE FROM wifi_heartbeats \
         WHERE truncated_timestamp >= $1 AND truncated_timestamp < $2",
    )
    .bind(plan.epoch_start)
    .bind(plan.epoch_end)
    .execute(pg)
    .await
    .context("clearing PG wifi_heartbeats window")?;

    let trino_delete = format!(
        "DELETE FROM poc.heartbeats \
         WHERE received_timestamp >= TIMESTAMP '{}' \
           AND received_timestamp < TIMESTAMP '{}'",
        fmt_ts(plan.epoch_start),
        fmt_ts(plan.epoch_end),
    );
    trino
        .execute_raw(trino_delete)
        .await
        .context("clearing iceberg poc.heartbeats window")?;

    let mut pg_inserts = 0usize;
    let mut trino_rows: Vec<String> = Vec::new();

    for h in &plan.hotspots {
        let (pg_hours, trino_hours) = match h.bucket {
            Bucket::Match => (PG_HOURS_MATCH, TRINO_HOURS_MATCH),
            Bucket::Mismatch => (PG_HOURS_MISMATCH, TRINO_HOURS_MISMATCH),
            Bucket::PgOnly => (PG_HOURS_MATCH, 0),
            Bucket::TrinoOnly => (0, TRINO_HOURS_MATCH),
        };

        let coverage_object = Uuid::new_v4();
        // Make the trust multiplier slightly different per bucket so the
        // mismatch path can show a non-zero delta even when group counts match.
        let multiplier = match h.bucket {
            Bucket::Mismatch => Decimal::new(75, 2), // 0.75 on PG side
            _ => Decimal::new(100, 2),               // 1.00
        };

        // Walk backwards from epoch_end so the most recent N hours are filled.
        for i in 0..pg_hours.max(trino_hours) {
            let ts = plan.epoch_end - chrono::Duration::hours(1 + i as i64);
            if i < pg_hours {
                sqlx::query(
                    "INSERT INTO wifi_heartbeats (
                        hotspot_key, cell_type, truncated_timestamp, first_timestamp,
                        location_validation_timestamp, distance_to_asserted,
                        coverage_object, location_trust_score_multiplier, lat, lon
                    ) VALUES ($1, $2::cell_type, $3, $4, $5, $6, $7, $8, $9, $10)
                    ON CONFLICT (hotspot_key, truncated_timestamp) DO UPDATE SET
                        cell_type = EXCLUDED.cell_type,
                        first_timestamp = EXCLUDED.first_timestamp,
                        location_validation_timestamp = EXCLUDED.location_validation_timestamp,
                        distance_to_asserted = EXCLUDED.distance_to_asserted,
                        coverage_object = EXCLUDED.coverage_object,
                        location_trust_score_multiplier = EXCLUDED.location_trust_score_multiplier,
                        lat = EXCLUDED.lat,
                        lon = EXCLUDED.lon",
                )
                .bind(&h.pubkey)
                .bind(PG_CELL_TYPE)
                .bind(ts)
                .bind(ts)
                .bind(ts)
                .bind(0_i64)
                .bind(coverage_object)
                .bind(multiplier)
                .bind(37.7749_f64)
                .bind(-122.4194_f64)
                .execute(pg)
                .await
                .context("inserting wifi_heartbeats row")?;
                pg_inserts += 1;
            }
            if i < trino_hours {
                let trino_mult = match h.bucket {
                    Bucket::Mismatch => 1.0_f64, // Trino side higher → delta != 0
                    _ => 1.0_f64,
                };
                trino_rows.push(format!(
                    "('{pubkey}', TIMESTAMP '{ts}', TIMESTAMP '{ts}', '{device}', \
                     {lat}, {lon}, '{cov}', NULL, 0, NULL, {mult}, 'skyhook')",
                    pubkey = h.pubkey,
                    ts = fmt_ts(ts),
                    device = TRINO_DEVICE_TYPE,
                    lat = 37.7749_f64,
                    lon = -122.4194_f64,
                    cov = coverage_object,
                    mult = trino_mult,
                ));
            }
        }
    }

    if !trino_rows.is_empty() {
        let sql = format!(
            "INSERT INTO poc.heartbeats (
                hotspot_pubkey, received_timestamp, heartbeat_timestamp, device_type,
                lat, lon, coverage_object, location_validation_timestamp,
                distance_to_asserted, asserted_location,
                location_trust_score_multiplier, location_source
             ) VALUES {}",
            trino_rows.join(",\n  ")
        );
        trino
            .execute_raw(sql)
            .await
            .context("inserting poc.heartbeats rows")?;
    }
    println!(
        "[heartbeats] inserted {pg_inserts} PG rows, {} Iceberg rows",
        trino_rows.len()
    );
    Ok(())
}

/// Format a UTC timestamp the way Trino's `TIMESTAMP '…'` literal expects.
/// Microsecond precision matches the table's `TIMESTAMP(6) WITH TIME ZONE`.
fn fmt_ts(ts: DateTime<Utc>) -> String {
    ts.format("%Y-%m-%d %H:%M:%S%.6f UTC").to_string()
}
