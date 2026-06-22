use super::plan::{Bucket, Plan};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use sqlx::PgPool;
use trino_client::Client as TrinoClient;

/// Default rewardable-bytes / DCs for a matched-bucket hotspot.
const MATCH_BYTES: u64 = 1_000_000_000; // 1 GB
const MATCH_DCS: u64 = 50_000;
const MATCH_PRICE: u64 = 100_000_000;

pub async fn seed(mv_pg: &PgPool, mpv_pg: &PgPool, trino: &TrinoClient, plan: &Plan) -> Result<()> {
    println!("[data_sessions] clearing window in PG (mv + mpv) + Iceberg");
    // PG mobile_verifier: scoped to burn_timestamp window so we don't trample
    // unrelated rows the user may have lying around.
    sqlx::query(
        "DELETE FROM hotspot_data_transfer_sessions \
         WHERE burn_timestamp >= $1 AND burn_timestamp < $2",
    )
    .bind(plan.epoch_start)
    .bind(plan.epoch_end)
    .execute(mv_pg)
    .await
    .context("clearing mv hotspot_data_transfer_sessions")?;

    // PG mobile_packet_verifier: `data_transfer_sessions` has no window column,
    // and `pending_burns::initialize` is a whole-table aggregate. Clear by
    // pubkey for our seeded set only.
    let pubkeys: Vec<String> = plan.hotspots.iter().map(|h| h.pubkey.clone()).collect();
    sqlx::query("DELETE FROM data_transfer_sessions WHERE pub_key = ANY($1)")
        .bind(&pubkeys)
        .execute(mpv_pg)
        .await
        .context("clearing mpv data_transfer_sessions")?;

    // Iceberg: scope to the same window.
    trino
        .execute_raw(format!(
            "DELETE FROM rewards.data_transfer \
             WHERE start_period >= TIMESTAMP '{}' AND start_period < TIMESTAMP '{}'",
            fmt_ts(plan.epoch_start),
            fmt_ts(plan.epoch_end),
        ))
        .await
        .context("clearing iceberg rewards.data_transfer window")?;

    let burn_ts = plan.epoch_end - chrono::Duration::minutes(30);
    let received_ts = burn_ts - chrono::Duration::minutes(5);

    let mut mv_inserts = 0usize;
    let mut mpv_inserts = 0usize;
    let mut trino_rows: Vec<String> = Vec::new();

    for h in &plan.hotspots {
        // Bucket value plan:
        // - match     : same bytes/DCs on PG (mv + mpv) and Iceberg
        // - mismatch  : Iceberg has double the bytes/DCs
        // - pg_only   : PG only — Iceberg row skipped
        // - trino_only: Iceberg only — PG rows skipped
        let (pg_bytes, pg_dcs, trino_bytes, trino_dcs) = match h.bucket {
            Bucket::Match => (MATCH_BYTES, MATCH_DCS, MATCH_BYTES, MATCH_DCS),
            Bucket::Mismatch => (MATCH_BYTES, MATCH_DCS, MATCH_BYTES * 2, MATCH_DCS * 2),
            Bucket::PgOnly => (MATCH_BYTES, MATCH_DCS, 0, 0),
            Bucket::TrinoOnly => (0, 0, MATCH_BYTES, MATCH_DCS),
        };
        let pg_enabled = !matches!(h.bucket, Bucket::TrinoOnly);
        let trino_enabled = !matches!(h.bucket, Bucket::PgOnly);

        if pg_enabled {
            sqlx::query(
                "INSERT INTO hotspot_data_transfer_sessions
                 (pub_key, payer, upload_bytes, download_bytes, num_dcs,
                  received_timestamp, rewardable_bytes, burn_timestamp)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                 ON CONFLICT (pub_key, payer, burn_timestamp) DO UPDATE SET
                    upload_bytes = EXCLUDED.upload_bytes,
                    download_bytes = EXCLUDED.download_bytes,
                    num_dcs = EXCLUDED.num_dcs,
                    received_timestamp = EXCLUDED.received_timestamp,
                    rewardable_bytes = EXCLUDED.rewardable_bytes",
            )
            .bind(&h.pubkey)
            .bind(&h.payer)
            .bind((pg_bytes / 2) as i64)
            .bind((pg_bytes / 2) as i64)
            .bind(pg_dcs as i64)
            .bind(received_ts)
            .bind(pg_bytes as i64)
            .bind(burn_ts)
            .execute(mv_pg)
            .await
            .context("inserting hotspot_data_transfer_sessions row")?;
            mv_inserts += 1;

            // mpv pending burns mirror: same pubkey/payer/rewardable_bytes
            sqlx::query(
                "INSERT INTO data_transfer_sessions
                 (pub_key, payer, uploaded_bytes, downloaded_bytes, rewardable_bytes,
                  first_timestamp, last_timestamp)
                 VALUES ($1, $2, $3, $4, $5, $6, $7)
                 ON CONFLICT (pub_key, payer) DO UPDATE SET
                    uploaded_bytes = EXCLUDED.uploaded_bytes,
                    downloaded_bytes = EXCLUDED.downloaded_bytes,
                    rewardable_bytes = EXCLUDED.rewardable_bytes,
                    first_timestamp = EXCLUDED.first_timestamp,
                    last_timestamp = EXCLUDED.last_timestamp",
            )
            .bind(&h.pubkey)
            .bind(&h.payer)
            .bind((pg_bytes / 2) as i64)
            .bind((pg_bytes / 2) as i64)
            .bind(pg_bytes as i64)
            .bind(received_ts)
            .bind(burn_ts)
            .execute(mpv_pg)
            .await
            .context("inserting mpv data_transfer_sessions row")?;
            mpv_inserts += 1;
        }

        if trino_enabled {
            trino_rows.push(format!(
                "('{hotspot}', {dc}, {bytes}, {price}, TIMESTAMP '{start}', TIMESTAMP '{end}')",
                hotspot = h.pubkey,
                dc = trino_dcs as i64,
                bytes = trino_bytes as i64,
                price = MATCH_PRICE as i64,
                start = fmt_ts(plan.epoch_start),
                end = fmt_ts(plan.epoch_end),
            ));
        }
    }

    if !trino_rows.is_empty() {
        let sql = format!(
            "INSERT INTO rewards.data_transfer
                (hotspot_key, dc_transfer_reward, rewardable_bytes, price,
                 start_period, end_period)
             VALUES {}",
            trino_rows.join(",\n  ")
        );
        trino
            .execute_raw(sql)
            .await
            .context("inserting rewards.data_transfer rows")?;
    }
    println!(
        "[data_sessions] mv={mv_inserts} mpv={mpv_inserts} iceberg={}",
        trino_rows.len()
    );
    Ok(())
}

fn fmt_ts(ts: DateTime<Utc>) -> String {
    ts.format("%Y-%m-%d %H:%M:%S%.6f UTC").to_string()
}
