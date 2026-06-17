//! Open a stream on `iceberg.poc.heartbeats` and print each event as a JSON
//! document on stdout — snapshot metadata (id, sequence number, operation,
//! commit time, row count), running totals, and a sample decoded row — plus a
//! final summary document. Status messages go to stderr so stdout stays a
//! clean stream of JSON docs (pipe it to `jq`).
//!
//! Run against the local docker stack (`docker compose up`). Requires the
//! `sqlite` feature (the watermark is persisted to a SQLite db file):
//!
//! ```sh
//! cargo run -p helium-iceberg --example stream_heartbeats --features sqlite
//! ```
//!
//! Connection defaults target the local Polaris/RustFS stack and can be
//! overridden with env vars:
//!   ICEBERG_CATALOG_URI, ICEBERG_CATALOG_NAME, ICEBERG_WAREHOUSE,
//!   ICEBERG_CREDENTIAL, ICEBERG_SCOPE,
//!   S3_ENDPOINT, S3_ACCESS_KEY, S3_SECRET_KEY, S3_REGION,
//!   WATERMARK_DB (path to the per-poller SQLite db, default
//!   `./heartbeats-watermark.db`)
//!
//! Rows are decoded into the typed [`Heartbeat`] struct below — the stream is
//! generic over any `T: serde::de::DeserializeOwned`, so you just define a
//! struct whose fields match the table's columns. The watermark is persisted in
//! a SQLite db (one file per poller, as you'd mount on a PVC), so re-running
//! resumes where the last run left off instead of replaying from the start.

use std::time::Duration;

use anyhow::Result;
use chrono::{DateTime, Utc};
use futures::StreamExt;
use helium_iceberg::{
    continuous, open_watermark_db, AuthConfig, Catalog, IcebergStream, S3Config, Settings,
};
use serde::{Deserialize, Serialize};
use sqlx::SqlitePool;

/// A row of `iceberg.poc.heartbeats`. Field names and types match the table's
/// columns; the poller decodes each Arrow row into this via serde
/// (`batch_to_records`).
///
/// Timestamp columns arrive as RFC3339 strings, which chrono deserializes
/// directly. Nullable columns use `Option<_>` with `#[serde(default)]` because
/// the Arrow→JSON bridge omits keys whose value is null.
// Several fields are only surfaced via the `Debug` print, which dead-code
// analysis intentionally ignores — silence the per-field warnings.
#[derive(Debug, Deserialize, Serialize)]
struct Heartbeat {
    hotspot_pubkey: String,
    received_timestamp: DateTime<Utc>,
    heartbeat_timestamp: DateTime<Utc>,
    #[serde(default)]
    device_type: Option<String>,
    lat: f64,
    lon: f64,
    coverage_object: String,
    #[serde(default)]
    location_validation_timestamp: Option<DateTime<Utc>>,
    #[serde(default)]
    distance_to_asserted: Option<i64>,
    #[serde(default)]
    asserted_location: Option<String>,
    location_trust_score_multiplier: f64,
    location_source: String,
}

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

/// Local-docker defaults, mirroring `infra/trino/etc/catalog/iceberg.properties`.
fn settings_from_env() -> Settings {
    Settings {
        catalog_uri: env_or("ICEBERG_CATALOG_URI", "http://localhost:8181/api/catalog"),
        catalog_name: env_or("ICEBERG_CATALOG_NAME", "iceberg"),
        warehouse: Some(env_or("ICEBERG_WAREHOUSE", "iceberg")),
        auth: AuthConfig {
            credential: Some(env_or("ICEBERG_CREDENTIAL", "root:s3cr3t")),
            scope: Some(env_or("ICEBERG_SCOPE", "PRINCIPAL_ROLE:ALL")),
            ..Default::default()
        },
        s3: S3Config {
            endpoint: Some(env_or("S3_ENDPOINT", "http://localhost:9000")),
            access_key_id: Some(env_or("S3_ACCESS_KEY", "admin")),
            secret_access_key: Some(env_or("S3_SECRET_KEY", "admin")),
            region: Some(env_or("S3_REGION", "us-east-1")),
            path_style_access: Some(true),
            disable_config_load: Some(true),
        },
        properties: Default::default(),
    }
}

/// Running tally of what's been streamed, carried across events.
#[derive(Default)]
struct Totals {
    snapshots: u64,
    rows: u64,
    first_seq: Option<i64>,
    last_seq: Option<i64>,
}

/// Decode one snapshot's rows, advance the watermark, and print the event as a
/// single JSON document: snapshot metadata, running totals, and a sample row.
///
/// The watermark advance is committed in the same transaction the consumer
/// would use for its own work — here there's no other side effect, so the
/// commit just persists the watermark. A crash before `commit` re-runs the
/// snapshot on restart (at-least-once).
async fn print_event(
    event: IcebergStream<Heartbeat>,
    pool: &SqlitePool,
    totals: &mut Totals,
) -> Result<()> {
    let snapshot = event.snapshot.clone();

    let mut txn = pool.begin().await?;
    // `into_stream` records the watermark into `txn`, then yields decoded rows.
    let mut rows = event.into_stream(&mut txn).await?;
    let mut row_count: u64 = 0;
    let mut sample: Option<Heartbeat> = None;
    while let Some(heartbeat) = rows.next().await {
        if sample.is_none() {
            sample = Some(heartbeat);
        }
        row_count += 1;
    }
    drop(rows);
    txn.commit().await?;

    totals.snapshots += 1;
    totals.rows += row_count;
    totals.first_seq.get_or_insert(snapshot.sequence_number);
    totals.last_seq = Some(snapshot.sequence_number);

    let doc = serde_json::json!({
        "snapshot_id": snapshot.snapshot_id,
        "sequence_number": snapshot.sequence_number,
        "operation": format!("{:?}", snapshot.operation),
        "committed": snapshot.timestamp,
        "row_count": row_count,
        "running_totals": {
            "snapshots_processed": totals.snapshots,
            "rows_processed": totals.rows,
        },
        "sample": sample,
    });
    println!("{}", serde_json::to_string_pretty(&doc)?);
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let catalog = Catalog::connect(&settings_from_env()).await?;

    // Per-poller SQLite watermark db (the file you'd mount on a PVC). Opening it
    // applies the bundled migration; the pool is shared so we both seed the
    // poller and record progress against the same db.
    let db_path = env_or("WATERMARK_DB", "heartbeats-watermark.db");
    let pool = open_watermark_db(&db_path).await?;
    eprintln!("watermark db: {db_path}");

    let (mut rx, server) = continuous::<Heartbeat, SqlitePool>()
        .catalog(catalog)
        .namespace("poc")
        .table("heartbeats")
        .state(pool.clone())
        // Start from the beginning of the table's history. Swap for
        // `.lookback_max(Duration::from_secs(24 * 60 * 60))` to only see the
        // last day, or `.lookback_start_after(some_datetime)`.
        .poll_duration(Duration::from_secs(5))
        .create()
        .await?;

    // Drive the poller in the background. Triggering stops it.
    let (trigger, listener) = triggered::trigger();
    let server = tokio::spawn(server.run(listener));

    eprintln!("streaming iceberg.poc.heartbeats — press Ctrl-C to stop");

    let mut totals = Totals::default();

    loop {
        tokio::select! {
            maybe_event = rx.recv() => {
                let Some(event) = maybe_event else { break };
                print_event(event, &pool, &mut totals).await?;
            }
            _ = tokio::signal::ctrl_c() => {
                eprintln!("shutdown requested");
                trigger.trigger();
                break;
            }
        }
    }

    // Drain any snapshots the poller already queued before shutdown.
    while let Ok(event) = rx.try_recv() {
        print_event(event, &pool, &mut totals).await?;
    }

    server.await??;

    let summary = serde_json::json!({
        "summary": {
            "snapshots_processed": totals.snapshots,
            "rows_processed": totals.rows,
            "sequence_range": totals.first_seq.zip(totals.last_seq)
                .map(|(first, last)| [first, last]),
        }
    });
    println!("{}", serde_json::to_string_pretty(&summary)?);
    Ok(())
}
