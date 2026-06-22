use anyhow::{Context, Result};
use trino_client::Client as TrinoClient;

/// DDL to create everything `compare-trino` reads from. Column types and
/// partitioning mirror the canonical `TableDefinition::builder()` calls in
/// `mobile_verifier/src/iceberg/{heartbeat,speedtest,gateway_reward}.rs` so
/// the seed binary stays in lockstep with what production writes.
///
/// Trino REST quirks worth knowing:
/// - The catalog name comes from the `[trino]` setting (`catalog = "iceberg"`
///   by default for the docker-compose stack). We use unqualified
///   `poc.heartbeats` etc. and rely on the catalog being selected at session
///   level by the trino-client.
/// - `CREATE SCHEMA IF NOT EXISTS` requires Trino 422+. Polaris 1.3 ships
///   Iceberg 1.6 metadata which is compatible.
/// - `CREATE TABLE … WITH (partitioning = ARRAY['day(received_timestamp)'])`
///   is the Trino-Iceberg connector spelling for `PartitionDefinition::day`.
const CREATE_STATEMENTS: &[&str] = &[
    "CREATE SCHEMA IF NOT EXISTS poc",
    "CREATE SCHEMA IF NOT EXISTS rewards",
    "CREATE TABLE IF NOT EXISTS poc.heartbeats (
        hotspot_pubkey                  VARCHAR,
        received_timestamp              TIMESTAMP(6) WITH TIME ZONE,
        heartbeat_timestamp             TIMESTAMP(6) WITH TIME ZONE,
        device_type                     VARCHAR,
        lat                             DOUBLE,
        lon                             DOUBLE,
        coverage_object                 VARCHAR,
        location_validation_timestamp   TIMESTAMP(6) WITH TIME ZONE,
        distance_to_asserted            BIGINT,
        asserted_location               VARCHAR,
        location_trust_score_multiplier DOUBLE,
        location_source                 VARCHAR
     )
     WITH (
        format = 'PARQUET',
        partitioning = ARRAY['day(received_timestamp)']
     )",
    "CREATE TABLE IF NOT EXISTS poc.speedtests (
        hotspot_pubkey      VARCHAR,
        serial              VARCHAR,
        received_timestamp  TIMESTAMP(6) WITH TIME ZONE,
        timestamp           TIMESTAMP(6) WITH TIME ZONE,
        upload_speed        BIGINT,
        download_speed      BIGINT,
        latency             BIGINT
     )
     WITH (
        format = 'PARQUET',
        partitioning = ARRAY['day(received_timestamp)']
     )",
    "CREATE TABLE IF NOT EXISTS rewards.data_transfer (
        hotspot_key         VARCHAR,
        dc_transfer_reward  BIGINT,
        rewardable_bytes    BIGINT,
        price               BIGINT,
        start_period        TIMESTAMP(6) WITH TIME ZONE,
        end_period          TIMESTAMP(6) WITH TIME ZONE
     )
     WITH (
        format = 'PARQUET',
        partitioning = ARRAY['day(start_period)']
     )",
];

pub async fn run(trino: &TrinoClient) -> Result<()> {
    for stmt in CREATE_STATEMENTS {
        let preview = first_line(stmt);
        println!("[iceberg] {preview}");
        trino
            .execute_raw(*stmt)
            .await
            .with_context(|| format!("running: {preview}"))?;
    }
    Ok(())
}

fn first_line(sql: &str) -> &str {
    sql.lines().next().unwrap_or("").trim()
}
