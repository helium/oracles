use chrono::{DateTime, Duration, SubsecRound, Utc};
use file_store::aws_local::AwsLocal;
use file_store_oracles::{
    unique_connections::{
        proto::VerifiedUniqueConnectionsIngestReportStatus, UniqueConnectionReq,
        UniqueConnectionsIngestReport, VerifiedUniqueConnectionsIngestReport,
    },
    FileType,
};
use helium_crypto::PublicKeyBinary;
use helium_proto::services::poc_mobile::{
    UniqueConnectionsIngestReportV1, UniqueConnectionsReqV1,
    VerifiedUniqueConnectionsIngestReportV1,
};
use mobile_verifier::{
    backfill::BackfillOptions,
    iceberg::{self, unique_connections::IcebergUniqueConnections},
    unique_connections::UniqueConnectionsBackfiller,
};
use sqlx::PgPool;

const TEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

fn test_backfill_options(
    process_name: &str,
    start_after: DateTime<Utc>,
    stop_after: DateTime<Utc>,
) -> BackfillOptions {
    BackfillOptions {
        process_name: process_name.to_string(),
        start_after,
        stop_after,
        poll_duration: Some(std::time::Duration::from_millis(100)),
        idle_timeout: Some(std::time::Duration::from_millis(500)),
    }
}

fn truncated_now() -> DateTime<Utc> {
    Utc::now().trunc_subsecs(3)
}

fn make_iceberg_unique_connections(seed: u8) -> IcebergUniqueConnections {
    let pubkey = PublicKeyBinary::from(vec![seed, seed, seed]);
    let carrier_key = PublicKeyBinary::from(vec![seed + 1, seed + 1]);
    let now = truncated_now();
    let start = now - Duration::days(7);
    let report = VerifiedUniqueConnectionsIngestReport {
        timestamp: now,
        report: UniqueConnectionsIngestReport {
            received_timestamp: now,
            report: UniqueConnectionReq {
                pubkey,
                start_timestamp: start,
                end_timestamp: now,
                unique_connections: seed as u64 * 10,
                timestamp: now - Duration::minutes(5),
                carrier_key,
                signature: vec![],
            },
        },
        status: VerifiedUniqueConnectionsIngestReportStatus::Valid,
    };
    IcebergUniqueConnections::from(&report)
}

fn make_verified_unique_connections_proto(
    timestamp: DateTime<Utc>,
    status: VerifiedUniqueConnectionsIngestReportStatus,
) -> VerifiedUniqueConnectionsIngestReportV1 {
    let pubkey: PublicKeyBinary = "112NqN2WWMwtK29PMzRby62fDydBJfsCLkCAf392stdok48ovNT6"
        .parse()
        .unwrap();
    let carrier_key = PublicKeyBinary::from(vec![1, 2, 3, 4]);
    let ts_ms = timestamp.timestamp_millis() as u64;
    let start_ms = (timestamp - Duration::days(7)).timestamp_millis() as u64;
    VerifiedUniqueConnectionsIngestReportV1 {
        timestamp: ts_ms,
        report: Some(UniqueConnectionsIngestReportV1 {
            received_timestamp: ts_ms,
            report: Some(UniqueConnectionsReqV1 {
                pubkey: pubkey.as_ref().to_vec(),
                start_timestamp: start_ms,
                end_timestamp: ts_ms,
                unique_connections: 42,
                timestamp: ts_ms,
                carrier_key: carrier_key.as_ref().to_vec(),
                signature: vec![],
            }),
        }),
        status: status as i32,
    }
}

// ── Pure write tests (no DB needed) ──────────────────────────────────────────

#[tokio::test]
async fn write_single_unique_connections() -> anyhow::Result<()> {
    let harness = crate::common::setup_iceberg().await?;
    let writer = harness
        .get_table_writer::<IcebergUniqueConnections>(iceberg::unique_connections::TABLE_NAME)
        .await?;

    let record = make_iceberg_unique_connections(1);

    writer
        .write_idempotent("test_single", vec![record.clone()])
        .await?;

    let all = iceberg::unique_connections::get_all(harness.trino()).await?;
    assert_eq!(all, vec![record]);

    Ok(())
}

#[tokio::test]
async fn write_multiple_unique_connections() -> anyhow::Result<()> {
    let harness = crate::common::setup_iceberg().await?;
    let writer = harness
        .get_table_writer::<IcebergUniqueConnections>(iceberg::unique_connections::TABLE_NAME)
        .await?;

    let records: Vec<IcebergUniqueConnections> =
        (1u8..=5).map(make_iceberg_unique_connections).collect();

    writer.write_idempotent("test_multiple", records).await?;

    let all = iceberg::unique_connections::get_all(harness.trino()).await?;
    assert_eq!(all.len(), 5);

    Ok(())
}

#[tokio::test]
async fn idempotent_write_deduplicates() -> anyhow::Result<()> {
    let harness = crate::common::setup_iceberg().await?;
    let writer = harness
        .get_table_writer::<IcebergUniqueConnections>(iceberg::unique_connections::TABLE_NAME)
        .await?;

    let record = make_iceberg_unique_connections(1);

    writer
        .write_idempotent("same_id", vec![record.clone()])
        .await?;
    writer
        .write_idempotent("same_id", vec![record.clone()])
        .await?;

    let all = iceberg::unique_connections::get_all(harness.trino()).await?;
    assert_eq!(all.len(), 1, "second write with same id should be a no-op");

    Ok(())
}

// ── Backfill tests (DB needed for file-tracking state) ────────────────────────

#[sqlx::test]
async fn backfill_writes_unique_connections_to_iceberg(pool: PgPool) -> anyhow::Result<()> {
    let harness = crate::common::setup_iceberg().await?;
    let writer = harness
        .get_table_writer::<IcebergUniqueConnections>(iceberg::unique_connections::TABLE_NAME)
        .await?;

    let awsl = AwsLocal::new().await;
    awsl.create_bucket().await?;

    let base_time = Utc::now() - Duration::hours(1);
    let start_time = base_time - Duration::minutes(1);
    let file1_time = base_time;
    let file2_time = base_time + Duration::minutes(5);
    let end_time = base_time + Duration::days(42);

    awsl.put_protos_at_time(
        FileType::VerifiedUniqueConnectionsReport.to_string(),
        vec![make_verified_unique_connections_proto(
            file1_time,
            VerifiedUniqueConnectionsIngestReportStatus::Valid,
        )],
        file1_time,
    )
    .await?;

    awsl.put_protos_at_time(
        FileType::VerifiedUniqueConnectionsReport.to_string(),
        vec![make_verified_unique_connections_proto(
            file2_time,
            VerifiedUniqueConnectionsIngestReportStatus::Valid,
        )],
        file2_time,
    )
    .await?;

    let opts = test_backfill_options("test-backfill", start_time, end_time);
    let (backfiller, server) =
        UniqueConnectionsBackfiller::create(pool, awsl.bucket_client(), Some(writer), Some(opts))
            .await?;

    tokio::time::timeout(
        TEST_TIMEOUT,
        task_manager::TaskManager::builder()
            .add_task(server)
            .add_task(backfiller)
            .build()
            .start(),
    )
    .await
    .map_err(|_| anyhow::anyhow!("backfill timed out after {:?}", TEST_TIMEOUT))??;

    let rows = iceberg::unique_connections::get_all(harness.trino()).await?;
    assert_eq!(rows.len(), 2, "expected 2 unique_connections in iceberg");

    awsl.cleanup().await?;
    Ok(())
}

#[sqlx::test]
async fn backfill_filters_invalid_unique_connections(pool: PgPool) -> anyhow::Result<()> {
    let harness = crate::common::setup_iceberg().await?;
    let writer = harness
        .get_table_writer::<IcebergUniqueConnections>(iceberg::unique_connections::TABLE_NAME)
        .await?;

    let awsl = AwsLocal::new().await;
    awsl.create_bucket().await?;

    let base_time = Utc::now() - Duration::hours(1);
    let start_time = base_time - Duration::minutes(1);
    let file_time = base_time;
    let end_time = base_time + Duration::days(42);

    awsl.put_protos_at_time(
        FileType::VerifiedUniqueConnectionsReport.to_string(),
        vec![
            make_verified_unique_connections_proto(
                file_time,
                VerifiedUniqueConnectionsIngestReportStatus::Valid,
            ),
            make_verified_unique_connections_proto(
                file_time,
                VerifiedUniqueConnectionsIngestReportStatus::InvalidCarrierKey,
            ),
        ],
        file_time,
    )
    .await?;

    let opts = test_backfill_options("test-backfill-filter", start_time, end_time);
    let (backfiller, server) =
        UniqueConnectionsBackfiller::create(pool, awsl.bucket_client(), Some(writer), Some(opts))
            .await?;

    tokio::time::timeout(
        TEST_TIMEOUT,
        task_manager::TaskManager::builder()
            .add_task(server)
            .add_task(backfiller)
            .build()
            .start(),
    )
    .await
    .map_err(|_| anyhow::anyhow!("backfill timed out after {:?}", TEST_TIMEOUT))??;

    let rows = iceberg::unique_connections::get_all(harness.trino()).await?;
    assert_eq!(
        rows.len(),
        1,
        "only valid unique_connections should be written to iceberg"
    );

    awsl.cleanup().await?;
    Ok(())
}

#[sqlx::test]
async fn backfill_stops_at_timestamp(pool: PgPool) -> anyhow::Result<()> {
    let harness = crate::common::setup_iceberg().await?;
    let writer = harness
        .get_table_writer::<IcebergUniqueConnections>(iceberg::unique_connections::TABLE_NAME)
        .await?;

    let awsl = AwsLocal::new().await;
    awsl.create_bucket().await?;

    let base_time = Utc::now() - Duration::hours(2);
    let start_time = base_time - Duration::minutes(1);
    let file1_time = base_time;
    let file2_time = base_time + Duration::minutes(30);
    let stop_time = base_time + Duration::minutes(45);
    let file3_time = base_time + Duration::hours(1); // should be skipped

    for (time, label) in [
        (file1_time, "file1"),
        (file2_time, "file2"),
        (file3_time, "file3"),
    ] {
        awsl.put_protos_at_time(
            FileType::VerifiedUniqueConnectionsReport.to_string(),
            vec![make_verified_unique_connections_proto(
                time,
                VerifiedUniqueConnectionsIngestReportStatus::Valid,
            )],
            time,
        )
        .await
        .unwrap_or_else(|e| panic!("failed to put {label}: {e}"));
    }

    let opts = test_backfill_options("test-backfill-stop", start_time, stop_time);
    let (backfiller, server) =
        UniqueConnectionsBackfiller::create(pool, awsl.bucket_client(), Some(writer), Some(opts))
            .await?;

    tokio::time::timeout(
        TEST_TIMEOUT,
        task_manager::TaskManager::builder()
            .add_task(server)
            .add_task(backfiller)
            .build()
            .start(),
    )
    .await
    .map_err(|_| anyhow::anyhow!("backfill timed out after {:?}", TEST_TIMEOUT))??;

    let rows = iceberg::unique_connections::get_all(harness.trino()).await?;
    assert_eq!(
        rows.len(),
        2,
        "expected 2 unique_connections (file3 after stop_after should be skipped)"
    );

    awsl.cleanup().await?;
    Ok(())
}
