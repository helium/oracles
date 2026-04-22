use chrono::{DateTime, Duration, SubsecRound, Utc};
use file_store::aws_local::AwsLocal;
use file_store_oracles::{speedtest::CellSpeedtestIngestReport, FileType};
use helium_crypto::PublicKeyBinary;
use helium_proto::services::poc_mobile::{
    SpeedtestIngestReportV1, SpeedtestReqV1, SpeedtestVerificationResult,
    VerifiedSpeedtest as VerifiedSpeedtestProto,
};
use mobile_verifier::{
    backfill::BackfillOptions,
    iceberg::{self, speedtest::IcebergSpeedtest},
    speedtests::SpeedtestBackfiller,
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

fn make_iceberg_speedtest(seed: u8) -> IcebergSpeedtest {
    let pubkey = PublicKeyBinary::from(vec![seed, seed, seed]);
    let now = Utc::now().trunc_subsecs(3);
    let received = now + Duration::seconds(1);
    let report = CellSpeedtestIngestReport {
        received_timestamp: received,
        report: file_store_oracles::speedtest::CellSpeedtest {
            pubkey,
            serial: "test-serial".to_string(),
            timestamp: now,
            upload_speed: 10_000_000,
            download_speed: 100_000_000,
            latency: 25,
        },
    };
    IcebergSpeedtest::from(&report)
}

fn make_verified_speedtest_proto(
    timestamp: DateTime<Utc>,
    result: SpeedtestVerificationResult,
) -> VerifiedSpeedtestProto {
    let pubkey: PublicKeyBinary = "112NqN2WWMwtK29PMzRby62fDydBJfsCLkCAf392stdok48ovNT6"
        .parse()
        .unwrap();
    let ts_ms = timestamp.timestamp_millis() as u64;
    let ts_s = timestamp.timestamp() as u64;
    VerifiedSpeedtestProto {
        report: Some(SpeedtestIngestReportV1 {
            received_timestamp: ts_ms,
            report: Some(SpeedtestReqV1 {
                pub_key: pubkey.as_ref().to_vec(),
                serial: "test-serial".to_string(),
                timestamp: ts_s,
                upload_speed: 10_000_000,
                download_speed: 100_000_000,
                latency: 25,
                signature: vec![],
            }),
        }),
        result: result as i32,
        timestamp: ts_ms,
    }
}

// ── Pure write tests (no DB needed) ──────────────────────────────────────────

#[tokio::test]
async fn write_single_speedtest() -> anyhow::Result<()> {
    let harness = crate::common::setup_iceberg().await?;
    let writer = harness
        .get_table_writer::<IcebergSpeedtest>(iceberg::speedtest::TABLE_NAME)
        .await?;

    let record = make_iceberg_speedtest(1);

    writer
        .write_idempotent("test_single", vec![record.clone()])
        .await?;

    let all = iceberg::speedtest::get_all(harness.trino()).await?;
    assert_eq!(all, vec![record]);

    Ok(())
}

#[tokio::test]
async fn write_multiple_speedtests() -> anyhow::Result<()> {
    let harness = crate::common::setup_iceberg().await?;
    let writer = harness
        .get_table_writer::<IcebergSpeedtest>(iceberg::speedtest::TABLE_NAME)
        .await?;

    let records: Vec<IcebergSpeedtest> = (1u8..=5).map(make_iceberg_speedtest).collect();

    writer.write_idempotent("test_multiple", records).await?;

    let all = iceberg::speedtest::get_all(harness.trino()).await?;
    assert_eq!(all.len(), 5);

    Ok(())
}

#[tokio::test]
async fn idempotent_write_deduplicates() -> anyhow::Result<()> {
    let harness = crate::common::setup_iceberg().await?;
    let writer = harness
        .get_table_writer::<IcebergSpeedtest>(iceberg::speedtest::TABLE_NAME)
        .await?;

    let record = make_iceberg_speedtest(1);

    writer
        .write_idempotent("same_id", vec![record.clone()])
        .await?;
    writer
        .write_idempotent("same_id", vec![record.clone()])
        .await?;

    let all = iceberg::speedtest::get_all(harness.trino()).await?;
    assert_eq!(all.len(), 1, "second write with same id should be a no-op");

    Ok(())
}

// ── Backfill tests (DB needed for file-tracking state) ────────────────────────

#[sqlx::test]
async fn backfill_writes_speedtests_to_iceberg(pool: PgPool) -> anyhow::Result<()> {
    let harness = crate::common::setup_iceberg().await?;
    let writer = harness
        .get_table_writer::<IcebergSpeedtest>(iceberg::speedtest::TABLE_NAME)
        .await?;

    let awsl = AwsLocal::new().await;
    awsl.create_bucket().await?;

    let base_time = Utc::now() - Duration::hours(1);
    let start_time = base_time - Duration::minutes(1);
    let file1_time = base_time;
    let file2_time = base_time + Duration::minutes(5);
    let end_time = base_time + Duration::days(42);

    awsl.put_protos_at_time(
        FileType::VerifiedSpeedtest.to_string(),
        vec![make_verified_speedtest_proto(
            file1_time,
            SpeedtestVerificationResult::SpeedtestValid,
        )],
        file1_time,
    )
    .await?;

    awsl.put_protos_at_time(
        FileType::VerifiedSpeedtest.to_string(),
        vec![make_verified_speedtest_proto(
            file2_time,
            SpeedtestVerificationResult::SpeedtestValid,
        )],
        file2_time,
    )
    .await?;

    let opts = test_backfill_options("test-backfill", start_time, end_time);
    let (backfiller, server) =
        SpeedtestBackfiller::create(pool, awsl.bucket_client(), Some(writer), Some(opts)).await?;

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

    let rows = iceberg::speedtest::get_all(harness.trino()).await?;
    assert_eq!(rows.len(), 2, "expected 2 speedtests in iceberg");

    awsl.cleanup().await?;
    Ok(())
}

#[sqlx::test]
async fn backfill_filters_invalid_speedtests(pool: PgPool) -> anyhow::Result<()> {
    let harness = crate::common::setup_iceberg().await?;
    let writer = harness
        .get_table_writer::<IcebergSpeedtest>(iceberg::speedtest::TABLE_NAME)
        .await?;

    let awsl = AwsLocal::new().await;
    awsl.create_bucket().await?;

    let base_time = Utc::now() - Duration::hours(1);
    let start_time = base_time - Duration::minutes(1);
    let file_time = base_time;
    let end_time = base_time + Duration::days(42);

    awsl.put_protos_at_time(
        FileType::VerifiedSpeedtest.to_string(),
        vec![
            make_verified_speedtest_proto(file_time, SpeedtestVerificationResult::SpeedtestValid),
            make_verified_speedtest_proto(
                file_time,
                SpeedtestVerificationResult::SpeedtestValueOutOfBounds,
            ),
            make_verified_speedtest_proto(
                file_time,
                SpeedtestVerificationResult::SpeedtestGatewayNotFound,
            ),
        ],
        file_time,
    )
    .await?;

    let opts = test_backfill_options("test-backfill-filter", start_time, end_time);
    let (backfiller, server) =
        SpeedtestBackfiller::create(pool, awsl.bucket_client(), Some(writer), Some(opts)).await?;

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

    let rows = iceberg::speedtest::get_all(harness.trino()).await?;
    assert_eq!(
        rows.len(),
        1,
        "only valid speedtests should be written to iceberg"
    );

    awsl.cleanup().await?;
    Ok(())
}

#[sqlx::test]
async fn backfill_stops_at_timestamp(pool: PgPool) -> anyhow::Result<()> {
    let harness = crate::common::setup_iceberg().await?;
    let writer = harness
        .get_table_writer::<IcebergSpeedtest>(iceberg::speedtest::TABLE_NAME)
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
            FileType::VerifiedSpeedtest.to_string(),
            vec![make_verified_speedtest_proto(
                time,
                SpeedtestVerificationResult::SpeedtestValid,
            )],
            time,
        )
        .await
        .unwrap_or_else(|e| panic!("failed to put {label}: {e}"));
    }

    let opts = test_backfill_options("test-backfill-stop", start_time, stop_time);
    let (backfiller, server) =
        SpeedtestBackfiller::create(pool, awsl.bucket_client(), Some(writer), Some(opts)).await?;

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

    let rows = iceberg::speedtest::get_all(harness.trino()).await?;
    assert_eq!(
        rows.len(),
        2,
        "expected 2 speedtests (file3 after stop_after should be skipped)"
    );

    awsl.cleanup().await?;
    Ok(())
}
