use chrono::{DateTime, Duration, SubsecRound, Utc};
use file_store::{aws_local::AwsLocal, traits::TimestampEncode};
use file_store_oracles::FileType;
use helium_crypto::PublicKeyBinary;
use helium_proto::services::poc_mobile::{Speedtest, SpeedtestAvg, SpeedtestAvgValidity};
use mobile_verifier::{
    backfill::BackfillOptions,
    iceberg::{self, speedtest_avg::IcebergSpeedtestAvg},
    speedtests_average::SpeedtestAvgBackfiller,
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

fn make_iceberg_speedtest_avg(seed: u8) -> IcebergSpeedtestAvg {
    let pubkey = PublicKeyBinary::from(vec![seed, seed, seed]);
    let now = Utc::now().trunc_subsecs(3);
    IcebergSpeedtestAvg {
        hotspot_pubkey: pubkey.to_string(),
        upload_speed_avg_bps: 10_000_000,
        download_speed_avg_bps: 100_000_000,
        latency_avg_ms: 25,
        reward_multiplier: 1.0,
        sample_count: 2,
        timestamp: now.into(),
        speedtests: vec![],
    }
}

fn make_speedtest_avg_proto(
    timestamp: DateTime<Utc>,
    validity: SpeedtestAvgValidity,
) -> SpeedtestAvg {
    let pubkey: PublicKeyBinary = "112NqN2WWMwtK29PMzRby62fDydBJfsCLkCAf392stdok48ovNT6"
        .parse()
        .unwrap();
    let ts_s = timestamp.encode_timestamp();
    SpeedtestAvg {
        pub_key: pubkey.as_ref().to_vec(),
        upload_speed_avg_bps: 10_000_000,
        download_speed_avg_bps: 100_000_000,
        latency_avg_ms: 25,
        timestamp: ts_s,
        speedtests: vec![Speedtest {
            upload_speed_bps: 11_000_000,
            download_speed_bps: 110_000_000,
            latency_ms: 20,
            timestamp: ts_s,
        }],
        validity: validity as i32,
        reward_multiplier: 1.0,
    }
}

// ── Pure write tests (no DB needed) ──────────────────────────────────────────

#[tokio::test]
async fn write_single_speedtest_avg() -> anyhow::Result<()> {
    let harness = crate::common::setup_iceberg().await?;
    let writer = harness
        .get_table_writer::<IcebergSpeedtestAvg>(iceberg::speedtest_avg::TABLE_NAME)
        .await?;

    let record = make_iceberg_speedtest_avg(1);

    writer
        .write_idempotent("test_single", vec![record.clone()])
        .await?;

    let all = iceberg::speedtest_avg::get_all(harness.trino()).await?;
    assert_eq!(all, vec![record]);

    Ok(())
}

#[tokio::test]
async fn write_multiple_speedtest_avgs() -> anyhow::Result<()> {
    let harness = crate::common::setup_iceberg().await?;
    let writer = harness
        .get_table_writer::<IcebergSpeedtestAvg>(iceberg::speedtest_avg::TABLE_NAME)
        .await?;

    let records: Vec<IcebergSpeedtestAvg> = (1u8..=5).map(make_iceberg_speedtest_avg).collect();

    writer.write_idempotent("test_multiple", records).await?;

    let all = iceberg::speedtest_avg::get_all(harness.trino()).await?;
    assert_eq!(all.len(), 5);

    Ok(())
}

#[tokio::test]
async fn idempotent_write_deduplicates() -> anyhow::Result<()> {
    let harness = crate::common::setup_iceberg().await?;
    let writer = harness
        .get_table_writer::<IcebergSpeedtestAvg>(iceberg::speedtest_avg::TABLE_NAME)
        .await?;

    let record = make_iceberg_speedtest_avg(1);

    writer
        .write_idempotent("same_id", vec![record.clone()])
        .await?;
    writer
        .write_idempotent("same_id", vec![record.clone()])
        .await?;

    let all = iceberg::speedtest_avg::get_all(harness.trino()).await?;
    assert_eq!(all.len(), 1, "second write with same id should be a no-op");

    Ok(())
}

// ── Backfill tests (DB needed for file-tracking state) ────────────────────────

#[sqlx::test]
async fn backfill_writes_speedtest_avgs_to_iceberg(pool: PgPool) -> anyhow::Result<()> {
    let harness = crate::common::setup_iceberg().await?;
    let (writer, writer_task, _spool_dir) =
        crate::common::make_batched_writer::<IcebergSpeedtestAvg>(
            &harness,
            iceberg::speedtest_avg::table_definition()?,
        )
        .await?;

    let awsl = AwsLocal::new().await;
    awsl.create_bucket().await?;

    let base_time = Utc::now() - Duration::hours(1);
    let start_time = base_time - Duration::minutes(1);
    let file1_time = base_time;
    let file2_time = base_time + Duration::minutes(5);
    let end_time = base_time + Duration::days(42);

    awsl.put_protos_at_time(
        FileType::SpeedtestAvg.to_string(),
        vec![make_speedtest_avg_proto(
            file1_time,
            SpeedtestAvgValidity::Valid,
        )],
        file1_time,
    )
    .await?;

    awsl.put_protos_at_time(
        FileType::SpeedtestAvg.to_string(),
        vec![make_speedtest_avg_proto(
            file2_time,
            SpeedtestAvgValidity::Valid,
        )],
        file2_time,
    )
    .await?;

    let opts = test_backfill_options("test-avg-backfill", start_time, end_time);
    let (backfiller, server) =
        SpeedtestAvgBackfiller::create_batched(pool, awsl.bucket_client(), writer, opts).await?;

    tokio::time::timeout(
        TEST_TIMEOUT,
        task_manager::TaskManager::builder()
            .add_task(writer_task)
            .add_task(server)
            .add_task(backfiller)
            .build()
            .start(),
    )
    .await
    .map_err(|_| anyhow::anyhow!("backfill timed out after {:?}", TEST_TIMEOUT))??;

    let rows = iceberg::speedtest_avg::get_all(harness.trino()).await?;
    assert_eq!(rows.len(), 2, "expected 2 speedtest_avgs in iceberg");

    awsl.cleanup().await?;
    Ok(())
}

#[sqlx::test]
async fn backfill_stops_at_timestamp(pool: PgPool) -> anyhow::Result<()> {
    let harness = crate::common::setup_iceberg().await?;
    let (writer, writer_task, _spool_dir) =
        crate::common::make_batched_writer::<IcebergSpeedtestAvg>(
            &harness,
            iceberg::speedtest_avg::table_definition()?,
        )
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
            FileType::SpeedtestAvg.to_string(),
            vec![make_speedtest_avg_proto(time, SpeedtestAvgValidity::Valid)],
            time,
        )
        .await
        .unwrap_or_else(|e| panic!("failed to put {label}: {e}"));
    }

    let opts = test_backfill_options("test-avg-backfill-stop", start_time, stop_time);
    let (backfiller, server) =
        SpeedtestAvgBackfiller::create_batched(pool, awsl.bucket_client(), writer, opts).await?;

    tokio::time::timeout(
        TEST_TIMEOUT,
        task_manager::TaskManager::builder()
            .add_task(writer_task)
            .add_task(server)
            .add_task(backfiller)
            .build()
            .start(),
    )
    .await
    .map_err(|_| anyhow::anyhow!("backfill timed out after {:?}", TEST_TIMEOUT))??;

    let rows = iceberg::speedtest_avg::get_all(harness.trino()).await?;
    assert_eq!(
        rows.len(),
        2,
        "expected 2 speedtest_avgs (file3 after stop_after should be skipped)"
    );

    awsl.cleanup().await?;
    Ok(())
}

#[sqlx::test]
async fn backfill_filters_invalid_speedtest_avgs(pool: PgPool) -> anyhow::Result<()> {
    let harness = crate::common::setup_iceberg().await?;
    let (writer, writer_task, _spool_dir) =
        crate::common::make_batched_writer::<IcebergSpeedtestAvg>(
            &harness,
            iceberg::speedtest_avg::table_definition()?,
        )
        .await?;

    let awsl = AwsLocal::new().await;
    awsl.create_bucket().await?;

    let base_time = Utc::now() - Duration::hours(1);
    let start_time = base_time - Duration::minutes(1);
    let file_time = base_time;
    let end_time = base_time + Duration::days(42);

    awsl.put_protos_at_time(
        FileType::SpeedtestAvg.to_string(),
        vec![
            make_speedtest_avg_proto(file_time, SpeedtestAvgValidity::Valid),
            make_speedtest_avg_proto(file_time, SpeedtestAvgValidity::TooFewSamples),
            make_speedtest_avg_proto(file_time, SpeedtestAvgValidity::SlowDownloadSpeed),
            make_speedtest_avg_proto(file_time, SpeedtestAvgValidity::SlowUploadSpeed),
            make_speedtest_avg_proto(file_time, SpeedtestAvgValidity::HighLatency),
        ],
        file_time,
    )
    .await?;

    let opts = test_backfill_options("test-avg-backfill-filter", start_time, end_time);
    let (backfiller, server) =
        SpeedtestAvgBackfiller::create_batched(pool, awsl.bucket_client(), writer, opts).await?;

    tokio::time::timeout(
        TEST_TIMEOUT,
        task_manager::TaskManager::builder()
            .add_task(writer_task)
            .add_task(server)
            .add_task(backfiller)
            .build()
            .start(),
    )
    .await
    .map_err(|_| anyhow::anyhow!("backfill timed out after {:?}", TEST_TIMEOUT))??;

    let rows = iceberg::speedtest_avg::get_all(harness.trino()).await?;
    assert_eq!(
        rows.len(),
        1,
        "only valid speedtest_avgs should be written to iceberg"
    );

    awsl.cleanup().await?;
    Ok(())
}
