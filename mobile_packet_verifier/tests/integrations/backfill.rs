use chrono::{DateTime, Duration, Utc};
use file_store::aws_local::AwsLocal;
use file_store_oracles::{
    mobile_session::{DataTransferEvent, DataTransferSessionIngestReport, DataTransferSessionReq},
    FileType,
};
use helium_crypto::PublicKeyBinary;
use helium_proto::services::poc_mobile::{
    CarrierIdV2, DataTransferRadioAccessTechnology, DataTransferSessionIngestReportV1,
};
use mobile_packet_verifier::{
    backfill::{run_sessions_backfill, BackfillOptions},
    iceberg,
};
use sqlx::PgPool;

use crate::common;

/// Timeout for backfill operations in tests
const TEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

fn make_report(timestamp: DateTime<Utc>, event_id: &str) -> DataTransferSessionIngestReportV1 {
    let key = PublicKeyBinary::from(vec![1]);
    DataTransferSessionIngestReport {
        received_timestamp: timestamp,
        report: DataTransferSessionReq {
            rewardable_bytes: 1_000,
            pub_key: key.clone(),
            signature: vec![],
            carrier_id: CarrierIdV2::Carrier9,
            sampling: false,
            data_transfer_usage: DataTransferEvent {
                pub_key: key,
                upload_bytes: 500,
                download_bytes: 500,
                radio_access_technology: DataTransferRadioAccessTechnology::Wlan,
                event_id: event_id.to_string(),
                payer: vec![0].into(),
                timestamp,
                signature: vec![],
            },
        },
    }
    .into()
}

fn test_backfill_options(process_name: &str, start_after: DateTime<Utc>) -> BackfillOptions {
    BackfillOptions::new(process_name, start_after)
        .poll_duration(std::time::Duration::from_millis(100))
        .idle_timeout(std::time::Duration::from_millis(500))
}

#[sqlx::test]
async fn backfill_writes_sessions_to_iceberg(pool: PgPool) -> anyhow::Result<()> {
    let harness = common::setup_iceberg().await?;
    let writer = harness
        .get_table_writer(iceberg::session::TABLE_NAME)
        .await?;

    let awsl = AwsLocal::new().await;
    awsl.create_bucket().await?;

    // Create test files with distinct timestamps
    // Truncate to milliseconds to avoid sub-millisecond comparison issues
    let base_time = Utc::now() - Duration::hours(1);
    let start_time = base_time - Duration::minutes(1);
    let file1_time = base_time;
    let file2_time = base_time + Duration::minutes(5);

    awsl.put_protos_at_time(
        FileType::DataTransferSessionIngestReport.to_string(),
        vec![make_report(file1_time, "event-1")],
        file1_time,
    )
    .await?;

    awsl.put_protos_at_time(
        FileType::DataTransferSessionIngestReport.to_string(),
        vec![make_report(file1_time, "event-2")],
        file2_time,
    )
    .await?;

    // Run backfill - poller will exit via idle_timeout after processing all files
    let options = test_backfill_options("test-backfill", start_time);
    let run_future = run_sessions_backfill(pool.clone(), awsl.bucket_client(), writer, options);
    tokio::time::timeout(TEST_TIMEOUT, run_future)
        .await
        .map_err(|_| anyhow::anyhow!("backfill timed out after {:?}", TEST_TIMEOUT))??;

    // Verify data was written to Iceberg
    let rows = iceberg::session::get_all(harness.trino()).await?;
    assert_eq!(rows.len(), 2, "expected 2 sessions in iceberg");

    awsl.cleanup().await?;
    Ok(())
}

#[sqlx::test]
async fn backfill_stops_at_timestamp(pool: PgPool) -> anyhow::Result<()> {
    let harness = common::setup_iceberg().await?;
    let writer = harness
        .get_table_writer(iceberg::session::TABLE_NAME)
        .await?;

    let awsl = AwsLocal::new().await;
    awsl.create_bucket().await?;

    // Create 3 files with distinct timestamps
    // Truncate to milliseconds to avoid sub-millisecond comparison issues
    let base_time = Utc::now() - Duration::hours(2);
    let start_time = base_time - Duration::minutes(1);
    let file1_time = base_time;
    let file2_time = base_time + Duration::minutes(30);
    let stop_time = base_time + Duration::minutes(45);
    let file3_time = base_time + Duration::hours(1); // This one should be skipped

    awsl.put_protos_at_time(
        FileType::DataTransferSessionIngestReport.to_string(),
        vec![make_report(file1_time, "event-1")],
        file1_time,
    )
    .await?;

    awsl.put_protos_at_time(
        FileType::DataTransferSessionIngestReport.to_string(),
        vec![make_report(file2_time, "event-2")],
        file2_time,
    )
    .await?;

    awsl.put_protos_at_time(
        FileType::DataTransferSessionIngestReport.to_string(),
        vec![make_report(file3_time, "event-3")],
        file3_time,
    )
    .await?;

    // Run backfill with stop_after = file3_time (should process only first 2 files)
    // Poller exits via stop_after before reaching file3, then idle_timeout triggers exit
    let options = test_backfill_options("test-backfill-stop", start_time).stop_after(stop_time);
    let run_future = run_sessions_backfill(pool.clone(), awsl.bucket_client(), writer, options);
    tokio::time::timeout(TEST_TIMEOUT, run_future)
        .await
        .map_err(|_| anyhow::anyhow!("backfill timed out after {:?}", TEST_TIMEOUT))??;

    // Verify only 2 sessions were written (file3 was skipped)
    let rows = iceberg::session::get_all(harness.trino()).await?;
    assert_eq!(
        rows.len(),
        2,
        "expected 2 sessions (3rd file should be skipped due to stop_after)"
    );

    awsl.cleanup().await?;
    Ok(())
}

#[sqlx::test]
async fn backfill_resumes_after_interruption(pool: PgPool) -> anyhow::Result<()> {
    let harness = common::setup_iceberg().await?;
    let writer = harness
        .get_table_writer(iceberg::session::TABLE_NAME)
        .await?;

    let awsl = AwsLocal::new().await;
    awsl.create_bucket().await?;

    // Create 3 files
    // Truncate to milliseconds to avoid sub-millisecond comparison issues
    let base_time = Utc::now() - Duration::hours(2);
    let start_time = base_time - Duration::minutes(1);
    let file1_time = base_time;
    let file2_time = base_time + Duration::minutes(30);
    let stop_time = base_time + Duration::minutes(45);
    let file3_time = base_time + Duration::hours(1);

    awsl.put_protos_at_time(
        FileType::DataTransferSessionIngestReport.to_string(),
        vec![make_report(file1_time, "event-1")],
        file1_time,
    )
    .await?;

    awsl.put_protos_at_time(
        FileType::DataTransferSessionIngestReport.to_string(),
        vec![make_report(file1_time, "event-2")],
        file2_time,
    )
    .await?;

    awsl.put_protos_at_time(
        FileType::DataTransferSessionIngestReport.to_string(),
        vec![make_report(file1_time, "event-3")],
        file3_time,
    )
    .await?;

    let process_name = "test-backfill-resume";

    // First run: process only first 2 files (stop before file3_time)
    let options = test_backfill_options(process_name, start_time).stop_after(stop_time);
    tokio::time::timeout(
        TEST_TIMEOUT,
        run_sessions_backfill(pool.clone(), awsl.bucket_client(), writer.clone(), options),
    )
    .await
    .map_err(|_| anyhow::anyhow!("backfill timed out after {:?}", TEST_TIMEOUT))??;

    let rows_after_first = iceberg::session::get_all(harness.trino()).await?;
    assert_eq!(
        rows_after_first.len(),
        2,
        "first run should process 2 files"
    );

    // Second run: resume and process remaining files (same process_name)
    // The FileInfoPoller should skip already-processed files
    // Poller exits via idle_timeout after processing file3
    let options = test_backfill_options(process_name, start_time);
    tokio::time::timeout(
        TEST_TIMEOUT,
        run_sessions_backfill(pool.clone(), awsl.bucket_client(), writer, options),
    )
    .await
    .map_err(|_| anyhow::anyhow!("backfill timed out after {:?}", TEST_TIMEOUT))??;

    // Verify all 3 sessions are now in iceberg
    let rows_after_resume = iceberg::session::get_all(harness.trino()).await?;
    assert_eq!(
        rows_after_resume.len(),
        3,
        "after resume, should have all 3 sessions"
    );

    awsl.cleanup().await?;
    Ok(())
}
