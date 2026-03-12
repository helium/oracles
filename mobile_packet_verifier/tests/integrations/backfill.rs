use chrono::{DateTime, Duration, Utc};
use file_store::aws_local::AwsLocal;
use file_store::file_source;
use file_store_oracles::{
    mobile_session::{DataTransferEvent, DataTransferSessionIngestReport, DataTransferSessionReq},
    FileType,
};
use helium_crypto::PublicKeyBinary;
use helium_proto::services::poc_mobile::{
    CarrierIdV2, DataTransferRadioAccessTechnology, DataTransferSessionIngestReportV1,
};
use mobile_packet_verifier::{
    backfill::SessionsBackfiller,
    iceberg::{self, session::IcebergDataTransferSession},
};
use sqlx::PgPool;
use task_manager::TaskManager;

use crate::common;

/// Timeout for backfill operations in tests
const TEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

fn make_report(timestamp: DateTime<Utc>, event_id: &str) -> DataTransferSessionIngestReport {
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
}

/// Helper to run backfill for sessions table using the actual SessionsBackfiller
async fn run_sessions_backfill(
    pool: &PgPool,
    awsl: &AwsLocal,
    writer: iceberg::DataTransferWriter,
    process_name: &str,
    start_after: DateTime<Utc>,
    stop_after: DateTime<Utc>,
) -> anyhow::Result<()> {
    let (reports, reports_server) =
        file_source::continuous_source::<DataTransferSessionIngestReport, _, _>()
            .state(pool.clone())
            .bucket_client(awsl.bucket_client())
            .prefix(FileType::DataTransferSessionIngestReport.to_string())
            .lookback_start_after(start_after)
            .stop_after(stop_after)
            .process_name(process_name.to_string())
            .poll_duration(std::time::Duration::from_secs(1))
            .create()
            .await?;

    let backfiller = SessionsBackfiller::new(pool.clone(), reports, writer);

    let run_future = async {
        TaskManager::builder()
            .add_task(reports_server)
            .add_task(backfiller)
            .build()
            .start()
            .await?;
        Ok::<_, anyhow::Error>(())
    };

    tokio::time::timeout(TEST_TIMEOUT, run_future)
        .await
        .map_err(|_| anyhow::anyhow!("backfill timed out after {:?}", TEST_TIMEOUT))??;

    Ok(())
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
    let base_time_ms = DateTime::from_timestamp_millis(base_time.timestamp_millis()).unwrap();
    let file1_time = base_time_ms;
    let file2_time = base_time_ms + Duration::minutes(5);
    // stop_after is after all files so backfiller will process everything then exit
    let stop_after = file2_time + Duration::minutes(1);

    let report1 = make_report(file1_time, "event-1");
    let report2 = make_report(file2_time, "event-2");

    let proto1: DataTransferSessionIngestReportV1 = report1.into();
    let proto2: DataTransferSessionIngestReportV1 = report2.into();

    awsl.put_protos_at_time(
        FileType::DataTransferSessionIngestReport.to_string(),
        vec![proto1],
        file1_time,
    )
    .await?;

    awsl.put_protos_at_time(
        FileType::DataTransferSessionIngestReport.to_string(),
        vec![proto2],
        file2_time,
    )
    .await?;

    // Create an empty file at stop_after time so the poller has something to trigger exit
    awsl.put_protos_at_time(
        FileType::DataTransferSessionIngestReport.to_string(),
        Vec::<DataTransferSessionIngestReportV1>::new(),
        stop_after,
    )
    .await?;

    // Run backfill using the actual SessionsBackfiller
    run_sessions_backfill(
        &pool,
        &awsl,
        writer,
        "test-backfill",
        base_time - Duration::minutes(1),
        stop_after,
    )
    .await?;

    // Verify data was written to Iceberg
    let rows: Vec<IcebergDataTransferSession> = iceberg::session::get_all(harness.trino()).await?;
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
    let base_time_ms = DateTime::from_timestamp_millis(base_time.timestamp_millis()).unwrap();
    let file1_time = base_time_ms;
    let file2_time = base_time_ms + Duration::minutes(30);
    let file3_time = base_time_ms + Duration::hours(1); // This one should be skipped

    let proto1: DataTransferSessionIngestReportV1 = make_report(file1_time, "event-1").into();
    let proto2: DataTransferSessionIngestReportV1 = make_report(file2_time, "event-2").into();
    let proto3: DataTransferSessionIngestReportV1 = make_report(file3_time, "event-3").into();

    awsl.put_protos_at_time(
        FileType::DataTransferSessionIngestReport.to_string(),
        vec![proto1],
        file1_time,
    )
    .await?;

    awsl.put_protos_at_time(
        FileType::DataTransferSessionIngestReport.to_string(),
        vec![proto2],
        file2_time,
    )
    .await?;

    awsl.put_protos_at_time(
        FileType::DataTransferSessionIngestReport.to_string(),
        vec![proto3],
        file3_time,
    )
    .await?;

    // Run backfill with stop_after = file3_time (should process only first 2 files)
    run_sessions_backfill(
        &pool,
        &awsl,
        writer,
        "test-backfill-stop",
        base_time - Duration::minutes(1),
        file3_time,
    )
    .await?;

    // Verify only 2 sessions were written (file3 was skipped)
    let rows: Vec<IcebergDataTransferSession> = iceberg::session::get_all(harness.trino()).await?;
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
    let base_time_ms = DateTime::from_timestamp_millis(base_time.timestamp_millis()).unwrap();
    let file1_time = base_time_ms;
    let file2_time = base_time_ms + Duration::minutes(30);
    let file3_time = base_time_ms + Duration::hours(1);
    // Sentinel file to trigger poller exit after processing all real files
    let sentinel_time = file3_time + Duration::minutes(1);

    let proto1: DataTransferSessionIngestReportV1 = make_report(file1_time, "event-1").into();
    let proto2: DataTransferSessionIngestReportV1 = make_report(file2_time, "event-2").into();
    let proto3: DataTransferSessionIngestReportV1 = make_report(file3_time, "event-3").into();

    awsl.put_protos_at_time(
        FileType::DataTransferSessionIngestReport.to_string(),
        vec![proto1],
        file1_time,
    )
    .await?;

    awsl.put_protos_at_time(
        FileType::DataTransferSessionIngestReport.to_string(),
        vec![proto2],
        file2_time,
    )
    .await?;

    awsl.put_protos_at_time(
        FileType::DataTransferSessionIngestReport.to_string(),
        vec![proto3],
        file3_time,
    )
    .await?;

    // Create an empty sentinel file to trigger poller exit
    awsl.put_protos_at_time(
        FileType::DataTransferSessionIngestReport.to_string(),
        Vec::<DataTransferSessionIngestReportV1>::new(),
        sentinel_time,
    )
    .await?;

    let process_name = "test-backfill-resume";

    // First run: process only first 2 files (stop at file3_time)
    run_sessions_backfill(
        &pool,
        &awsl,
        writer.clone(),
        process_name,
        base_time - Duration::minutes(1),
        file3_time,
    )
    .await?;

    let rows_after_first: Vec<IcebergDataTransferSession> =
        iceberg::session::get_all(harness.trino()).await?;
    assert_eq!(
        rows_after_first.len(),
        2,
        "first run should process 2 files"
    );

    // Second run: resume and process remaining files (same process_name)
    // The FileInfoPoller should skip already-processed files
    // Use sentinel_time as stop_after so poller exits after processing file3
    run_sessions_backfill(
        &pool,
        &awsl,
        writer,
        process_name,
        base_time - Duration::minutes(1),
        sentinel_time,
    )
    .await?;

    // Verify all 3 sessions are now in iceberg
    let rows_after_resume: Vec<IcebergDataTransferSession> =
        iceberg::session::get_all(harness.trino()).await?;
    assert_eq!(
        rows_after_resume.len(),
        3,
        "after resume, should have all 3 sessions"
    );

    awsl.cleanup().await?;
    Ok(())
}
