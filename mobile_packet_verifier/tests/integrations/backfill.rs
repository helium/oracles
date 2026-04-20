use anyhow::Context;
use chrono::{DateTime, Duration, Utc};
use file_store::aws_local::AwsLocal;
use file_store_oracles::{
    mobile_session::{DataTransferEvent, DataTransferSessionIngestReport, DataTransferSessionReq},
    mobile_transfer::ValidDataTransferSession,
    FileType,
};
use helium_crypto::PublicKeyBinary;
use helium_proto::services::{
    packet_verifier as proto,
    poc_mobile::{
        verified_data_transfer_ingest_report_v1::ReportStatus, CarrierIdV2,
        DataTransferRadioAccessTechnology, DataTransferSessionIngestReportV1,
        VerifiedDataTransferIngestReportV1,
    },
};
use mobile_packet_verifier::{
    backfill::{BurnedSessionsBackfiller, DataSessionsBackfiller},
    iceberg,
};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use trino_rust_client::Trino;

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

fn make_verified_report(
    timestamp: DateTime<Utc>,
    event_id: &str,
    status: ReportStatus,
) -> VerifiedDataTransferIngestReportV1 {
    VerifiedDataTransferIngestReportV1 {
        report: Some(make_report(timestamp, event_id)),
        status: status as i32,
        timestamp: timestamp.timestamp_millis() as u64,
    }
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
    let base_time = Utc::now() - Duration::hours(1);
    let start_time = base_time - Duration::minutes(1); // start polling for files
    let file1_time = base_time;
    let file2_time = base_time + Duration::minutes(5);
    let end_time = base_time + Duration::days(42); // backfill requires stop time

    awsl.put_protos_at_time(
        FileType::VerifiedDataTransferSession.to_string(),
        vec![make_verified_report(
            file1_time,
            "event-1",
            ReportStatus::Valid,
        )],
        file1_time,
    )
    .await?;

    awsl.put_protos_at_time(
        FileType::VerifiedDataTransferSession.to_string(),
        vec![make_verified_report(
            file1_time,
            "event-2",
            ReportStatus::Valid,
        )],
        file2_time,
    )
    .await?;

    // Run backfill - poller will exit via idle_timeout after processing all files
    let options = common::test_backfill_options("test-backfill", start_time, end_time);

    tokio::time::timeout(
        TEST_TIMEOUT,
        DataSessionsBackfiller::create_managed_task(
            pool.clone(),
            awsl.bucket_client(),
            Some(writer),
            options,
        )
        .await?
        .start(),
    )
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
    let start_time = base_time - Duration::minutes(1); // start polling for files
    let file1_time = base_time;
    let file2_time = base_time + Duration::minutes(30);
    let stop_time = base_time + Duration::minutes(45); // stop processing files
    let file3_time = base_time + Duration::hours(1); // This one should be skipped

    awsl.put_protos_at_time(
        FileType::VerifiedDataTransferSession.to_string(),
        vec![make_verified_report(
            file1_time,
            "event-1",
            ReportStatus::Valid,
        )],
        file1_time,
    )
    .await?;

    awsl.put_protos_at_time(
        FileType::VerifiedDataTransferSession.to_string(),
        vec![make_verified_report(
            file2_time,
            "event-2",
            ReportStatus::Valid,
        )],
        file2_time,
    )
    .await?;

    awsl.put_protos_at_time(
        FileType::VerifiedDataTransferSession.to_string(),
        vec![make_verified_report(
            file3_time,
            "event-3",
            ReportStatus::Valid,
        )],
        file3_time,
    )
    .await?;

    // Run backfill with stop_after = file3_time (should process only first 2 files)
    // Poller exits via stop_after before reaching file3, then idle_timeout triggers exit
    let options = common::test_backfill_options("test-backfill-stop", start_time, stop_time);
    tokio::time::timeout(
        TEST_TIMEOUT,
        DataSessionsBackfiller::create_managed_task(
            pool.clone(),
            awsl.bucket_client(),
            Some(writer),
            options,
        )
        .await?
        .start(),
    )
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
    let start_time = base_time - Duration::minutes(1); // start polling for files
    let file1_time = base_time;
    let file2_time = base_time + Duration::minutes(30);
    let stop_time = base_time + Duration::minutes(45); // interruption
    let file3_time = base_time + Duration::hours(1);
    let end_time = base_time + Duration::days(42); // continue to end of files

    awsl.put_protos_at_time(
        FileType::VerifiedDataTransferSession.to_string(),
        vec![make_verified_report(
            file1_time,
            "event-1",
            ReportStatus::Valid,
        )],
        file1_time,
    )
    .await?;

    awsl.put_protos_at_time(
        FileType::VerifiedDataTransferSession.to_string(),
        vec![make_verified_report(
            file1_time,
            "event-2",
            ReportStatus::Valid,
        )],
        file2_time,
    )
    .await?;

    awsl.put_protos_at_time(
        FileType::VerifiedDataTransferSession.to_string(),
        vec![make_verified_report(
            file1_time,
            "event-3",
            ReportStatus::Valid,
        )],
        file3_time,
    )
    .await?;

    let process_name = "test-backfill-resume";

    // First run: process only first 2 files (stop before file3_time)
    let options = common::test_backfill_options(process_name, start_time, stop_time);
    tokio::time::timeout(
        TEST_TIMEOUT,
        DataSessionsBackfiller::create_managed_task(
            pool.clone(),
            awsl.bucket_client(),
            Some(writer.clone()),
            options,
        )
        .await?
        .start(),
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
    let options = common::test_backfill_options(process_name, start_time, end_time);
    tokio::time::timeout(
        TEST_TIMEOUT,
        DataSessionsBackfiller::create_managed_task(
            pool.clone(),
            awsl.bucket_client(),
            Some(writer),
            options,
        )
        .await?
        .start(),
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

#[sqlx::test]
async fn backfill_filters_invalid_sessions(pool: PgPool) -> anyhow::Result<()> {
    let harness = common::setup_iceberg().await?;
    let writer = harness
        .get_table_writer(iceberg::session::TABLE_NAME)
        .await?;

    let awsl = AwsLocal::new().await;
    awsl.create_bucket().await?;

    let base_time = Utc::now() - Duration::hours(1);
    let start_time = base_time - Duration::minutes(1);
    let file_time = base_time;
    let end_time = base_time + Duration::days(42);

    // Write a file with mixed statuses - only Valid ones should be written to Iceberg
    awsl.put_protos_at_time(
        FileType::VerifiedDataTransferSession.to_string(),
        vec![
            make_verified_report(file_time, "valid-1", ReportStatus::Valid),
            make_verified_report(
                file_time,
                "invalid-gateway",
                ReportStatus::InvalidGatewayKey,
            ),
            make_verified_report(file_time, "valid-2", ReportStatus::Valid),
            make_verified_report(file_time, "duplicate", ReportStatus::Duplicate),
            make_verified_report(file_time, "banned", ReportStatus::Banned),
        ],
        file_time,
    )
    .await?;

    let options = common::test_backfill_options("test-backfill-filter", start_time, end_time);

    tokio::time::timeout(
        TEST_TIMEOUT,
        DataSessionsBackfiller::create_managed_task(
            pool.clone(),
            awsl.bucket_client(),
            Some(writer),
            options,
        )
        .await?
        .start(),
    )
    .await
    .map_err(|_| anyhow::anyhow!("backfill timed out after {:?}", TEST_TIMEOUT))??;

    // Verify only the 2 Valid sessions were written to Iceberg
    let rows = iceberg::session::get_all(harness.trino()).await?;
    assert_eq!(rows.len(), 2, "expected only 2 valid sessions in iceberg");

    awsl.cleanup().await?;
    Ok(())
}

fn make_valid_data_transfer_session(
    first_timestamp: DateTime<Utc>,
    last_timestamp: DateTime<Utc>,
    burn_timestamp: DateTime<Utc>,
) -> proto::ValidDataTransferSession {
    ValidDataTransferSession {
        pub_key: PublicKeyBinary::from(vec![1]),
        payer: PublicKeyBinary::from(vec![2]),
        upload_bytes: 100,
        download_bytes: 200,
        rewardable_bytes: 300,
        num_dcs: 10,
        first_timestamp,
        last_timestamp,
        burn_timestamp,
    }
    .into()
}

#[sqlx::test]
async fn burned_backfill_writes_sessions_to_iceberg(pool: PgPool) -> anyhow::Result<()> {
    let harness = common::setup_iceberg().await?;
    let writer = harness
        .get_table_writer(iceberg::burned_session::TABLE_NAME)
        .await?;

    let awsl = AwsLocal::new().await;
    awsl.create_bucket().await?;

    let base_time = Utc::now() - Duration::hours(1);
    let start_time = base_time - Duration::minutes(1);
    let file_time = base_time;
    let end_time = base_time + Duration::days(42);

    let burn_time = base_time - Duration::minutes(5);

    awsl.put_protos_at_time(
        FileType::ValidDataTransferSession.to_string(),
        vec![make_valid_data_transfer_session(
            base_time - Duration::hours(1),
            base_time - Duration::minutes(30),
            burn_time,
        )],
        file_time,
    )
    .await?;

    let options = common::test_backfill_options("test-burned-backfill", start_time, end_time);

    tokio::time::timeout(
        TEST_TIMEOUT,
        BurnedSessionsBackfiller::create_managed_task(
            pool.clone(),
            awsl.bucket_client(),
            Some(writer),
            options,
        )
        .await?
        .start(),
    )
    .await
    .map_err(|_| anyhow::anyhow!("backfill timed out after {:?}", TEST_TIMEOUT))??;

    let rows = iceberg::burned_session::get_all(harness.trino()).await?;
    assert_eq!(rows.len(), 1, "expected 1 burned session in iceberg");

    awsl.cleanup().await?;
    Ok(())
}

#[sqlx::test]
async fn burned_backfill_uses_file_timestamp_when_burn_timestamp_is_epoch(
    pool: PgPool,
) -> anyhow::Result<()> {
    let harness = common::setup_iceberg().await?;
    let writer = harness
        .get_table_writer(iceberg::burned_session::TABLE_NAME)
        .await?;

    let awsl = AwsLocal::new().await;
    awsl.create_bucket().await?;

    let base_time = Utc::now() - Duration::hours(1);
    let start_time = base_time - Duration::minutes(1);
    let file_time = base_time;
    let end_time = base_time + Duration::days(42);

    // Use epoch (0) for burn_timestamp to simulate old data before the field was added
    let epoch = DateTime::UNIX_EPOCH;

    awsl.put_protos_at_time(
        FileType::ValidDataTransferSession.to_string(),
        vec![make_valid_data_transfer_session(
            base_time - Duration::hours(1),
            base_time - Duration::minutes(30),
            epoch,
        )],
        file_time,
    )
    .await?;

    let options = common::test_backfill_options("test-burned-backfill-epoch", start_time, end_time);

    tokio::time::timeout(
        TEST_TIMEOUT,
        BurnedSessionsBackfiller::create_managed_task(
            pool.clone(),
            awsl.bucket_client(),
            Some(writer),
            options,
        )
        .await
        .context("burned backfiller")?
        .start(),
    )
    .await
    .map_err(|_| anyhow::anyhow!("backfill timed out after {:?}", TEST_TIMEOUT))??;

    let rows = iceberg::burned_session::get_all(harness.trino())
        .await
        .context("attempting to get all")?;
    assert_eq!(rows.len(), 1, "expected 1 burned session in iceberg");

    // Query the files metadata to verify burn_timestamp was set to file_time (not epoch).
    // The partition field is a ROW type with a burn_timestamp_day field.
    let namespace = iceberg::burned_session::NAMESPACE;
    let table_name = iceberg::burned_session::TABLE_NAME;
    let query = format!(
        r#"SELECT partition.burn_timestamp_day as burn_timestamp_day FROM {namespace}."{table_name}$files""#
    );

    #[derive(Debug, Trino, Serialize, Deserialize)]
    struct FileRow {
        burn_timestamp_day: chrono::NaiveDate,
    }

    let files = harness
        .trino()
        .get_all::<FileRow>(query)
        .await
        .context("get all")?
        .into_vec();

    assert_eq!(files.len(), 1, "expected 1 file");

    let actual_day = files[0].burn_timestamp_day;
    let expected_day = file_time.date_naive();
    let epoch_day = epoch.date_naive();

    assert_ne!(
        actual_day, epoch_day,
        "partition should not be epoch date (1970-01-01)"
    );
    assert_eq!(
        actual_day, expected_day,
        "partition should be file_time date"
    );

    awsl.cleanup().await?;
    Ok(())
}
