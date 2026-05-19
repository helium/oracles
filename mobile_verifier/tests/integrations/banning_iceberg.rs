use chrono::{DateTime, Duration, SubsecRound, Utc};
use file_store::aws_local::AwsLocal;
use file_store_oracles::{
    mobile_ban::{
        proto::{
            BanAction as ProtoBanAction, BanDetailsV1, BanIngestReportV1, BanReason, BanReqV1,
            BanType as ProtoBanType, VerifiedBanIngestReportStatus, VerifiedBanIngestReportV1,
        },
        BanAction, BanDetails, BanReport, BanRequest, BanType, VerifiedBanReport,
    },
    FileType,
};
use helium_crypto::PublicKeyBinary;
use mobile_verifier::{
    backfill::BackfillOptions,
    banning::BanBackfiller,
    iceberg::{self, ban::IcebergBan},
};
use sqlx::PgPool;

const TEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

fn truncated_now() -> DateTime<Utc> {
    Utc::now().trunc_subsecs(3)
}

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

fn make_iceberg_ban(seed: u8) -> IcebergBan {
    let hotspot_pubkey = PublicKeyBinary::from(vec![seed, seed, seed]);
    let ban_pubkey = PublicKeyBinary::from(vec![seed + 1, seed + 1]);
    let now = truncated_now();

    let report = VerifiedBanReport {
        verified_timestamp: now,
        report: BanReport {
            received_timestamp: now,
            report: BanRequest {
                hotspot_pubkey,
                timestamp: now - Duration::minutes(5),
                ban_pubkey,
                signature: vec![],
                ban_action: BanAction::Ban(BanDetails {
                    hotspot_serial: format!("SN-{seed}"),
                    message: format!("ban {seed}"),
                    reason: BanReason::LocationGaming,
                    ban_type: BanType::Poc,
                    expiration_timestamp: None,
                }),
            },
        },
        status: VerifiedBanIngestReportStatus::Valid,
    };
    IcebergBan::from(&report)
}

fn make_verified_ban_proto(
    timestamp: DateTime<Utc>,
    status: VerifiedBanIngestReportStatus,
) -> VerifiedBanIngestReportV1 {
    let hotspot_pubkey: PublicKeyBinary = "112E7TxoNHV46M6tiPA8N1MkeMeQxc9ztb4JQLXBVAAUfq1kJLoF"
        .parse()
        .unwrap();
    let ban_pubkey: PublicKeyBinary = "112NqN2WWMwtK29PMzRby62fDydBJfsCLkCAf392stdok48ovNT6"
        .parse()
        .unwrap();
    let ts_ms = timestamp.timestamp_millis() as u64;

    VerifiedBanIngestReportV1 {
        verified_timestamp_ms: ts_ms,
        report: Some(BanIngestReportV1 {
            received_timestamp_ms: ts_ms,
            report: Some(BanReqV1 {
                hotspot_pubkey: hotspot_pubkey.as_ref().to_vec(),
                timestamp_ms: ts_ms,
                ban_pubkey: ban_pubkey.as_ref().to_vec(),
                signature: vec![],
                ban_action: Some(ProtoBanAction::Ban(BanDetailsV1 {
                    hotspot_serial: "SN-12345".to_string(),
                    message: "test ban".to_string(),
                    reason: BanReason::LocationGaming as i32,
                    ban_type: ProtoBanType::Poc as i32,
                    expiration_timestamp_ms: 0,
                })),
            }),
        }),
        status: status as i32,
    }
}

// ── Pure write tests (no DB needed) ──────────────────────────────────────────

#[tokio::test]
async fn write_single_ban() -> anyhow::Result<()> {
    let harness = crate::common::setup_iceberg().await?;
    let writer = harness
        .get_table_writer::<IcebergBan>(iceberg::ban::TABLE_NAME)
        .await?;

    let record = make_iceberg_ban(1);

    writer
        .write_idempotent("test_single", vec![record.clone()])
        .await?;

    let all = iceberg::ban::get_all(harness.trino()).await?;
    assert_eq!(all, vec![record]);

    Ok(())
}

#[tokio::test]
async fn write_multiple_bans() -> anyhow::Result<()> {
    let harness = crate::common::setup_iceberg().await?;
    let writer = harness
        .get_table_writer::<IcebergBan>(iceberg::ban::TABLE_NAME)
        .await?;

    let records: Vec<IcebergBan> = (1u8..=5).map(make_iceberg_ban).collect();

    writer.write_idempotent("test_multiple", records).await?;

    let all = iceberg::ban::get_all(harness.trino()).await?;
    assert_eq!(all.len(), 5);

    Ok(())
}

#[tokio::test]
async fn idempotent_write_deduplicates() -> anyhow::Result<()> {
    let harness = crate::common::setup_iceberg().await?;
    let writer = harness
        .get_table_writer::<IcebergBan>(iceberg::ban::TABLE_NAME)
        .await?;

    let record = make_iceberg_ban(1);

    writer
        .write_idempotent("same_id", vec![record.clone()])
        .await?;
    writer
        .write_idempotent("same_id", vec![record.clone()])
        .await?;

    let all = iceberg::ban::get_all(harness.trino()).await?;
    assert_eq!(all.len(), 1, "second write with same id should be a no-op");

    Ok(())
}

// ── Backfill tests (DB needed for file-tracking state) ────────────────────────

#[sqlx::test]
async fn backfill_writes_bans_to_iceberg(pool: PgPool) -> anyhow::Result<()> {
    let harness = crate::common::setup_iceberg().await?;
    let (writer, writer_task, _spool_dir) = crate::common::make_batched_writer::<IcebergBan>(
        &harness,
        iceberg::ban::table_definition()?,
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
        FileType::VerifiedMobileBanReport.to_string(),
        vec![make_verified_ban_proto(
            file1_time,
            VerifiedBanIngestReportStatus::Valid,
        )],
        file1_time,
    )
    .await?;

    awsl.put_protos_at_time(
        FileType::VerifiedMobileBanReport.to_string(),
        vec![make_verified_ban_proto(
            file2_time,
            VerifiedBanIngestReportStatus::Valid,
        )],
        file2_time,
    )
    .await?;

    let opts = test_backfill_options("test-backfill", start_time, end_time);
    let (backfiller, server) =
        BanBackfiller::create_batched(pool, awsl.bucket_client(), writer, opts).await?;

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

    let rows = iceberg::ban::get_all(harness.trino()).await?;
    assert_eq!(rows.len(), 2, "expected 2 bans in iceberg");

    awsl.cleanup().await?;
    Ok(())
}

#[sqlx::test]
async fn backfill_filters_invalid_bans(pool: PgPool) -> anyhow::Result<()> {
    let harness = crate::common::setup_iceberg().await?;
    let (writer, writer_task, _spool_dir) = crate::common::make_batched_writer::<IcebergBan>(
        &harness,
        iceberg::ban::table_definition()?,
    )
    .await?;

    let awsl = AwsLocal::new().await;
    awsl.create_bucket().await?;

    let base_time = Utc::now() - Duration::hours(1);
    let start_time = base_time - Duration::minutes(1);
    let file_time = base_time;
    let end_time = base_time + Duration::days(42);

    awsl.put_protos_at_time(
        FileType::VerifiedMobileBanReport.to_string(),
        vec![
            make_verified_ban_proto(file_time, VerifiedBanIngestReportStatus::Valid),
            make_verified_ban_proto(file_time, VerifiedBanIngestReportStatus::InvalidBanKey),
        ],
        file_time,
    )
    .await?;

    let opts = test_backfill_options("test-backfill-filter", start_time, end_time);
    let (backfiller, server) =
        BanBackfiller::create_batched(pool, awsl.bucket_client(), writer, opts).await?;

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

    let rows = iceberg::ban::get_all(harness.trino()).await?;
    assert_eq!(
        rows.len(),
        1,
        "only valid bans should be written to iceberg"
    );

    awsl.cleanup().await?;
    Ok(())
}

#[sqlx::test]
async fn backfill_stops_at_timestamp(pool: PgPool) -> anyhow::Result<()> {
    let harness = crate::common::setup_iceberg().await?;
    let (writer, writer_task, _spool_dir) = crate::common::make_batched_writer::<IcebergBan>(
        &harness,
        iceberg::ban::table_definition()?,
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
            FileType::VerifiedMobileBanReport.to_string(),
            vec![make_verified_ban_proto(
                time,
                VerifiedBanIngestReportStatus::Valid,
            )],
            time,
        )
        .await
        .unwrap_or_else(|e| panic!("failed to put {label}: {e}"));
    }

    let opts = test_backfill_options("test-backfill-stop", start_time, stop_time);
    let (backfiller, server) =
        BanBackfiller::create_batched(pool, awsl.bucket_client(), writer, opts).await?;

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

    let rows = iceberg::ban::get_all(harness.trino()).await?;
    assert_eq!(
        rows.len(),
        2,
        "expected 2 bans (file3 after stop_after should be skipped)"
    );

    awsl.cleanup().await?;
    Ok(())
}
