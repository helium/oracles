use chrono::{Duration, Utc};
use file_store::{
    aws_local::AwsLocal, file_sink::FileSinkClient, file_source, file_upload, BucketClient,
    FileInfo,
};
use file_store_oracles::{
    mobile_session::{DataTransferEvent, DataTransferSessionIngestReport, DataTransferSessionReq},
    traits::{FileSinkCommitStrategy, FileSinkRollTime, FileSinkWriteExt},
    FileType,
};
use helium_crypto::PublicKeyBinary;
use helium_proto::services::{
    packet_verifier::ValidDataTransferSession,
    poc_mobile::{
        verified_data_transfer_ingest_report_v1::ReportStatus, CarrierIdV2,
        DataTransferRadioAccessTechnology, DataTransferSessionIngestReportV1,
        VerifiedDataTransferIngestReportV1,
    },
};
use mobile_packet_verifier::{
    backfill::{BurnedSessionsBackfiller, DataSessionsBackfiller},
    burner::Burner,
    daemon::{Daemon, IngestReports},
    iceberg,
    pending_burns::{self, DataTransferSession},
};
use solana::{self, burn::TestSolanaClientMap};
use sqlx::PgPool;
use task_manager::{ManagedTask, TaskManager};

use crate::{common, daemon::trigger::TriggerExt};

const TEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
/// Burn period much longer than the test timeout — ensures burn never fires during tests.
const NO_BURN: std::time::Duration = std::time::Duration::from_secs(3600);

fn make_ingest_report(
    timestamp: chrono::DateTime<Utc>,
    event_id: &str,
) -> DataTransferSessionIngestReportV1 {
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
    timestamp: chrono::DateTime<Utc>,
    event_id: &str,
) -> VerifiedDataTransferIngestReportV1 {
    VerifiedDataTransferIngestReportV1 {
        report: Some(make_ingest_report(timestamp, event_id)),
        status: ReportStatus::Valid as i32,
        timestamp: timestamp.timestamp_millis() as u64,
    }
}

fn mk_data_transfer_session(
    payer_key: &PublicKeyBinary,
    pubkey: &PublicKeyBinary,
    rewardable_bytes: u64,
) -> DataTransferSession {
    use helium_proto::services::poc_mobile::DataTransferRadioAccessTechnology;
    let req = DataTransferSessionReq {
        data_transfer_usage: DataTransferEvent {
            pub_key: pubkey.clone(),
            upload_bytes: rewardable_bytes / 2,
            download_bytes: rewardable_bytes / 2,
            radio_access_technology: DataTransferRadioAccessTechnology::Wlan,
            event_id: "event-id".to_string(),
            payer: payer_key.clone(),
            timestamp: Utc::now(),
            signature: vec![],
        },
        rewardable_bytes,
        pub_key: pubkey.clone(),
        signature: vec![],
        carrier_id: CarrierIdV2::Carrier9,
        sampling: false,
    };
    DataTransferSession::from_req(&req, Utc::now())
}

async fn save_data_transfer_sessions(
    pool: &PgPool,
    sessions: &[(&PublicKeyBinary, &PublicKeyBinary, u64)],
) -> anyhow::Result<()> {
    let mut txn = pool.begin().await?;
    let sessions = sessions
        .iter()
        .map(|(payer, pubkey, amount)| mk_data_transfer_session(payer, pubkey, *amount))
        .collect::<Vec<_>>();
    pending_burns::save_data_transfer_sessions(&mut txn, &sessions).await?;
    txn.commit().await?;
    Ok(())
}

/// Build a no-op `Burner` that discards all output (for tests that don't exercise burn).
fn noop_burner() -> Burner<TestSolanaClientMap> {
    let (valid_tx, _valid_rx) = tokio::sync::mpsc::channel(100);
    Burner::new(
        FileSinkClient::new(valid_tx, "test"),
        TestSolanaClientMap::default(),
        0,
        std::time::Duration::default(),
        None,
    )
}

/// Verify that the daemon correctly processes incoming data transfer ingest reports.
/// The primary ingest path is exercised end-to-end:
///   - one ingest report → pending burn in DB
///   - Iceberg session record
///   - VerifiedDataTransferSession file in S3 (via real FileSink)
///
/// Shutdown is driven by polling pending_burns rather than a fixed sleep or
/// idle_timeout.  idle_timeout on the reports poller is intentionally omitted —
/// in production, idle_timeout is treated as a failure and should not be used as
/// a test exit mechanism.
#[sqlx::test]
async fn daemon_processes_ingest_reports(pool: PgPool) -> anyhow::Result<()> {
    pending_burns::initialize(&pool).await?;

    let harness = common::setup_iceberg().await?;
    let session_writer = harness
        .get_table_writer(iceberg::session::TABLE_NAME)
        .await?;

    let awsl = AwsLocal::new().await;
    awsl.create_bucket().await?;

    let base_time = Utc::now() - Duration::hours(1);
    let start_time = base_time - Duration::minutes(1);
    let file_time = base_time;

    awsl.put_protos_at_time(
        FileType::DataTransferSessionIngestReport.to_string(),
        vec![make_ingest_report(file_time, "event-1")],
        file_time,
    )
    .await?;

    // Real FileSink for VerifiedDataTransferIngestReportV1 (CommitStrategy::Manual,
    // matching production). A short roll_time ensures the file is committed promptly.
    let cache_dir = tempfile::tempdir()?;
    let (file_upload_client, file_upload_server) =
        file_upload::FileUpload::from_bucket_client(awsl.bucket_client()).await;
    let file_upload_watcher = file_upload_client.clone();
    let (verified_sessions_sink, verified_sessions_server) =
        VerifiedDataTransferIngestReportV1::file_sink(
            cache_dir.path(),
            file_upload_client,
            FileSinkCommitStrategy::Manual,
            FileSinkRollTime::Duration(std::time::Duration::from_millis(100)),
            "test",
        )
        .await?;

    // Reports poller: no idle_timeout — this is a continuous source that should
    // never exit on its own.
    let (reports, reports_server) = file_source::continuous_source()
        .state(pool.clone())
        .bucket_client(awsl.bucket_client())
        .prefix(FileType::DataTransferSessionIngestReport.to_string())
        .lookback_start_after(start_time)
        .poll_duration_opt(Some(std::time::Duration::from_millis(100)))
        .create()
        .await?;

    // Backfillers with None writer: done=true on construction, recv() returns
    // pending() immediately, so the select! arms never fire.
    let (_, session_rx) = tokio::sync::mpsc::channel(10);
    let (_, burned_rx) = tokio::sync::mpsc::channel(10);

    let ingest_reports = IngestReports::new(
        pool.clone(),
        reports,
        verified_sessions_sink,
        Some(session_writer),
    );

    let daemon = Daemon::new(
        pool.clone(),
        NO_BURN,
        NO_BURN,
        NO_BURN, // initial_burn_delay — burn never fires in this test
        ingest_reports,
        noop_burner(),
        common::TestMobileConfig::all_valid(),
        DataSessionsBackfiller::new(pool.clone(), session_rx, None),
        BurnedSessionsBackfiller::new(pool.clone(), burned_rx, None),
    );

    let (trigger, listener) = triggered::trigger();
    let events = daemon.event_rx();

    let job = TaskManager::builder()
        .add_task(file_upload_server)
        .add_task(verified_sessions_server)
        .add_task(reports_server)
        .add_task(daemon)
        .add_task(|_| async {
            // Shutdown on ReportHandle + upload completion: ReportHandle fires
            // after the FileSink commit is enqueued. We then wait for the
            // FileUploadServer to finish the upload (via the shared completion
            // counter on file_upload_watcher) before triggering shutdown,
            // ensuring the verified session file is in S3.
            trigger
                .when_uploads_completed_at_least(1, events, file_upload_watcher)
                .await
                .expect("uploads completed");
            Ok(())
        })
        .build();

    tokio::time::timeout(TEST_TIMEOUT, Box::new(job).start_task(listener))
        .await
        .map_err(|err| anyhow::anyhow!("daemon failed {err:?}"))??;

    let burns = pending_burns::get_all(&pool).await?;
    assert_eq!(burns.len(), 1, "expected 1 pending burn from ingest");

    let rows = iceberg::session::get_all(harness.trino()).await?;
    assert_eq!(rows.len(), 1, "expected 1 session in iceberg");

    let verified_files = verified_data_transfer_files(&awsl.bucket_client()).await?;
    assert!(
        !verified_files.is_empty(),
        "expected verified data transfer session file in S3"
    );

    awsl.cleanup().await?;
    Ok(())
}

/// Verify that when Iceberg is configured, the daemon's backfill arms process
/// verified session files and write them to Iceberg during idle time.
/// No ingest reports are present; the test exits when the Iceberg row appears.
#[sqlx::test]
async fn daemon_with_iceberg_processes_backfill(pool: PgPool) -> anyhow::Result<()> {
    let harness = common::setup_iceberg().await?;
    let session_writer = harness
        .get_table_writer(iceberg::session::TABLE_NAME)
        .await?;

    let awsl = AwsLocal::new().await;
    awsl.create_bucket().await?;

    let base_time = Utc::now() - Duration::hours(1);
    let start_time = base_time - Duration::minutes(1);
    let end_time = base_time + Duration::days(42);
    let file_time = base_time;

    // Put a verified session file that the session backfiller will pick up.
    awsl.put_protos_at_time(
        FileType::VerifiedDataTransferSession.to_string(),
        vec![make_verified_report(file_time, "backfill-event-1")],
        file_time,
    )
    .await?;

    // Session backfiller with a real writer.
    let backfill_opts = common::test_backfill_options("daemon-backfill", start_time, end_time);
    let (session_backfill, session_backfill_server) = DataSessionsBackfiller::create(
        pool.clone(),
        awsl.bucket_client(),
        Some(session_writer),
        Some(backfill_opts),
    )
    .await?;

    // Burned backfiller with no writer (no burned session files in this test).
    let (_, burned_rx) = tokio::sync::mpsc::channel(10);
    let burned_backfill = BurnedSessionsBackfiller::new(pool.clone(), burned_rx, None);

    // Reports channel kept open so the daemon's ingest arm just blocks.
    let (_reports_tx, reports_rx) = tokio::sync::mpsc::channel(10);

    let (verified_tx, _verified_rx) = tokio::sync::mpsc::channel(100);
    let ingest_reports = IngestReports::new(
        pool.clone(),
        reports_rx,
        FileSinkClient::new(verified_tx, "test"),
        None,
    );

    let daemon = Daemon::new(
        pool.clone(),
        NO_BURN,
        NO_BURN,
        NO_BURN,
        ingest_reports,
        noop_burner(),
        common::TestMobileConfig::all_valid(),
        session_backfill,
        burned_backfill,
    );

    // Shutdown: poll Iceberg until the backfilled row appears, then trigger.
    // When the Iceberg write is committed the backfiller has finished its work.
    let (trigger, listener) = triggered::trigger();

    let trino_client = harness.owned_trino().await?;
    let job = TaskManager::builder()
        .add_task(session_backfill_server)
        .add_task(daemon)
        .add_task(|_| async {
            trigger
                .when_iceberg_sessions_exist(trino_client)
                .await
                .expect("iceberg sessions exist");
            Ok(())
        })
        .build();

    // Spawn the TaskManager separately so we can borrow `harness` in this task.
    tokio::time::timeout(TEST_TIMEOUT, Box::new(job).start_task(listener))
        .await
        .map_err(|err| anyhow::anyhow!("deamon failed {:?}", err))??;

    let rows = iceberg::session::get_all(harness.trino()).await?;
    assert_eq!(rows.len(), 1, "expected 1 backfilled session in iceberg");

    awsl.cleanup().await?;
    Ok(())
}

/// Verify that when Iceberg is not configured, the daemon starts and runs without
/// panicking. Backfillers are disabled (done=true on construction). Shutdown is
/// driven by an explicit trigger after a brief delay — this test only checks that
/// the daemon wires up and starts cleanly, not that any processing occurs.
#[sqlx::test]
async fn daemon_without_iceberg_skips_backfill(pool: PgPool) -> anyhow::Result<()> {
    pending_burns::initialize(&pool).await?;

    // Manual reports channel — keep the sender alive so the daemon's ingest arm
    // just blocks rather than bailing with "sender dropped".
    // No S3 is needed since no files are processed in this test.
    let (_reports_tx, reports_rx) = tokio::sync::mpsc::channel(10);

    // No-op backfillers (no Iceberg writer → done=true, recv() returns pending())
    let (_, session_rx) = tokio::sync::mpsc::channel(10);
    let (_, burned_rx) = tokio::sync::mpsc::channel(10);

    let (verified_tx, _verified_rx) = tokio::sync::mpsc::channel(100);
    let ingest_reports = IngestReports::new(
        pool.clone(),
        reports_rx,
        FileSinkClient::new(verified_tx, "test"),
        None,
    );

    let daemon = Daemon::new(
        pool.clone(),
        NO_BURN,
        NO_BURN,
        NO_BURN,
        ingest_reports,
        noop_burner(),
        common::TestMobileConfig::all_valid(),
        DataSessionsBackfiller::new(pool.clone(), session_rx, None),
        BurnedSessionsBackfiller::new(pool.clone(), burned_rx, None),
    );

    let (trigger, listener) = triggered::trigger();
    let job = TaskManager::builder()
        .add_task(daemon)
        .add_task(|_| async {
            // Trigger shutdown after a short delay — enough to confirm the
            // daemon starts without panicking and idles cleanly.
            trigger.after_sleep(300).await;
            Ok(())
        })
        .build();

    tokio::time::timeout(TEST_TIMEOUT, Box::new(job).start_task(listener))
        .await
        .map_err(|err| anyhow::anyhow!("daemon failed {err:?}"))??;

    // No files were processed; pending burns table should be empty.
    let burns = pending_burns::get_all(&pool).await?;
    assert!(burns.is_empty(), "expected no pending burns");

    Ok(())
}

/// Verify that the daemon's burn arm fires, consumes pending burns, writes a
/// ValidDataTransferSession file to S3, and records a burned session in Iceberg.
///
/// This test exercises the full burn lifecycle inside the daemon's select! loop.
/// The ingest path is bypassed — pending burns are seeded directly in the DB.
/// A short `initial_burn_delay` (100 ms) ensures the burn fires quickly.
///
/// Shutdown is driven by polling for both the Iceberg burned_session row AND the
/// S3 ValidDataTransferSession file.  FileUploadServer exits immediately on
/// shutdown (does not drain its queue), so we must confirm the S3 file exists
/// *before* triggering shutdown.
#[sqlx::test]
async fn daemon_burns_sessions(pool: PgPool) -> anyhow::Result<()> {
    pending_burns::initialize(&pool).await?;

    let harness = common::setup_iceberg().await?;
    let burned_writer = harness
        .get_table_writer(iceberg::burned_session::TABLE_NAME)
        .await?;

    let awsl = AwsLocal::new().await;
    awsl.create_bucket().await?;

    // Seed a pending burn directly (bypass the ingest path).
    let payer = PublicKeyBinary::from(vec![1]);
    let pubkey = PublicKeyBinary::from(vec![2]);
    save_data_transfer_sessions(&pool, &[(&payer, &pubkey, 1_000)]).await?;

    let burns_before = pending_burns::get_all(&pool).await?;
    assert_eq!(
        burns_before.len(),
        1,
        "pre-condition: 1 pending burn seeded"
    );

    // Real FileSink for ValidDataTransferSession (Automatic commit — matches
    // production). Short roll_time so the file appears in S3 promptly.
    let cache_dir = tempfile::tempdir()?;
    let (file_upload_client, file_upload_server) =
        file_upload::FileUpload::from_bucket_client(awsl.bucket_client()).await;
    let (valid_sessions_sink, valid_sessions_server) = ValidDataTransferSession::file_sink(
        cache_dir.path(),
        file_upload_client,
        FileSinkCommitStrategy::Automatic,
        FileSinkRollTime::Duration(std::time::Duration::from_millis(100)),
        "test",
    )
    .await?;

    // Burner with TestSolanaClientMap configured with sufficient payer balance.
    // Clone the sink so we can manually commit it from the event handler below.
    let solana = TestSolanaClientMap::default();
    solana.insert(&payer, 1_000_000_000).await;
    let burner = Burner::new(
        valid_sessions_sink.clone(),
        solana,
        0,
        std::time::Duration::default(),
        Some(burned_writer),
    );

    // No-op backfillers
    let (_, session_rx) = tokio::sync::mpsc::channel(10);
    let (_, burned_rx) = tokio::sync::mpsc::channel(10);

    // Keep reports channel sender alive so the daemon's ingest arm just blocks.
    let (_reports_tx, reports_rx) = tokio::sync::mpsc::channel(1);

    let (verified_tx, _verified_rx) = tokio::sync::mpsc::channel(100);
    // No real-time Iceberg writer — this test only exercises the burn path.
    let ingest_reports = IngestReports::new(
        pool.clone(),
        reports_rx,
        FileSinkClient::new(verified_tx, "test"),
        None,
    );

    let daemon = Daemon::new(
        pool.clone(),
        std::time::Duration::from_millis(100), // burn_period (short)
        std::time::Duration::from_millis(100), // min_burn_period
        std::time::Duration::from_millis(100), // initial_burn_delay — fires in ~100ms
        ingest_reports,
        burner,
        common::TestMobileConfig::all_valid(),
        DataSessionsBackfiller::new(pool.clone(), session_rx, None),
        BurnedSessionsBackfiller::new(pool.clone(), burned_rx, None),
    );

    // On BurnSuccess, manually commit the Automatic FileSink so the rolled file
    // is sent to FileUploadServer immediately rather than waiting for the roll timer.
    tokio::spawn(commit_valid_session_sink_on_burn(
        daemon.event_rx(),
        valid_sessions_sink,
    ));

    // Shutdown: poll for both the Iceberg burned_session row AND the S3 valid file.
    // FileUploadServer exits without draining on shutdown, so both must be present
    // before we trigger. The Automatic FileSink only uploads on its roll timer;
    // we force an immediate commit on BurnSuccess so the file lands in S3 promptly.
    let (trigger, listener) = triggered::trigger();

    let job = TaskManager::builder()
        .add_task(file_upload_server)
        .add_task(valid_sessions_server)
        .add_task(daemon)
        .build();

    let job_handle = tokio::spawn(Box::new(job).start_task(listener));

    let bucket_client = awsl.bucket_client();
    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            let rows = iceberg::burned_session::get_all(harness.trino()).await?;
            let valid_files = valid_data_transfer_files(&bucket_client).await?;

            if !rows.is_empty() && !valid_files.is_empty() {
                trigger.trigger();
                return anyhow::Ok(());
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    })
    .await
    .map_err(|_| anyhow::anyhow!("daemon burn test timed out after {:?}", TEST_TIMEOUT))??;

    job_handle
        .await
        .map_err(|e| anyhow::anyhow!("job panicked: {e}"))??;

    assert!(
        pending_burns::get_all(&pool).await?.is_empty(),
        "pending burns should be consumed by the burn"
    );

    let burned_rows = iceberg::burned_session::get_all(harness.trino()).await?;
    assert_eq!(burned_rows.len(), 1, "expected 1 burned session in iceberg");

    let valid_files = valid_data_transfer_files(&awsl.bucket_client()).await?;
    assert!(
        !valid_files.is_empty(),
        "expected valid data transfer session file in S3"
    );

    awsl.cleanup().await?;
    Ok(())
}

/// Full end-to-end flow: ingest report → verified session + Iceberg session →
/// burn → burned session in Iceberg + ValidDataTransferSession in S3.
///
/// This test drives the complete daemon lifecycle in a single pass:
/// 1. An ingest report lands in S3.
/// 2. The daemon's ingest arm processes it: writes a pending burn to the DB,
///    an Iceberg session record, and a VerifiedDataTransferSession to S3.
/// 3. After `initial_burn_delay` the burn arm fires, consuming the pending burn
///    and writing a ValidDataTransferSession to S3 and a burned session to Iceberg.
///
/// Shutdown is triggered only after ALL four outputs are confirmed present so that
/// FileUploadServer (which exits without draining on shutdown) does not miss any
/// uploads.
#[sqlx::test]
async fn daemon_full_flow(pool: PgPool) -> anyhow::Result<()> {
    pending_burns::initialize(&pool).await?;

    let harness = common::setup_iceberg().await?;
    let session_writer = harness
        .get_table_writer(iceberg::session::TABLE_NAME)
        .await?;
    let burned_writer = harness
        .get_table_writer(iceberg::burned_session::TABLE_NAME)
        .await?;

    let awsl = AwsLocal::new().await;
    awsl.create_bucket().await?;

    let file_time = Utc::now() - Duration::hours(1);
    let start_time = file_time - Duration::minutes(1);
    let payer = PublicKeyBinary::from(vec![0]);

    awsl.put_protos_at_time(
        FileType::DataTransferSessionIngestReport.to_string(),
        vec![make_ingest_report(file_time, "full-flow-event-1")],
        file_time,
    )
    .await?;

    // Both file sinks share one FileUpload client / server pair.
    let cache_dir = tempfile::tempdir()?;
    let (file_upload_client, file_upload_server) =
        file_upload::FileUpload::from_bucket_client(awsl.bucket_client()).await;

    let (verified_sessions_sink, verified_sessions_server) =
        VerifiedDataTransferIngestReportV1::file_sink(
            cache_dir.path(),
            file_upload_client.clone(),
            FileSinkCommitStrategy::Manual,
            FileSinkRollTime::Duration(std::time::Duration::from_millis(100)),
            "test",
        )
        .await?;

    let (valid_sessions_sink, valid_sessions_server) = ValidDataTransferSession::file_sink(
        cache_dir.path(),
        file_upload_client,
        FileSinkCommitStrategy::Automatic,
        FileSinkRollTime::Duration(std::time::Duration::from_millis(100)),
        "test",
    )
    .await?;

    // Solana mock configured with sufficient payer balance.
    let burner = Burner::new(
        valid_sessions_sink.clone(),
        TestSolanaClientMap::with(&[(&payer, 1_000_000_000)]).await,
        0,
        std::time::Duration::default(),
        Some(burned_writer),
    );

    let (reports, reports_server) = file_source::continuous_source()
        .state(pool.clone())
        .bucket_client(awsl.bucket_client())
        .prefix(FileType::DataTransferSessionIngestReport.to_string())
        .lookback_start_after(start_time)
        .poll_duration_opt(Some(std::time::Duration::from_millis(100)))
        .create()
        .await?;

    // No-op backfillers — this test exercises only the real-time ingest + burn path.
    let (_, session_rx) = tokio::sync::mpsc::channel(10);
    let (_, burned_rx) = tokio::sync::mpsc::channel(10);

    let ingest_reports = IngestReports::new(
        pool.clone(),
        reports,
        verified_sessions_sink,
        Some(session_writer),
    );

    let daemon = Daemon::new(
        pool.clone(),
        std::time::Duration::from_millis(100), // burn_period
        std::time::Duration::from_millis(100), // min_burn_period
        // initial_burn_delay: long enough for ingest to complete first so the
        // pending burn exists when the burn arm fires.
        std::time::Duration::from_millis(500),
        ingest_reports,
        burner,
        common::TestMobileConfig::all_valid(),
        DataSessionsBackfiller::new(pool.clone(), session_rx, None),
        BurnedSessionsBackfiller::new(pool.clone(), burned_rx, None),
    );

    // On BurnSuccess, manually commit the Automatic FileSink so the rolled file
    // is sent to FileUploadServer immediately rather than waiting for the roll timer.
    tokio::spawn(commit_valid_session_sink_on_burn(
        daemon.event_rx(),
        valid_sessions_sink,
    ));

    // Shutdown: all four outputs must be present before triggering.
    // FileUploadServer exits without draining its upload queue on shutdown,
    // so we confirm S3 files are already there before we pull the trigger.
    let (trigger, listener) = triggered::trigger();

    let job = TaskManager::builder()
        .add_task(file_upload_server)
        .add_task(valid_sessions_server)
        .add_task(verified_sessions_server)
        .add_task(reports_server)
        .add_task(daemon)
        .build();

    let job_handle = tokio::spawn(Box::new(job).start_task(listener));

    let bucket_client = awsl.bucket_client();
    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            let session_rows = iceberg::session::get_all(harness.trino()).await?;
            let burned_rows = iceberg::burned_session::get_all(harness.trino()).await?;
            let verified_files = verified_data_transfer_files(&bucket_client).await?;
            let valid_files = valid_data_transfer_files(&bucket_client).await?;

            if !session_rows.is_empty()
                && !burned_rows.is_empty()
                && !verified_files.is_empty()
                && !valid_files.is_empty()
            {
                trigger.trigger();
                return anyhow::Ok(());
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    })
    .await
    .map_err(|_| anyhow::anyhow!("full flow timed out after {:?}", TEST_TIMEOUT))??;

    job_handle
        .await
        .map_err(|e| anyhow::anyhow!("job panicked: {e}"))??;

    assert!(
        pending_burns::get_all(&pool).await?.is_empty(),
        "burn should have consumed all pending burns"
    );

    let session_rows = iceberg::session::get_all(harness.trino()).await?;
    assert_eq!(
        session_rows.len(),
        1,
        "expected 1 iceberg session from ingest"
    );

    let burned_rows = iceberg::burned_session::get_all(harness.trino()).await?;
    assert_eq!(burned_rows.len(), 1, "expected 1 iceberg burned session");

    let verified_files = awsl
        .bucket_client()
        .list_all_files(
            FileType::VerifiedDataTransferSession.to_string(),
            None,
            None,
        )
        .await?;
    assert!(
        !verified_files.is_empty(),
        "expected verified data transfer session file in S3"
    );

    let valid_files = awsl
        .bucket_client()
        .list_all_files(FileType::ValidDataTransferSession.to_string(), None, None)
        .await?;
    assert!(
        !valid_files.is_empty(),
        "expected valid data transfer session file in S3"
    );

    awsl.cleanup().await?;
    Ok(())
}

/// Verify that the backfill `stop_after` boundary is respected when the daemon
/// runs with embedded backfillers alongside a real-time ingest path.
///
/// **Scenario**:
/// - A "historical" VerifiedDataTransferSession file exists at `T_historical`
///   (one hour before the boundary).
/// - A "new" VerifiedDataTransferSession file exists at `T_new` (30 minutes
///   after the boundary).
/// - Backfill is configured with `stop_after = boundary`.
/// - An ingest report is also present and processed by the real-time path,
///   writing its own Iceberg session record.
///
/// **Expected**:
/// - Iceberg has exactly 2 session records:
///   1. The historical session (written by the backfiller).
///   2. The fresh ingest session (written by the real-time daemon path).
/// - The new VerifiedDataTransferSession file (T_new) is **not** processed by
///   the backfiller because it lies beyond `stop_after`.
#[sqlx::test]
async fn daemon_backfill_boundary(pool: PgPool) -> anyhow::Result<()> {
    pending_burns::initialize(&pool).await?;

    let harness = common::setup_iceberg().await?;
    let session_writer = harness
        .get_table_writer(iceberg::session::TABLE_NAME)
        .await?;

    let awsl = AwsLocal::new().await;
    awsl.create_bucket().await?;

    // Times
    let boundary = Utc::now() - Duration::hours(1); // "Iceberg deployment date"
    let historical_time = boundary - Duration::hours(1); // before boundary → backfiller processes
    let new_verified_time = boundary + Duration::minutes(30); // after boundary → backfiller skips
    let ingest_time = Utc::now() - Duration::minutes(5); // fresh ingest → real-time path
    let ingest_start = ingest_time - Duration::minutes(1);

    // Historical VerifiedDataTransferSession file (before boundary)
    awsl.put_protos_at_time(
        FileType::VerifiedDataTransferSession.to_string(),
        vec![make_verified_report(historical_time, "historical-event")],
        historical_time,
    )
    .await?;

    // New VerifiedDataTransferSession file (after boundary) — backfiller must skip this
    awsl.put_protos_at_time(
        FileType::VerifiedDataTransferSession.to_string(),
        vec![make_verified_report(
            new_verified_time,
            "new-verified-event",
        )],
        new_verified_time,
    )
    .await?;

    // Fresh ingest report — processed by the daemon's real-time path
    awsl.put_protos_at_time(
        FileType::DataTransferSessionIngestReport.to_string(),
        vec![make_ingest_report(ingest_time, "ingest-event")],
        ingest_time,
    )
    .await?;

    // Backfill: start before historical_time, stop at boundary
    let backfill_start = historical_time - Duration::minutes(1);
    let backfill_opts = common::test_backfill_options("boundary-test", backfill_start, boundary);
    let (session_backfill, session_backfill_server) = DataSessionsBackfiller::create(
        pool.clone(),
        awsl.bucket_client(),
        Some(session_writer),
        Some(backfill_opts),
    )
    .await?;

    let (_, burned_rx) = tokio::sync::mpsc::channel(10);
    let burned_backfill = BurnedSessionsBackfiller::new(pool.clone(), burned_rx, None);

    // Real-time reports poller for the fresh ingest file.
    let (reports, reports_server) = file_source::continuous_source()
        .state(pool.clone())
        .bucket_client(awsl.bucket_client())
        .prefix(FileType::DataTransferSessionIngestReport.to_string())
        .lookback_start_after(ingest_start)
        .poll_duration_opt(Some(std::time::Duration::from_millis(100)))
        .create()
        .await?;

    // No-op file sinks for verified sessions output (not asserting S3 in this test).
    let (verified_tx, _verified_rx) = tokio::sync::mpsc::channel(100);
    // No real-time Iceberg writer: the ingest path produces a pending_burn which
    // serves as its "done" signal; session rows come from the backfiller only.
    let ingest_reports = IngestReports::new(
        pool.clone(),
        reports,
        FileSinkClient::new(verified_tx, "test"),
        None,
    );

    let daemon = Daemon::new(
        pool.clone(),
        NO_BURN,
        NO_BURN,
        NO_BURN,
        ingest_reports,
        noop_burner(),
        common::TestMobileConfig::all_valid(),
        session_backfill,
        burned_backfill,
    );

    let (trigger, listener) = triggered::trigger();

    let job = TaskManager::builder()
        .add_task(session_backfill_server)
        .add_task(reports_server)
        .add_task(daemon)
        .build();

    let job_handle = tokio::spawn(Box::new(job).start_task(listener));

    // Shutdown: wait for:
    //   - the historical backfill row in Iceberg, AND
    //   - the pending burn from the fresh ingest (signals real-time path completed)
    let pool_watch = pool.clone();
    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            let iceberg_rows = iceberg::session::get_all(harness.trino()).await?;
            let burns = pending_burns::get_all(&pool_watch).await?;
            // 1 backfilled row + ingest produced a pending burn
            if !iceberg_rows.is_empty() && !burns.is_empty() {
                trigger.trigger();
                return anyhow::Ok(());
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    })
    .await
    .map_err(|_| anyhow::anyhow!("boundary test timed out after {:?}", TEST_TIMEOUT))??;

    job_handle
        .await
        .map_err(|e| anyhow::anyhow!("job panicked: {e}"))??;

    // The backfiller should have written exactly 1 row: the historical session.
    // The "new" VerifiedDataTransferSession file (T_new) must NOT appear because
    // it lies beyond stop_after.
    let rows = iceberg::session::get_all(harness.trino()).await?;
    assert_eq!(
        rows.len(),
        1,
        "backfiller must write only the historical session; \
         new-verified-event (past stop_after) must be excluded"
    );

    // The fresh ingest produced a pending burn (real-time path ran correctly).
    let burns = pending_burns::get_all(&pool).await?;
    assert_eq!(burns.len(), 1, "ingest should have produced 1 pending burn");

    awsl.cleanup().await?;
    Ok(())
}

async fn verified_data_transfer_files(
    bucket_client: &BucketClient,
) -> anyhow::Result<Vec<FileInfo>> {
    let files = bucket_client
        .list_all_files(FileType::VerifiedDataTransferSession.to_str(), None, None)
        .await?;
    Ok(files)
}

async fn valid_data_transfer_files(bucket_client: &BucketClient) -> anyhow::Result<Vec<FileInfo>> {
    let files = bucket_client
        .list_all_files(FileType::ValidDataTransferSession.to_str(), None, None)
        .await
        .unwrap_or_default();
    Ok(files)
}

async fn commit_valid_session_sink_on_burn(
    mut events: tokio::sync::broadcast::Receiver<mobile_packet_verifier::daemon::DaemonEvent>,
    valid_sessions_sink: FileSinkClient<ValidDataTransferSession>,
) {
    while let Ok(event) = events.recv().await {
        if matches!(
            event,
            mobile_packet_verifier::daemon::DaemonEvent::BurnSuccess
        ) {
            let _ = valid_sessions_sink
                .commit()
                .await
                .expect("commit sent")
                .await
                .expect("commit oneshot");
        }
    }
}

mod trigger {
    use file_store::file_upload::FileUpload;
    use mobile_packet_verifier::daemon::DaemonEvent;
    use mobile_packet_verifier::iceberg;
    use std::time::Duration;
    use tokio::sync::broadcast::Receiver;
    use tokio::time::sleep;

    pub trait TriggerExt {
        async fn when_iceberg_sessions_exist(
            self,
            trino: trino_rust_client::Client,
        ) -> anyhow::Result<()>;
        async fn when_uploads_completed_at_least(
            self,
            n_uploads: u64,
            events: Receiver<DaemonEvent>,
            file_upload_watcher: FileUpload,
        ) -> anyhow::Result<()>;

        async fn after_sleep(self, ms: u64);
    }

    impl TriggerExt for triggered::Trigger {
        async fn when_iceberg_sessions_exist(
            self,
            trino: trino_rust_client::Client,
        ) -> anyhow::Result<()> {
            loop {
                let rows = iceberg::session::get_all(&trino).await.unwrap_or_default();
                if !rows.is_empty() {
                    self.trigger();
                    return Ok(());
                }
                sleep(Duration::from_millis(100)).await;
            }
        }

        async fn when_uploads_completed_at_least(
            self,
            n_uploads: u64,
            mut events: Receiver<DaemonEvent>,
            file_upload_watcher: FileUpload,
        ) -> anyhow::Result<()> {
            while let Ok(event) = events.recv().await {
                if matches!(event, DaemonEvent::ReportHandle) {
                    file_upload_watcher
                        .wait_for_uploads_at_least(n_uploads)
                        .await;
                    self.trigger();
                    return Ok(());
                }
            }
            anyhow::bail!("no report handle event received");
        }

        async fn after_sleep(self, ms: u64) {
            sleep(Duration::from_millis(ms)).await;
            self.trigger();
        }
    }
}
