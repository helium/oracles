use chrono::{DateTime, Duration, Utc};
use file_store::{aws_local::AwsLocal, file_source};
use file_store_oracles::{
    mobile_ban::{
        BanAction, BanDetails, BanReason, BanReport, BanRequest, BanType,
        VerifiedBanIngestReportStatus, VerifiedBanReport,
    },
    mobile_session::{DataTransferEvent, DataTransferSessionIngestReport, DataTransferSessionReq},
    FileType,
};
use helium_crypto::PublicKeyBinary;
use helium_proto::services::poc_mobile::{
    CarrierIdV2, DataTransferRadioAccessTechnology, DataTransferSessionIngestReportV1,
};
use mobile_packet_verifier::{
    banning::handle_verified_ban_report,
    burner::Burner,
    daemon::{Daemon, IngestReports},
    iceberg,
    pending_burns::{self, DataTransferSession},
    routing::RoutingKeys,
};
use solana::{self, burn::TestSolanaClientMap};
use sqlx::PgPool;
use task_manager::{ManagedTask, TaskManager};

use crate::{
    common::{self, hotspot_inventory::MobileHotspotInventory},
    daemon::trigger::TriggerExt,
};

const TEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
/// Burn period much longer than the test timeout — ensures burn never fires during tests.
const NO_BURN: std::time::Duration = std::time::Duration::from_secs(3600);

fn make_ingest_report(
    timestamp: chrono::DateTime<Utc>,
    event_id: &str,
) -> DataTransferSessionIngestReportV1 {
    let key = PublicKeyBinary::from(vec![1]);
    make_ingest_report_with_keys(timestamp, event_id, key.clone(), key)
}

fn make_ingest_report_with_keys(
    timestamp: chrono::DateTime<Utc>,
    event_id: &str,
    gw_pubkey: PublicKeyBinary,
    routing_pubkey: PublicKeyBinary,
) -> DataTransferSessionIngestReportV1 {
    DataTransferSessionIngestReport {
        received_timestamp: timestamp,
        report: DataTransferSessionReq {
            rewardable_bytes: 1_000,
            pub_key: routing_pubkey,
            signature: vec![],
            carrier_id: CarrierIdV2::Carrier9,
            sampling: false,
            data_transfer_usage: DataTransferEvent {
                pub_key: gw_pubkey,
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

fn make_ban_report(
    received_timestamp: DateTime<Utc>,
    hotspot_pubkey: &PublicKeyBinary,
    expiration: Option<DateTime<Utc>>,
) -> VerifiedBanReport {
    VerifiedBanReport {
        verified_timestamp: received_timestamp,
        status: VerifiedBanIngestReportStatus::Valid,
        report: BanReport {
            received_timestamp,
            report: BanRequest {
                hotspot_pubkey: hotspot_pubkey.clone(),
                timestamp: received_timestamp,
                ban_pubkey: PublicKeyBinary::from(vec![0]),
                signature: vec![],
                ban_action: BanAction::Ban(BanDetails {
                    hotspot_serial: "test-serial".to_string(),
                    message: "test-ban".to_string(),
                    reason: BanReason::LocationGaming,
                    ban_type: BanType::All,
                    expiration_timestamp: expiration,
                }),
            },
        },
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

/// Verify that the daemon correctly processes incoming data transfer ingest reports.
/// The primary ingest path is exercised end-to-end:
///   - one ingest report → pending burn in DB
///   - Iceberg session record
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
    let invalid_session_writer = harness
        .get_table_writer(iceberg::invalid_session::TABLE_NAME)
        .await?;
    let burned_writer = harness
        .get_table_writer(iceberg::burned_session::TABLE_NAME)
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

    // Seed the report's gateway (vec![1]) as known, as of before the report time.
    common::hotspot_inventory::seed(
        &harness,
        vec![MobileHotspotInventory::known(
            &PublicKeyBinary::from(vec![1]),
            file_time - Duration::hours(1),
        )],
    )
    .await?;
    let resolver = common::gateway_resolver(&harness).await?;

    let ingest_reports = IngestReports::new(
        pool.clone(),
        reports,
        session_writer,
        invalid_session_writer,
        resolver,
        RoutingKeys::from_iter([PublicKeyBinary::from(vec![1])]),
    );

    let burner = Burner::new(
        TestSolanaClientMap::default(),
        0,
        std::time::Duration::default(),
        burned_writer,
    );

    let daemon = Daemon::new(
        pool.clone(),
        NO_BURN,
        NO_BURN,
        NO_BURN, // initial_burn_delay — burn never fires in this test
        ingest_reports,
        burner,
    );

    let (trigger, listener) = triggered::trigger();
    let events = daemon.event_rx();

    let job = TaskManager::builder()
        .add_task(reports_server)
        .add_task(daemon)
        .add_task(|_| async {
            // Shutdown on ReportHandle: it fires after the file has been
            // handled, so its Iceberg and Postgres writes are done.
            trigger
                .when_report_handled(events)
                .await
                .expect("report handled");
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

    awsl.cleanup().await?;
    Ok(())
}

/// Verify that the daemon's burn arm fires, consumes pending burns, and records
/// a burned session in Iceberg.
///
/// This test exercises the full burn lifecycle inside the daemon's select! loop.
/// The ingest path is bypassed — pending burns are seeded directly in the DB.
/// A short `initial_burn_delay` (100 ms) ensures the burn fires quickly.
///
/// Shutdown is driven by polling for the Iceberg burned_session row.
#[sqlx::test]
async fn daemon_burns_sessions(pool: PgPool) -> anyhow::Result<()> {
    pending_burns::initialize(&pool).await?;

    let harness = common::setup_iceberg().await?;
    let session_writer = harness
        .get_table_writer(iceberg::session::TABLE_NAME)
        .await?;
    let invalid_session_writer = harness
        .get_table_writer(iceberg::invalid_session::TABLE_NAME)
        .await?;
    let burned_writer = harness
        .get_table_writer(iceberg::burned_session::TABLE_NAME)
        .await?;

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

    // Burner with TestSolanaClientMap configured with sufficient payer balance.
    let solana = TestSolanaClientMap::default();
    solana.insert(&payer, 1_000_000_000).await;
    let burner = Burner::new(solana, 0, std::time::Duration::default(), burned_writer);

    // Keep reports channel sender alive so the daemon's ingest arm just blocks.
    let (_reports_tx, reports_rx) = tokio::sync::mpsc::channel(1);

    // No real-time Iceberg writer — this test only exercises the burn path.
    // This test seeds pending burns directly and sends no ingest reports, so the
    // resolver is never queried; an empty one is sufficient.
    let resolver = common::gateway_resolver(&harness).await?;

    let ingest_reports = IngestReports::new(
        pool.clone(),
        reports_rx,
        session_writer,
        invalid_session_writer,
        resolver,
        RoutingKeys::default(),
    );

    let daemon = Daemon::new(
        pool.clone(),
        std::time::Duration::from_millis(100), // burn_period (short)
        std::time::Duration::from_millis(100), // min_burn_period
        std::time::Duration::from_millis(100), // initial_burn_delay — fires in ~100ms
        ingest_reports,
        burner,
    );

    // Shutdown: poll for the Iceberg burned_session row.
    let (trigger, listener) = triggered::trigger();

    let job = TaskManager::builder().add_task(daemon).build();

    let job_handle = tokio::spawn(Box::new(job).start_task(listener));

    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            let rows = iceberg::burned_session::get_all(harness.trino()).await?;

            if !rows.is_empty() {
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

    Ok(())
}

/// Full end-to-end flow: ingest report → Iceberg session → burn → burned
/// session in Iceberg.
///
/// This test drives the complete daemon lifecycle in a single pass:
/// 1. An ingest report lands in S3.
/// 2. The daemon's ingest arm processes it: writes a pending burn to the DB and
///    an Iceberg session record.
/// 3. After `initial_burn_delay` the burn arm fires, consuming the pending burn
///    and writing a burned session to Iceberg.
///
/// Shutdown is triggered once both Iceberg outputs are present.
#[sqlx::test]
async fn daemon_full_flow(pool: PgPool) -> anyhow::Result<()> {
    pending_burns::initialize(&pool).await?;

    let harness = common::setup_iceberg().await?;
    let session_writer = harness
        .get_table_writer(iceberg::session::TABLE_NAME)
        .await?;
    let invalid_session_writer = harness
        .get_table_writer(iceberg::invalid_session::TABLE_NAME)
        .await?;
    let burned_writer = harness
        .get_table_writer(iceberg::burned_session::TABLE_NAME)
        .await?;

    let awsl = AwsLocal::new().await;
    awsl.create_bucket().await?;

    let file_time = Utc::now() - Duration::hours(1);
    let start_time = file_time - Duration::minutes(1);
    let payer = PublicKeyBinary::from(vec![0]);

    let valid_gw = PublicKeyBinary::from(vec![1]);
    let invalid_gw = PublicKeyBinary::from(vec![2]);
    let banned_gw = PublicKeyBinary::from(vec![3]);

    let valid_routing = PublicKeyBinary::from(vec![4]);
    let invalid_routing = PublicKeyBinary::from(vec![5]);

    let mk_report = |label: &str, gw: &PublicKeyBinary, routing: &PublicKeyBinary| {
        make_ingest_report_with_keys(file_time, label, gw.clone(), routing.clone())
    };

    let ingest_report = mk_report("full-flow-event-1", &valid_gw, &valid_routing);
    let duplicate_report = ingest_report.clone();
    let banned_report = mk_report("banned-event-1", &banned_gw, &valid_routing);
    let unknown_gw_report = mk_report("unknown-gw-event-1", &invalid_gw, &valid_routing);
    let invalid_routing_report = mk_report("invalid-routing-event-1", &valid_gw, &invalid_routing);
    awsl.put_protos_at_time(
        FileType::DataTransferSessionIngestReport.to_string(),
        vec![
            ingest_report,
            duplicate_report,
            banned_report,
            unknown_gw_report,
            invalid_routing_report,
        ],
        file_time,
    )
    .await?;

    // Bans are not injected into the Daemon, so we ban through the DB.
    let mut conn = pool.acquire().await?;
    let ban_report = make_ban_report(Utc::now() - Duration::hours(2), &banned_gw, None);
    handle_verified_ban_report(&mut conn, ban_report).await?;

    // Solana mock configured with sufficient payer balance.
    let burner = Burner::new(
        TestSolanaClientMap::with(&[(&payer, 1_000_000_000)]).await,
        0,
        std::time::Duration::default(),
        burned_writer,
    );

    let (reports, reports_server) = file_source::continuous_source()
        .state(pool.clone())
        .bucket_client(awsl.bucket_client())
        .prefix(FileType::DataTransferSessionIngestReport.to_string())
        .lookback_start_after(start_time)
        .poll_duration_opt(Some(std::time::Duration::from_millis(100)))
        .create()
        .await?;

    // Seed only `valid_gw` as known and allow-list only `valid_routing`, so the
    // unknown-gateway and invalid-routing reports are rejected. `banned_gw` is
    // rejected earlier by the ban check and needs no inventory row.
    common::hotspot_inventory::seed(
        &harness,
        vec![MobileHotspotInventory::known(
            &valid_gw,
            file_time - Duration::hours(1),
        )],
    )
    .await?;
    let resolver = common::gateway_resolver(&harness).await?;

    let ingest_reports = IngestReports::new(
        pool.clone(),
        reports,
        session_writer,
        invalid_session_writer,
        resolver,
        RoutingKeys::from_iter([valid_routing.clone()]),
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
    );

    // Shutdown: both Iceberg outputs must be present before triggering.
    let (trigger, listener) = triggered::trigger();

    let job = TaskManager::builder()
        .add_task(reports_server)
        .add_task(daemon)
        .build();

    let job_handle = tokio::spawn(Box::new(job).start_task(listener));

    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            let session_rows = iceberg::session::get_all(harness.trino()).await?;
            let burned_rows = iceberg::burned_session::get_all(harness.trino()).await?;

            if !session_rows.is_empty() && !burned_rows.is_empty() {
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

    let invalid_session_rows = iceberg::invalid_session::get_all(harness.trino()).await?;
    assert_eq!(
        invalid_session_rows.len(),
        4,
        "expected 4 iceberg invalid session from ingest"
    );

    let burned_rows = iceberg::burned_session::get_all(harness.trino()).await?;
    assert_eq!(burned_rows.len(), 1, "expected 1 iceberg burned session");

    awsl.cleanup().await?;
    Ok(())
}

mod trigger {
    use mobile_packet_verifier::daemon::DaemonEvent;
    use tokio::sync::broadcast::Receiver;

    pub trait TriggerExt {
        async fn when_report_handled(self, events: Receiver<DaemonEvent>) -> anyhow::Result<()>;
    }

    impl TriggerExt for triggered::Trigger {
        async fn when_report_handled(
            self,
            mut events: Receiver<DaemonEvent>,
        ) -> anyhow::Result<()> {
            while let Ok(event) = events.recv().await {
                if matches!(event, DaemonEvent::ReportHandle) {
                    self.trigger();
                    return Ok(());
                }
            }
            anyhow::bail!("no report handle event received");
        }
    }
}
