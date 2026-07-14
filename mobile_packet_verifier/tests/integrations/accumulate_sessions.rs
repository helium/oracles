use chrono::{DateTime, Duration, Utc};
use file_store::file_sink::{FileSinkClient, MessageReceiver};
use file_store_oracles::{
    mobile_ban::{
        BanAction, BanDetails, BanReason, BanReport, BanRequest, BanType,
        VerifiedBanIngestReportStatus, VerifiedBanReport,
    },
    mobile_session::{DataTransferEvent, DataTransferSessionIngestReport, DataTransferSessionReq},
};
use helium_crypto::PublicKeyBinary;
use helium_iceberg::IcebergTestHarness;
use helium_proto::services::poc_mobile::{
    CarrierIdV2, DataTransferRadioAccessTechnology, VerifiedDataTransferIngestReportV1,
};
use mobile_packet_verifier::{
    banning, bytes_to_dc, daemon::handle_data_transfer_session_file, iceberg, pending_burns,
    routing::RoutingKeys,
};
use sqlx::PgPool;

use crate::common::{self, hotspot_inventory::MobileHotspotInventory, TestChannelExt};

#[sqlx::test]
async fn accumulate_no_reports(pool: PgPool) -> anyhow::Result<()> {
    let harness = common::setup_iceberg().await?;
    let writer = harness
        .get_table_writer(iceberg::session::TABLE_NAME)
        .await?;

    let mut report_rx =
        run_accumulate_sessions(&pool, &harness, vec![], vec![], vec![], Some(writer)).await?;

    report_rx.assert_is_empty()?;

    let pending = pending_burns::get_all(&pool).await?;
    assert!(pending.is_empty());

    Ok(())
}

#[sqlx::test]
async fn accumlate_reports_for_same_key(pool: PgPool) -> anyhow::Result<()> {
    let harness = common::setup_iceberg().await?;
    let writer = harness
        .get_table_writer(iceberg::session::TABLE_NAME)
        .await?;

    let key = PublicKeyBinary::from(vec![1]);

    let reports = vec![
        DataTransferSessionIngestReport {
            received_timestamp: Utc::now(),
            report: DataTransferSessionReq {
                rewardable_bytes: 1_000,
                pub_key: key.clone(),
                signature: vec![],
                carrier_id: CarrierIdV2::Carrier9,
                sampling: false,
                data_transfer_usage: DataTransferEvent {
                    pub_key: key.clone(),
                    upload_bytes: 1,
                    download_bytes: 2,
                    radio_access_technology: DataTransferRadioAccessTechnology::Wlan,
                    event_id: "test".to_string(),
                    payer: vec![0].into(),
                    timestamp: Utc::now(),
                    signature: vec![],
                },
            },
        },
        DataTransferSessionIngestReport {
            received_timestamp: Utc::now(),
            report: DataTransferSessionReq {
                rewardable_bytes: 1_000,
                pub_key: key.clone(),
                signature: vec![],
                carrier_id: CarrierIdV2::Carrier9,
                sampling: false,
                data_transfer_usage: DataTransferEvent {
                    pub_key: key.clone(),
                    upload_bytes: 1,
                    download_bytes: 2,
                    radio_access_technology: DataTransferRadioAccessTechnology::Wlan,
                    event_id: "test".to_string(),
                    payer: vec![0].into(),
                    timestamp: Utc::now(),
                    signature: vec![],
                },
            },
        },
    ];

    let mut report_rx = run_accumulate_sessions(
        &pool,
        &harness,
        reports,
        vec![key.clone()],
        vec![key.clone()],
        Some(writer),
    )
    .await?;

    report_rx.assert_num_msgs(2)?;

    let pending = pending_burns::get_all(&pool).await?;
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].dc_to_burn(), bytes_to_dc(2_000));

    Ok(())
}

#[sqlx::test]
async fn accumulate_writes_zero_data_event_as_verified_but_not_for_burning(
    pool: PgPool,
) -> anyhow::Result<()> {
    let harness = common::setup_iceberg().await?;
    let writer = harness
        .get_table_writer(iceberg::session::TABLE_NAME)
        .await?;

    let key = PublicKeyBinary::from(vec![0]);

    let reports = vec![DataTransferSessionIngestReport {
        report: DataTransferSessionReq {
            data_transfer_usage: DataTransferEvent {
                pub_key: key.clone(),
                upload_bytes: 1,
                download_bytes: 2,
                radio_access_technology: DataTransferRadioAccessTechnology::Wlan,
                event_id: "test".to_string(),
                payer: vec![0].into(),
                timestamp: Utc::now(),
                signature: vec![],
            },
            rewardable_bytes: 0,
            pub_key: key.clone(),
            signature: vec![],
            carrier_id: CarrierIdV2::Carrier9,
            sampling: false,
        },
        received_timestamp: Utc::now(),
    }];

    let mut report_rx = run_accumulate_sessions(
        &pool,
        &harness,
        reports,
        vec![key.clone()],
        vec![key.clone()],
        Some(writer),
    )
    .await?;

    report_rx.assert_not_empty()?;

    let pending = pending_burns::get_all(&pool).await?;
    assert!(pending.is_empty());

    Ok(())
}

#[sqlx::test]
async fn writes_valid_event_to_db(pool: PgPool) -> anyhow::Result<()> {
    let harness = common::setup_iceberg().await?;
    let writer = harness
        .get_table_writer(iceberg::session::TABLE_NAME)
        .await?;

    let key = PublicKeyBinary::from(vec![0]);

    let reports = vec![DataTransferSessionIngestReport {
        received_timestamp: Utc::now(),
        report: DataTransferSessionReq {
            rewardable_bytes: 1_000,
            pub_key: key.clone(),
            signature: vec![],
            carrier_id: CarrierIdV2::Carrier9,
            sampling: false,
            data_transfer_usage: DataTransferEvent {
                pub_key: key.clone(),
                upload_bytes: 1_000,
                download_bytes: 1_000,
                radio_access_technology: DataTransferRadioAccessTechnology::Wlan,
                event_id: "test-event-id".to_string(),
                payer: PublicKeyBinary::from(vec![0]),
                timestamp: Utc::now(),
                signature: vec![],
            },
        },
    }];

    let mut report_rx = run_accumulate_sessions(
        &pool,
        &harness,
        reports,
        vec![key.clone()],
        vec![key.clone()],
        Some(writer),
    )
    .await?;

    report_rx.assert_not_empty()?;

    let pending = pending_burns::get_all(&pool).await?;
    assert_eq!(pending.len(), 1);

    Ok(())
}

#[sqlx::test]
async fn ignores_cbrs_data_sessions(pool: PgPool) -> anyhow::Result<()> {
    let harness = common::setup_iceberg().await?;
    let writer = harness
        .get_table_writer(iceberg::session::TABLE_NAME)
        .await?;

    let reports = vec![DataTransferSessionIngestReport {
        received_timestamp: Utc::now(),
        report: DataTransferSessionReq {
            rewardable_bytes: 1_000,
            pub_key: PublicKeyBinary::from(vec![0]),
            signature: vec![],
            carrier_id: CarrierIdV2::Carrier9,
            sampling: false,
            data_transfer_usage: DataTransferEvent {
                pub_key: PublicKeyBinary::from(vec![0]),
                upload_bytes: 1_000,
                download_bytes: 1_000,
                // NOTE: Eutran is CBRS
                radio_access_technology: DataTransferRadioAccessTechnology::Eutran,
                event_id: "test-event-id".to_string(),
                payer: PublicKeyBinary::from(vec![0]),
                timestamp: Utc::now(),
                signature: vec![],
            },
        },
    }];

    // CBRS reports are dropped before the gateway/routing checks run, so no
    // gateway needs to be seeded and no routing key allow-listed.
    let mut report_rx =
        run_accumulate_sessions(&pool, &harness, reports, vec![], vec![], Some(writer)).await?;

    // record not written to file or db
    report_rx.assert_is_empty()?;

    let pending = pending_burns::get_all(&pool).await?;
    assert!(pending.is_empty());

    Ok(())
}

#[sqlx::test]
async fn ignores_invalid_gateway_keys(pool: PgPool) -> anyhow::Result<()> {
    let harness = common::setup_iceberg().await?;
    let writer = harness
        .get_table_writer(iceberg::session::TABLE_NAME)
        .await?;

    let key = PublicKeyBinary::from(vec![0]);

    let reports = vec![DataTransferSessionIngestReport {
        received_timestamp: Utc::now(),
        report: DataTransferSessionReq {
            rewardable_bytes: 1_000,
            pub_key: key.clone(),
            signature: vec![],
            carrier_id: CarrierIdV2::Carrier9,
            sampling: false,
            data_transfer_usage: DataTransferEvent {
                pub_key: key.clone(),
                upload_bytes: 1_000,
                download_bytes: 1_000,
                radio_access_technology: DataTransferRadioAccessTechnology::Wlan,
                event_id: "test-event-id".to_string(),
                payer: PublicKeyBinary::from(vec![0]),
                timestamp: Utc::now(),
                signature: vec![],
            },
        },
    }];

    // Gateway is NOT seeded into the inventory -> InvalidGatewayKey. The routing
    // key is allow-listed so the gateway check is unambiguously the cause.
    let mut report_rx = run_accumulate_sessions(
        &pool,
        &harness,
        reports,
        vec![],
        vec![key.clone()],
        Some(writer),
    )
    .await?;

    // record written to file, but not db
    report_rx.assert_not_empty()?;

    let pending = pending_burns::get_all(&pool).await?;
    assert!(pending.is_empty());

    Ok(())
}

#[sqlx::test]
async fn ignores_invalid_routing_keys(pool: PgPool) -> anyhow::Result<()> {
    let harness = common::setup_iceberg().await?;
    let writer = harness
        .get_table_writer(iceberg::session::TABLE_NAME)
        .await?;

    let key = PublicKeyBinary::from(vec![0]);

    let reports = vec![DataTransferSessionIngestReport {
        received_timestamp: Utc::now(),
        report: DataTransferSessionReq {
            rewardable_bytes: 1_000,
            pub_key: key.clone(),
            signature: vec![],
            carrier_id: CarrierIdV2::Carrier9,
            sampling: false,
            data_transfer_usage: DataTransferEvent {
                pub_key: key.clone(),
                upload_bytes: 1_000,
                download_bytes: 1_000,
                radio_access_technology: DataTransferRadioAccessTechnology::Wlan,
                event_id: "test-event-id".to_string(),
                payer: PublicKeyBinary::from(vec![0]),
                timestamp: Utc::now(),
                signature: vec![],
            },
        },
    }];

    // Gateway is seeded (known) but the routing key is NOT allow-listed ->
    // InvalidRoutingKey.
    let mut report_rx = run_accumulate_sessions(
        &pool,
        &harness,
        reports,
        vec![key.clone()],
        vec![],
        Some(writer),
    )
    .await?;

    // record written to file, but not db
    report_rx.assert_not_empty()?;

    let pending = pending_burns::get_all(&pool).await?;
    assert!(pending.is_empty());

    Ok(())
}

#[sqlx::test]
async fn ignores_ban_type_all_keys(pool: PgPool) -> anyhow::Result<()> {
    let harness = common::setup_iceberg().await?;
    let writer = harness
        .get_table_writer(iceberg::session::TABLE_NAME)
        .await?;

    let key = PublicKeyBinary::from(vec![1]);

    let reports = vec![DataTransferSessionIngestReport {
        received_timestamp: Utc::now(),
        report: DataTransferSessionReq {
            rewardable_bytes: 1_000,
            pub_key: key.clone(),
            signature: vec![],
            carrier_id: CarrierIdV2::Carrier9,
            sampling: false,
            data_transfer_usage: DataTransferEvent {
                pub_key: key.clone(),
                upload_bytes: 1_000,
                download_bytes: 1_000,
                radio_access_technology: DataTransferRadioAccessTechnology::Wlan,
                event_id: "test-event-id".to_string(),
                payer: PublicKeyBinary::from(vec![0]),
                timestamp: Utc::now(),
                signature: vec![],
            },
        },
    }];

    // Ban radio
    ban_hotspot(&pool, key.clone(), BanType::All).await?;

    // Otherwise-valid gateway/routing so the ban is the reason for rejection.
    let mut report_rx = run_accumulate_sessions(
        &pool,
        &harness,
        reports,
        vec![key.clone()],
        vec![key.clone()],
        Some(writer),
    )
    .await?;

    // record written to file, but not db
    report_rx.assert_not_empty()?;

    let pending = pending_burns::get_all(&pool).await?;
    assert!(pending.is_empty());

    Ok(())
}

#[sqlx::test]
async fn ignores_ban_type_data_transfer_keys(pool: PgPool) -> anyhow::Result<()> {
    let harness = common::setup_iceberg().await?;
    let writer = harness
        .get_table_writer(iceberg::session::TABLE_NAME)
        .await?;

    let key = PublicKeyBinary::from(vec![1]);

    let reports = vec![DataTransferSessionIngestReport {
        received_timestamp: Utc::now(),
        report: DataTransferSessionReq {
            rewardable_bytes: 1_000,
            pub_key: key.clone(),
            signature: vec![],
            carrier_id: CarrierIdV2::Carrier9,
            sampling: false,
            data_transfer_usage: DataTransferEvent {
                pub_key: key.clone(),
                upload_bytes: 1_000,
                download_bytes: 1_000,
                radio_access_technology: DataTransferRadioAccessTechnology::Wlan,
                event_id: "test-event-id".to_string(),
                payer: PublicKeyBinary::from(vec![0]),
                timestamp: Utc::now(),
                signature: vec![],
            },
        },
    }];

    // Ban radio
    ban_hotspot(&pool, key.clone(), BanType::Data).await?;

    let mut report_rx = run_accumulate_sessions(
        &pool,
        &harness,
        reports,
        vec![key.clone()],
        vec![key.clone()],
        Some(writer),
    )
    .await?;

    // record written to file, but not db
    report_rx.assert_not_empty()?;

    let pending = pending_burns::get_all(&pool).await?;
    assert!(pending.is_empty());

    Ok(())
}

#[sqlx::test]
async fn allows_ban_type_poc_keys(pool: PgPool) -> anyhow::Result<()> {
    let harness = common::setup_iceberg().await?;
    let writer = harness
        .get_table_writer(iceberg::session::TABLE_NAME)
        .await?;

    let key = PublicKeyBinary::from(vec![1]);

    let reports = vec![DataTransferSessionIngestReport {
        received_timestamp: Utc::now(),
        report: DataTransferSessionReq {
            rewardable_bytes: 1_000,
            pub_key: key.clone(),
            signature: vec![],
            carrier_id: CarrierIdV2::Carrier9,
            sampling: false,
            data_transfer_usage: DataTransferEvent {
                pub_key: key.clone(),
                upload_bytes: 1_000,
                download_bytes: 1_000,
                radio_access_technology: DataTransferRadioAccessTechnology::Wlan,
                event_id: "test-event-id".to_string(),
                payer: PublicKeyBinary::from(vec![0]),
                timestamp: Utc::now(),
                signature: vec![],
            },
        },
    }];

    // Ban radio
    ban_hotspot(&pool, key.clone(), BanType::Poc).await?;

    // A POC ban doesn't block data transfer, so the gateway/routing must be
    // valid for the session to be burned.
    let mut report_rx = run_accumulate_sessions(
        &pool,
        &harness,
        reports,
        vec![key.clone()],
        vec![key.clone()],
        Some(writer),
    )
    .await?;

    // record written to file and db
    report_rx.assert_not_empty()?;

    let pending = pending_burns::get_all(&pool).await?;
    assert!(!pending.is_empty());
    Ok(())
}

#[sqlx::test]
async fn allows_expired_ban_type_data_transfer_keys(pool: PgPool) -> anyhow::Result<()> {
    let harness = common::setup_iceberg().await?;
    let writer = harness
        .get_table_writer(iceberg::session::TABLE_NAME)
        .await?;

    let key = PublicKeyBinary::from(vec![1]);

    let reports = vec![DataTransferSessionIngestReport {
        received_timestamp: Utc::now(),
        report: DataTransferSessionReq {
            rewardable_bytes: 1_000,
            pub_key: key.clone(),
            signature: vec![],
            carrier_id: CarrierIdV2::Carrier9,
            sampling: false,
            data_transfer_usage: DataTransferEvent {
                pub_key: key.clone(),
                upload_bytes: 1_000,
                download_bytes: 1_000,
                radio_access_technology: DataTransferRadioAccessTechnology::Wlan,
                event_id: "test-event-id".to_string(),
                payer: PublicKeyBinary::from(vec![0]),
                timestamp: Utc::now(),
                signature: vec![],
            },
        },
    }];

    // Ban radio with expiration in the past
    let mut conn = pool.acquire().await?;
    banning::db::update_hotspot_ban(
        &mut conn,
        VerifiedBanReport {
            verified_timestamp: Utc::now(),
            status: VerifiedBanIngestReportStatus::Valid,
            report: BanReport {
                received_timestamp: Utc::now(),
                report: BanRequest {
                    hotspot_pubkey: key.clone(),
                    timestamp: Utc::now(),
                    ban_pubkey: PublicKeyBinary::from(vec![0]),
                    signature: vec![],
                    ban_action: BanAction::Ban(BanDetails {
                        hotspot_serial: "serial".to_string(),
                        message: "notes".to_string(),
                        reason: BanReason::LocationGaming,
                        ban_type: BanType::Data,
                        expiration_timestamp: Some(Utc::now() - Duration::hours(6)),
                    }),
                },
            },
        },
    )
    .await?;

    let mut report_rx = run_accumulate_sessions(
        &pool,
        &harness,
        reports,
        vec![key.clone()],
        vec![key.clone()],
        Some(writer),
    )
    .await?;

    // record written to file and db
    report_rx.assert_not_empty()?;

    let pending = pending_burns::get_all(&pool).await?;
    assert!(!pending.is_empty());

    Ok(())
}

#[sqlx::test]
async fn rejected_sessions_go_to_the_invalid_table_with_a_reason(
    pool: PgPool,
) -> anyhow::Result<()> {
    let harness = common::setup_iceberg().await?;
    let session_writer = harness
        .get_table_writer::<iceberg::IcebergDataTransferSession>(iceberg::session::TABLE_NAME)
        .await?;
    let invalid_session_writer = harness
        .get_table_writer::<iceberg::IcebergInvalidDataTransferSession>(
            iceberg::invalid_session::TABLE_NAME,
        )
        .await?;

    let valid_gateway = PublicKeyBinary::from(vec![1]);
    let invalid_gateway = PublicKeyBinary::from(vec![2]);

    let reports = vec![make_report(&valid_gateway), make_report(&invalid_gateway)];

    // Only `valid_gateway` is seeded into the inventory (and allow-listed as a
    // routing key); the other gateway is unknown and gets rejected.
    common::hotspot_inventory::seed(
        &harness,
        vec![MobileHotspotInventory::known(
            &valid_gateway,
            Utc::now() - Duration::hours(1),
        )],
    )
    .await?;
    let resolver = common::gateway_resolver(&harness).await?;
    let routing_keys: RoutingKeys = [valid_gateway.clone()].into_iter().collect();

    let mut txn = pool.begin().await?;
    let (verified_tx, _verified_rx) = tokio::sync::mpsc::channel(10);
    let verified_sink = FileSinkClient::new(verified_tx, "test");
    let banned_radios = banning::get_banned_radios(&mut txn, Utc::now()).await?;

    handle_data_transfer_session_file(
        &mut txn,
        Some(&session_writer),
        Some(&invalid_session_writer),
        "test_write_id",
        banned_radios,
        &resolver,
        &routing_keys,
        &verified_sink,
        Utc::now(),
        futures::stream::iter(reports),
    )
    .await?;
    txn.commit().await?;

    let trino = harness.trino();

    // The valid session went to the sessions table only.
    let valids = iceberg::session::get_all(trino).await?;
    assert_eq!(valids.len(), 1);

    // The rejected session went to invalid_sessions, tagged with its status.
    let invalids = iceberg::invalid_session::get_all(trino).await?;
    assert_eq!(invalids.len(), 1);
    assert_eq!(invalids[0].reason, "invalid_gateway_key");
    assert_eq!(
        invalids[0].data_transfer_event_pub_key,
        invalid_gateway.to_string()
    );

    Ok(())
}

/// A gateway written to the inventory *after* the resolver built its snapshot is
/// not in that snapshot, so `is_gateway_known` must fall back to the per-pubkey
/// Trino query — which finds it and marks the session valid.
#[sqlx::test]
async fn fallback_resolves_gateway_added_after_snapshot(pool: PgPool) -> anyhow::Result<()> {
    let harness = common::setup_iceberg().await?;
    let session_writer = harness
        .get_table_writer(iceberg::session::TABLE_NAME)
        .await?;

    let gateway = PublicKeyBinary::from(vec![1]);

    // Build the resolver against an empty inventory: `gateway` is NOT in the snapshot.
    let resolver = common::gateway_resolver(&harness).await?;
    let routing_keys: RoutingKeys = [gateway.clone()].into_iter().collect();

    // Add the gateway to the inventory only now, first seen an hour ago.
    common::hotspot_inventory::seed(
        &harness,
        vec![MobileHotspotInventory::known(
            &gateway,
            Utc::now() - Duration::hours(1),
        )],
    )
    .await?;

    let mut txn = pool.begin().await?;
    let (verified_tx, _verified_rx) = tokio::sync::mpsc::channel(10);
    let verified_sink = FileSinkClient::new(verified_tx, "test");
    let banned_radios = banning::get_banned_radios(&mut txn, Utc::now()).await?;
    handle_data_transfer_session_file(
        &mut txn,
        Some(&session_writer),
        None,
        "test_write_id",
        banned_radios,
        &resolver,
        &routing_keys,
        &verified_sink,
        Utc::now(),
        futures::stream::iter(vec![make_report(&gateway)]),
    )
    .await?;
    txn.commit().await?;

    // Valid only if the fallback query found it — the snapshot was empty.
    let pending = pending_burns::get_all(&pool).await?;
    assert_eq!(pending.len(), 1);

    Ok(())
}

/// The fallback filters on `inserted_at` (stable first-seen), not
/// `received_timestamp` (which advances on reassertion). A gateway first seen
/// before a report — but whose latest assertion was ingested after it — is still
/// known as of the report time. Guards against reverting to `received_timestamp`.
#[sqlx::test]
async fn fallback_filters_on_inserted_at_not_received_timestamp(
    pool: PgPool,
) -> anyhow::Result<()> {
    let harness = common::setup_iceberg().await?;
    let session_writer = harness
        .get_table_writer(iceberg::session::TABLE_NAME)
        .await?;

    let gateway = PublicKeyBinary::from(vec![1]);
    let report_time = Utc::now() - Duration::hours(1);

    // Not in the startup snapshot.
    let resolver = common::gateway_resolver(&harness).await?;
    let routing_keys: RoutingKeys = [gateway.clone()].into_iter().collect();

    // First seen before the report, but the latest assertion was ingested after
    // it (as if the gateway reasserted since).
    let mut row = MobileHotspotInventory::known(&gateway, report_time - Duration::hours(1));
    row.received_timestamp = (report_time + Duration::hours(1)).into();
    common::hotspot_inventory::seed(&harness, vec![row]).await?;

    let mut txn = pool.begin().await?;
    let (verified_tx, _verified_rx) = tokio::sync::mpsc::channel(10);
    let verified_sink = FileSinkClient::new(verified_tx, "test");
    let banned_radios = banning::get_banned_radios(&mut txn, report_time).await?;
    handle_data_transfer_session_file(
        &mut txn,
        Some(&session_writer),
        None,
        "test_write_id",
        banned_radios,
        &resolver,
        &routing_keys,
        &verified_sink,
        report_time,
        futures::stream::iter(vec![make_report_at(&gateway, report_time)]),
    )
    .await?;
    txn.commit().await?;

    // Known via `inserted_at` (report_time - 1h ≤ report_time); it would be
    // rejected if the query used `received_timestamp` (report_time + 1h).
    let pending = pending_burns::get_all(&pool).await?;
    assert_eq!(pending.len(), 1);

    Ok(())
}

fn make_report(gateway: &PublicKeyBinary) -> DataTransferSessionIngestReport {
    make_report_at(gateway, Utc::now())
}

fn make_report_at(
    gateway: &PublicKeyBinary,
    received_timestamp: DateTime<Utc>,
) -> DataTransferSessionIngestReport {
    DataTransferSessionIngestReport {
        received_timestamp,
        report: DataTransferSessionReq {
            rewardable_bytes: 1_000,
            pub_key: gateway.clone(),
            signature: vec![],
            carrier_id: CarrierIdV2::Carrier9,
            sampling: false,
            data_transfer_usage: DataTransferEvent {
                pub_key: gateway.clone(),
                upload_bytes: 1_000,
                download_bytes: 1_000,
                radio_access_technology: DataTransferRadioAccessTechnology::Wlan,
                event_id: format!("event-{gateway}"),
                payer: PublicKeyBinary::from(vec![0]),
                timestamp: received_timestamp,
                signature: vec![],
            },
        },
    }
}

/// Seed the given `known_gateways` into the per-test inventory table, build a
/// real [`GatewayResolver`](mobile_packet_verifier::GatewayResolver) backed by
/// the harness Trino, and run the reports through
/// `handle_data_transfer_session_file`.
async fn run_accumulate_sessions(
    pool: &PgPool,
    harness: &IcebergTestHarness,
    reports: Vec<DataTransferSessionIngestReport>,
    known_gateways: Vec<PublicKeyBinary>,
    routing_keys: Vec<PublicKeyBinary>,
    iceberg_writer: Option<iceberg::DataTransferWriter>,
) -> anyhow::Result<MessageReceiver<VerifiedDataTransferIngestReportV1>> {
    // Mark each known gateway as present on-chain, comfortably before the
    // reports' received timestamps so the `received_timestamp <=` check passes.
    let seed_ts = Utc::now() - Duration::hours(1);
    let rows = known_gateways
        .iter()
        .map(|gw| MobileHotspotInventory::known(gw, seed_ts))
        .collect();
    common::hotspot_inventory::seed(harness, rows).await?;

    let resolver = common::gateway_resolver(harness).await?;
    let routing_keys: RoutingKeys = routing_keys.into_iter().collect();

    let mut txn = pool.begin().await?;

    let ts = Utc::now();

    let (verified_sessions_tx, verified_sessions_rx) = tokio::sync::mpsc::channel(10);
    let verified_sessions = FileSinkClient::new(verified_sessions_tx, "test");

    let banned_radios = banning::get_banned_radios(&mut txn, Utc::now()).await?;

    handle_data_transfer_session_file(
        &mut txn,
        iceberg_writer.as_ref(),
        None,
        "test_write_id",
        banned_radios,
        &resolver,
        &routing_keys,
        &verified_sessions,
        ts,
        futures::stream::iter(reports),
    )
    .await?;

    txn.commit().await?;

    Ok(verified_sessions_rx)
}

async fn ban_hotspot(
    pool: &PgPool,
    hotspot_pubkey: PublicKeyBinary,
    ban_type: BanType,
) -> anyhow::Result<()> {
    let mut conn = pool.acquire().await?;
    banning::db::update_hotspot_ban(
        &mut conn,
        VerifiedBanReport {
            verified_timestamp: Utc::now(),
            status: VerifiedBanIngestReportStatus::Valid,
            report: BanReport {
                received_timestamp: Utc::now(),
                report: BanRequest {
                    hotspot_pubkey,
                    timestamp: Utc::now(),
                    ban_pubkey: PublicKeyBinary::from(vec![0]),
                    signature: vec![],
                    ban_action: BanAction::Ban(BanDetails {
                        hotspot_serial: "serial".to_string(),
                        message: "notes".to_string(),
                        reason: BanReason::LocationGaming,
                        ban_type,
                        expiration_timestamp: None,
                    }),
                },
            },
        },
    )
    .await?;

    Ok(())
}
