use chrono::{Duration, Utc};
use file_store::{
    file_sink::{FileSinkClient, MessageReceiver},
    mobile_ban::{
        BanAction, BanDetails, BanReason, BanReport, BanRequest, BanType,
        VerifiedBanIngestReportStatus, VerifiedBanReport,
    },
    mobile_session::{DataTransferEvent, DataTransferSessionIngestReport, DataTransferSessionReq},
};
use helium_crypto::PublicKeyBinary;
use helium_proto::services::poc_mobile::{
    CarrierIdV2, DataTransferRadioAccessTechnology, VerifiedDataTransferIngestReportV1,
};
use mobile_packet_verifier::{
    accumulate::accumulate_sessions, banning, bytes_to_dc, pending_burns,
};
use sqlx::PgPool;

use crate::common::{TestChannelExt, TestMobileConfig};

#[sqlx::test]
#[ignore]
async fn accumulate_no_reports(pool: PgPool) -> anyhow::Result<()> {
    let mut report_rx =
        run_accumulate_sessions(&pool, vec![], TestMobileConfig::all_valid()).await?;

    report_rx.assert_is_empty()?;

    let pending = pending_burns::get_all(&pool).await?;
    assert!(pending.is_empty());

    Ok(())
}

#[sqlx::test]
#[ignore]
async fn accumlate_reports_for_same_key(pool: PgPool) -> anyhow::Result<()> {
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

    let mut report_rx =
        run_accumulate_sessions(&pool, reports, TestMobileConfig::all_valid()).await?;

    report_rx.assert_num_msgs(2)?;

    let pending = pending_burns::get_all(&pool).await?;
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].dc_to_burn(), bytes_to_dc(2_000));

    Ok(())
}

#[sqlx::test]
#[ignore]
async fn accumulate_writes_zero_data_event_as_verified_but_not_for_burning(
    pool: PgPool,
) -> anyhow::Result<()> {
    let reports = vec![DataTransferSessionIngestReport {
        report: DataTransferSessionReq {
            data_transfer_usage: DataTransferEvent {
                pub_key: vec![0].into(),
                upload_bytes: 1,
                download_bytes: 2,
                radio_access_technology: DataTransferRadioAccessTechnology::Wlan,
                event_id: "test".to_string(),
                payer: vec![0].into(),
                timestamp: Utc::now(),
                signature: vec![],
            },
            rewardable_bytes: 0,
            pub_key: vec![0].into(),
            signature: vec![],
            carrier_id: CarrierIdV2::Carrier9,
            sampling: false,
        },
        received_timestamp: Utc::now(),
    }];

    let mut report_rx =
        run_accumulate_sessions(&pool, reports, TestMobileConfig::all_valid()).await?;

    report_rx.assert_not_empty()?;

    let pending = pending_burns::get_all(&pool).await?;
    assert!(pending.is_empty());

    Ok(())
}

#[sqlx::test]
#[ignore]
async fn writes_valid_event_to_db(pool: PgPool) -> anyhow::Result<()> {
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
                radio_access_technology: DataTransferRadioAccessTechnology::Wlan,
                event_id: "test-event-id".to_string(),
                payer: PublicKeyBinary::from(vec![0]),
                timestamp: Utc::now(),
                signature: vec![],
            },
        },
    }];

    let mut report_rx =
        run_accumulate_sessions(&pool, reports, TestMobileConfig::all_valid()).await?;

    report_rx.assert_not_empty()?;

    let pending = pending_burns::get_all(&pool).await?;
    assert_eq!(pending.len(), 1);

    Ok(())
}

#[sqlx::test]
#[ignore]
async fn ignores_cbrs_data_sessions(pool: PgPool) -> anyhow::Result<()> {
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

    let mut report_rx =
        run_accumulate_sessions(&pool, reports, TestMobileConfig::all_valid()).await?;

    // record not written to file or db
    report_rx.assert_is_empty()?;

    let pending = pending_burns::get_all(&pool).await?;
    assert!(pending.is_empty());

    Ok(())
}

#[sqlx::test]
#[ignore]
async fn ignores_invalid_gateway_keys(pool: PgPool) -> anyhow::Result<()> {
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
                radio_access_technology: DataTransferRadioAccessTechnology::Wlan,
                event_id: "test-event-id".to_string(),
                payer: PublicKeyBinary::from(vec![0]),
                timestamp: Utc::now(),
                signature: vec![],
            },
        },
    }];

    let mut report_rx =
        run_accumulate_sessions(&pool, reports, TestMobileConfig::valid_gateways(vec![])).await?;

    // record written to file, but not db
    report_rx.assert_not_empty()?;

    let pending = pending_burns::get_all(&pool).await?;
    assert!(pending.is_empty());

    Ok(())
}

#[sqlx::test]
#[ignore]
async fn ignores_invalid_routing_keys(pool: PgPool) -> anyhow::Result<()> {
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
                radio_access_technology: DataTransferRadioAccessTechnology::Wlan,
                event_id: "test-event-id".to_string(),
                payer: PublicKeyBinary::from(vec![0]),
                timestamp: Utc::now(),
                signature: vec![],
            },
        },
    }];

    let mut report_rx =
        run_accumulate_sessions(&pool, reports, TestMobileConfig::valid_routing_keys(vec![]))
            .await?;

    // record written to file, but not db
    report_rx.assert_not_empty()?;

    let pending = pending_burns::get_all(&pool).await?;
    assert!(pending.is_empty());

    Ok(())
}

#[sqlx::test]
#[ignore]
async fn ignores_ban_type_all_keys(pool: PgPool) -> anyhow::Result<()> {
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
    ban_hotspot(&pool, key, BanType::All).await?;

    let mut report_rx =
        run_accumulate_sessions(&pool, reports, TestMobileConfig::all_valid()).await?;

    // record written to file, but not db
    report_rx.assert_not_empty()?;

    let pending = pending_burns::get_all(&pool).await?;
    assert!(pending.is_empty());

    Ok(())
}

#[sqlx::test]
#[ignore]
async fn ignores_ban_type_data_transfer_keys(pool: PgPool) -> anyhow::Result<()> {
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
    ban_hotspot(&pool, key, BanType::Data).await?;

    let mut report_rx =
        run_accumulate_sessions(&pool, reports, TestMobileConfig::all_valid()).await?;

    // record written to file, but not db
    report_rx.assert_not_empty()?;

    let pending = pending_burns::get_all(&pool).await?;
    assert!(pending.is_empty());

    Ok(())
}

#[sqlx::test]
#[ignore]
async fn allows_ban_type_poc_keys(pool: PgPool) -> anyhow::Result<()> {
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
    ban_hotspot(&pool, key, BanType::Poc).await?;

    let mut report_rx =
        run_accumulate_sessions(&pool, reports, TestMobileConfig::all_valid()).await?;

    // record written to file and db
    report_rx.assert_not_empty()?;

    let pending = pending_burns::get_all(&pool).await?;
    assert!(!pending.is_empty());
    Ok(())
}

#[sqlx::test]
#[ignore]
async fn allows_expired_ban_type_data_transfer_keys(pool: PgPool) -> anyhow::Result<()> {
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

    let mut report_rx =
        run_accumulate_sessions(&pool, reports, TestMobileConfig::all_valid()).await?;

    // record written to file and db
    report_rx.assert_not_empty()?;

    let pending = pending_burns::get_all(&pool).await?;
    assert!(!pending.is_empty());

    Ok(())
}

async fn run_accumulate_sessions(
    pool: &PgPool,
    reports: Vec<DataTransferSessionIngestReport>,
    mobile_config: TestMobileConfig,
) -> anyhow::Result<MessageReceiver<VerifiedDataTransferIngestReportV1>> {
    let mut txn = pool.begin().await?;

    let (verified_sessions_tx, verified_sessions_rx) = tokio::sync::mpsc::channel(10);
    let verified_sessions = FileSinkClient::new(verified_sessions_tx, "test");

    let banned_radios = banning::get_banned_radios(&mut txn, Utc::now()).await?;
    accumulate_sessions(
        &mobile_config,
        banned_radios,
        &mut txn,
        &verified_sessions,
        Utc::now(),
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
