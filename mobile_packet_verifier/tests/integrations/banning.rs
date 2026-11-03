use chrono::{DateTime, Duration, Utc};
use file_store_oracles::mobile_ban::{
    BanAction, BanDetails, BanReason, BanReport, BanRequest, BanType, UnbanDetails,
    VerifiedBanIngestReportStatus, VerifiedBanReport,
};
use helium_crypto::PublicKeyBinary;

use crate::common;

#[tokio::test]
async fn extremities_of_banning() -> anyhow::Result<()> {
    let harness = common::setup_iceberg().await?;
    let bans = common::bans::resolver(&harness).await?;
    const EPOCH_LENGTH: i64 = 60 * 60 * 24;
    let epoch = chrono::Utc::now().timestamp() / EPOCH_LENGTH;

    let banned_before = PublicKeyBinary::from(vec![1]); // banned
    let banned_on_start = PublicKeyBinary::from(vec![2]); // banned
    let banned_within = PublicKeyBinary::from(vec![3]); // banned
    let banned_on_end = PublicKeyBinary::from(vec![4]); // not banned
    let banned_after = PublicKeyBinary::from(vec![5]); // not banned

    let expired_before = PublicKeyBinary::from(vec![6]); // not banned
    let expired_on_start = PublicKeyBinary::from(vec![7]); // not banned
    let expired_within = PublicKeyBinary::from(vec![8]); // not banned
    let expired_on_end = PublicKeyBinary::from(vec![9]); // banned
    let expired_after = PublicKeyBinary::from(vec![10]); // banned

    fn mk_ban_report(
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

    fn hours(h: i64) -> Duration {
        Duration::hours(h)
    }
    // Inlined from the former `mobile_config::EpochInfo`: epoch N spans day N.
    let start = DateTime::<Utc>::UNIX_EPOCH + Duration::days(epoch);
    let end = start + Duration::days(1);

    #[rustfmt::skip]
    let reports = vec![
        // Bans
        mk_ban_report(start - hours(2), &banned_before,   None),
        mk_ban_report(start           , &banned_on_start, None),
        mk_ban_report(start + hours(2), &banned_within,   None),
        mk_ban_report(end             , &banned_on_end,   None),
        mk_ban_report(end   + hours(2), &banned_after,    None),
        // Expirations (always start within epoch)
        mk_ban_report(start + hours(2), &expired_before,    Some(end - hours(1))),
        mk_ban_report(start + hours(2), &expired_on_start,  Some(start)),
        mk_ban_report(start + hours(2), &expired_within,    Some(end - hours(2))),
        mk_ban_report(start + hours(2), &expired_on_end,    Some(end)),
        mk_ban_report(start + hours(2), &expired_after,     Some(end + hours(2))),
    ];

    common::bans::record(&harness, reports).await?;

    let banned = bans.banned_radios(end).await?;

    assert!(banned.contains(&banned_before), "banned before");
    assert!(banned.contains(&banned_on_start), "banned on start");
    assert!(banned.contains(&banned_within), "banned within");
    assert!(!banned.contains(&banned_on_end), "banned on end");
    assert!(!banned.contains(&banned_after), "banned after");

    assert!(!banned.contains(&expired_before), "expired before");
    assert!(!banned.contains(&expired_on_start), "expired on start");
    assert!(!banned.contains(&expired_within), "expired within");
    assert!(banned.contains(&expired_on_end), "expired on end");
    assert!(banned.contains(&expired_after), "expired after");

    Ok(())
}

#[tokio::test]
async fn ban_unban() -> anyhow::Result<()> {
    let harness = common::setup_iceberg().await?;
    let bans = common::bans::resolver(&harness).await?;
    let key = PublicKeyBinary::from(vec![1]);

    let ban_report = VerifiedBanReport {
        verified_timestamp: Utc::now(),
        status: VerifiedBanIngestReportStatus::Valid,
        report: BanReport {
            received_timestamp: Utc::now(),
            report: BanRequest {
                hotspot_pubkey: key.clone(),
                timestamp: Utc::now(),
                ban_pubkey: vec![0].into(),
                signature: vec![],
                ban_action: BanAction::Ban(BanDetails {
                    hotspot_serial: "test-serial".to_string(),
                    message: "test-message".to_string(),
                    reason: BanReason::LocationGaming,
                    ban_type: BanType::All,
                    expiration_timestamp: None,
                }),
            },
        },
    };
    let unban_report = VerifiedBanReport {
        verified_timestamp: Utc::now(),
        status: VerifiedBanIngestReportStatus::Valid,
        report: BanReport {
            received_timestamp: Utc::now(),
            report: BanRequest {
                hotspot_pubkey: key.clone(),
                timestamp: Utc::now(),
                ban_pubkey: vec![0].into(),
                signature: vec![],
                ban_action: BanAction::Unban(UnbanDetails {
                    hotspot_serial: "test-serial".to_string(),
                    message: "test-message".to_string(),
                }),
            },
        },
    };

    // Ban radio
    common::bans::record(&harness, [ban_report]).await?;
    let banned = bans.banned_radios(Utc::now()).await?;
    assert!(banned.contains(&key));

    // Unban radio
    common::bans::record(&harness, [unban_report]).await?;
    let banned = bans.banned_radios(Utc::now()).await?;
    assert!(!banned.contains(&key));

    Ok(())
}

#[tokio::test]
async fn past_ban_future_unban() -> anyhow::Result<()> {
    let harness = common::setup_iceberg().await?;
    let bans = common::bans::resolver(&harness).await?;
    let key = PublicKeyBinary::from(vec![1]);

    let yesterday = Utc::now() - Duration::hours(12);
    let today = Utc::now() - Duration::seconds(1);
    let now = Utc::now();

    let ban_report = VerifiedBanReport {
        verified_timestamp: yesterday,
        status: VerifiedBanIngestReportStatus::Valid,
        report: BanReport {
            received_timestamp: yesterday,
            report: BanRequest {
                hotspot_pubkey: key.clone(),
                timestamp: yesterday,
                ban_pubkey: vec![0].into(),
                signature: vec![],
                ban_action: BanAction::Ban(BanDetails {
                    hotspot_serial: "test-serial".to_string(),
                    message: "test-message".to_string(),
                    reason: BanReason::LocationGaming,
                    ban_type: BanType::All,
                    expiration_timestamp: None,
                }),
            },
        },
    };
    let unban_report = VerifiedBanReport {
        verified_timestamp: today,
        status: VerifiedBanIngestReportStatus::Valid,
        report: BanReport {
            received_timestamp: today,
            report: BanRequest {
                hotspot_pubkey: key.clone(),
                timestamp: today,
                ban_pubkey: vec![0].into(),
                signature: vec![],
                ban_action: BanAction::Unban(UnbanDetails {
                    hotspot_serial: "test-serial".to_string(),
                    message: "test-message".to_string(),
                }),
            },
        },
    };

    // Ban the radio yesterday, unban today.
    common::bans::record(&harness, [ban_report]).await?;
    common::bans::record(&harness, [unban_report]).await?;

    // Yesterday, radio was banned for today.
    let yesterday_banned = bans.banned_radios(today).await?;
    assert!(yesterday_banned.contains(&key));

    // Now, not banned
    let today_banned = bans.banned_radios(now).await?;
    assert!(!today_banned.contains(&key));

    Ok(())
}

#[tokio::test]
async fn past_poc_ban_future_data_ban() -> anyhow::Result<()> {
    let harness = common::setup_iceberg().await?;
    let bans = common::bans::resolver(&harness).await?;
    let key = PublicKeyBinary::from(vec![1]);

    let yesterday = Utc::now() - Duration::hours(12);
    let today = Utc::now() - Duration::seconds(1);
    let now = Utc::now();

    let data_ban_report = VerifiedBanReport {
        verified_timestamp: yesterday,
        status: VerifiedBanIngestReportStatus::Valid,
        report: BanReport {
            received_timestamp: yesterday,
            report: BanRequest {
                hotspot_pubkey: key.clone(),
                timestamp: yesterday,
                ban_pubkey: vec![0].into(),
                signature: vec![],
                ban_action: BanAction::Ban(BanDetails {
                    hotspot_serial: "test-serial".to_string(),
                    message: "test-message".to_string(),
                    reason: BanReason::LocationGaming,
                    ban_type: BanType::Data,
                    expiration_timestamp: None,
                }),
            },
        },
    };
    let poc_ban_report = VerifiedBanReport {
        verified_timestamp: today,
        status: VerifiedBanIngestReportStatus::Valid,
        report: BanReport {
            received_timestamp: today,
            report: BanRequest {
                hotspot_pubkey: key.clone(),
                timestamp: today,
                ban_pubkey: vec![0].into(),
                signature: vec![],
                ban_action: BanAction::Ban(BanDetails {
                    hotspot_serial: "test-serial".to_string(),
                    message: "test-message".to_string(),
                    reason: BanReason::LocationGaming,
                    ban_type: BanType::Poc,
                    expiration_timestamp: None,
                }),
            },
        },
    };

    // Ban the radio yesterday, unban today.
    common::bans::record(&harness, [data_ban_report]).await?;
    common::bans::record(&harness, [poc_ban_report]).await?;

    // Yesterday, radio was banned for today.
    let yesterday_banned = bans.banned_radios(today).await?;
    assert!(yesterday_banned.contains(&key));

    // Now, not banned
    let today_banned = bans.banned_radios(now).await?;
    assert!(!today_banned.contains(&key));

    Ok(())
}

#[tokio::test]
async fn new_ban_replaces_old_ban() -> anyhow::Result<()> {
    let harness = common::setup_iceberg().await?;
    let bans = common::bans::resolver(&harness).await?;
    let key = PublicKeyBinary::from(vec![1]);

    let mk_ban_report = |ban_type: BanType| VerifiedBanReport {
        verified_timestamp: Utc::now(),
        status: VerifiedBanIngestReportStatus::Valid,
        report: BanReport {
            received_timestamp: Utc::now(),
            report: BanRequest {
                hotspot_pubkey: key.clone(),
                timestamp: Utc::now(),
                ban_pubkey: vec![0].into(),
                signature: vec![],
                ban_action: BanAction::Ban(BanDetails {
                    hotspot_serial: "test-serial".to_string(),
                    message: "test-message".to_string(),
                    reason: BanReason::LocationGaming,
                    ban_type,
                    expiration_timestamp: None,
                }),
            },
        },
    };

    common::bans::record(&harness, [mk_ban_report(BanType::Data)]).await?;
    let banned = bans.banned_radios(Utc::now()).await?;
    assert!(banned.contains(&key));

    common::bans::record(&harness, [mk_ban_report(BanType::Poc)]).await?;
    let banned = bans.banned_radios(Utc::now()).await?;
    assert!(!banned.contains(&key));

    Ok(())
}

#[tokio::test]
async fn expired_bans_are_not_used() -> anyhow::Result<()> {
    let harness = common::setup_iceberg().await?;
    let bans = common::bans::resolver(&harness).await?;
    let expired_hotspot_pubkey = PublicKeyBinary::from(vec![1]);
    let banned_hotspot_pubkey = PublicKeyBinary::from(vec![2]);

    let expired_ban_report = VerifiedBanReport {
        verified_timestamp: Utc::now(),
        status: VerifiedBanIngestReportStatus::Valid,
        report: BanReport {
            received_timestamp: Utc::now(),
            report: BanRequest {
                hotspot_pubkey: expired_hotspot_pubkey.clone(),
                timestamp: Utc::now() - chrono::Duration::hours(6),
                ban_pubkey: PublicKeyBinary::from(vec![1]),
                signature: vec![],
                ban_action: BanAction::Ban(BanDetails {
                    hotspot_serial: "test-serial".to_string(),
                    message: "test-ban".to_string(),
                    reason: BanReason::LocationGaming,
                    ban_type: BanType::All,
                    expiration_timestamp: Some(Utc::now() - chrono::Duration::hours(5)),
                }),
            },
        },
    };

    let ban_report = VerifiedBanReport {
        verified_timestamp: Utc::now(),
        status: VerifiedBanIngestReportStatus::Valid,
        report: BanReport {
            received_timestamp: Utc::now(),
            report: BanRequest {
                hotspot_pubkey: banned_hotspot_pubkey.clone(),
                timestamp: Utc::now(),
                ban_pubkey: PublicKeyBinary::from(vec![1]),
                signature: vec![],
                ban_action: BanAction::Ban(BanDetails {
                    hotspot_serial: "test-serial".to_string(),
                    message: "test-ban".to_string(),
                    reason: BanReason::LocationGaming,
                    ban_type: BanType::All,
                    expiration_timestamp: None,
                }),
            },
        },
    };

    common::bans::record(&harness, [expired_ban_report]).await?;
    common::bans::record(&harness, [ban_report]).await?;

    let banned = bans.banned_radios(Utc::now()).await?;
    assert!(!banned.contains(&expired_hotspot_pubkey));
    assert!(banned.contains(&banned_hotspot_pubkey));

    Ok(())
}

#[tokio::test]
async fn bans_outside_of_rewardable_period_are_not_used() -> anyhow::Result<()> {
    let harness = common::setup_iceberg().await?;
    let bans = common::bans::resolver(&harness).await?;

    let current_hotspot_pubkey = PublicKeyBinary::from(vec![1]);
    let future_hotspot_pubkey = PublicKeyBinary::from(vec![2]);

    let current_timestamp = Utc::now();
    let future_timestamp = current_timestamp + Duration::hours(6);

    let current_ban_report = VerifiedBanReport {
        verified_timestamp: current_timestamp,
        status: VerifiedBanIngestReportStatus::Valid,
        report: BanReport {
            received_timestamp: current_timestamp,
            report: BanRequest {
                hotspot_pubkey: current_hotspot_pubkey.clone(),
                timestamp: current_timestamp - chrono::Duration::hours(6),
                ban_pubkey: PublicKeyBinary::from(vec![1]),
                signature: vec![],
                ban_action: BanAction::Ban(BanDetails {
                    hotspot_serial: "test-serial".to_string(),
                    message: "test-ban".to_string(),
                    reason: BanReason::LocationGaming,
                    ban_type: BanType::All,
                    expiration_timestamp: None,
                }),
            },
        },
    };

    let future_ban_report = VerifiedBanReport {
        verified_timestamp: future_timestamp,
        status: VerifiedBanIngestReportStatus::Valid,
        report: BanReport {
            received_timestamp: future_timestamp,
            report: BanRequest {
                hotspot_pubkey: future_hotspot_pubkey.clone(),
                timestamp: future_timestamp,
                ban_pubkey: PublicKeyBinary::from(vec![1]),
                signature: vec![],
                ban_action: BanAction::Ban(BanDetails {
                    hotspot_serial: "test-serial".to_string(),
                    message: "test-ban".to_string(),
                    reason: BanReason::LocationGaming,
                    ban_type: BanType::All,
                    expiration_timestamp: None,
                }),
            },
        },
    };

    common::bans::record(&harness, [current_ban_report]).await?;
    common::bans::record(&harness, [future_ban_report]).await?;

    let banned = bans.banned_radios(Utc::now()).await?;
    assert!(banned.contains(&current_hotspot_pubkey));
    assert!(!banned.contains(&future_hotspot_pubkey));

    Ok(())
}

#[tokio::test]
async fn ban_after_unban_applies() -> anyhow::Result<()> {
    let harness = common::setup_iceberg().await?;
    let bans = common::bans::resolver(&harness).await?;
    let key = PublicKeyBinary::from(vec![1]);

    let mk_report = |received_timestamp: DateTime<Utc>, ban_action: BanAction| VerifiedBanReport {
        verified_timestamp: received_timestamp,
        status: VerifiedBanIngestReportStatus::Valid,
        report: BanReport {
            received_timestamp,
            report: BanRequest {
                hotspot_pubkey: key.clone(),
                timestamp: received_timestamp,
                ban_pubkey: vec![0].into(),
                signature: vec![],
                ban_action,
            },
        },
    };
    let ban = || {
        BanAction::Ban(BanDetails {
            hotspot_serial: "test-serial".to_string(),
            message: "test-message".to_string(),
            reason: BanReason::LocationGaming,
            ban_type: BanType::All,
            expiration_timestamp: None,
        })
    };
    let unban = BanAction::Unban(UnbanDetails {
        hotspot_serial: "test-serial".to_string(),
        message: "test-message".to_string(),
    });

    let now = Utc::now();
    let first_ban = now - Duration::hours(3);
    let unbanned = now - Duration::hours(2);
    let second_ban = now - Duration::hours(1);

    common::bans::record(
        &harness,
        [
            mk_report(first_ban, ban()),
            mk_report(unbanned, unban),
            mk_report(second_ban, ban()),
        ],
    )
    .await?;

    // Banned by the first ban, lifted by the unban, banned again by the second.
    assert!(bans.banned_radios(unbanned).await?.contains(&key));
    assert!(!bans.banned_radios(second_ban).await?.contains(&key));
    assert!(bans.banned_radios(now).await?.contains(&key));

    Ok(())
}
