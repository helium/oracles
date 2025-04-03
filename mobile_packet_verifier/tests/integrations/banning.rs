use chrono::{Duration, Utc};
use file_store::mobile_ban::{
    BanAction, BanDetails, BanReason, BanReport, BanRequest, BanType, UnbanDetails,
    VerifiedBanIngestReportStatus, VerifiedBanReport,
};
use helium_crypto::PublicKeyBinary;
use mobile_packet_verifier::banning::{get_banned_radios, handle_verified_ban_report};
use sqlx::PgPool;

#[sqlx::test]
async fn ban_unban(pool: PgPool) -> anyhow::Result<()> {
    let mut conn = pool.acquire().await?;
    let key = PublicKeyBinary::from(vec![1]);

    let ban_report = VerifiedBanReport {
        verified_timestamp: Utc::now(),
        status: VerifiedBanIngestReportStatus::Valid,
        report: BanReport {
            received_timestamp: Utc::now(),
            report: BanRequest {
                hotspot_pubkey: key.clone(),
                timestamp: Utc::now(),
                ban_key: vec![0].into(),
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
                ban_key: vec![0].into(),
                signature: vec![],
                ban_action: BanAction::Unban(UnbanDetails {
                    hotspot_serial: "test-serial".to_string(),
                    message: "test-message".to_string(),
                }),
            },
        },
    };

    // Ban radio
    handle_verified_ban_report(&mut conn, ban_report).await?;
    let banned = get_banned_radios(&mut conn, Utc::now()).await?;
    assert!(banned.contains(&key));

    // Unban radio
    handle_verified_ban_report(&mut conn, unban_report).await?;
    let banned = get_banned_radios(&mut conn, Utc::now()).await?;
    assert!(!banned.contains(&key));

    Ok(())
}

#[sqlx::test]
async fn new_ban_replaces_old_ban(pool: PgPool) -> anyhow::Result<()> {
    let mut conn = pool.acquire().await?;
    let key = PublicKeyBinary::from(vec![1]);

    let mk_ban_report = |ban_type: BanType| VerifiedBanReport {
        verified_timestamp: Utc::now(),
        status: VerifiedBanIngestReportStatus::Valid,
        report: BanReport {
            received_timestamp: Utc::now(),
            report: BanRequest {
                hotspot_pubkey: key.clone(),
                timestamp: Utc::now(),
                ban_key: vec![0].into(),
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

    handle_verified_ban_report(&mut conn, mk_ban_report(BanType::Data)).await?;
    let banned = get_banned_radios(&mut conn, Utc::now()).await?;
    assert!(banned.contains(&key));

    handle_verified_ban_report(&mut conn, mk_ban_report(BanType::Poc)).await?;
    let banned = get_banned_radios(&mut conn, Utc::now()).await?;
    assert!(!banned.contains(&key));

    Ok(())
}

#[sqlx::test]
async fn expired_bans_are_not_used(pool: PgPool) -> anyhow::Result<()> {
    let mut conn = pool.acquire().await?;
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
                ban_key: PublicKeyBinary::from(vec![1]),
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
                ban_key: PublicKeyBinary::from(vec![1]),
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

    handle_verified_ban_report(&mut conn, expired_ban_report).await?;
    handle_verified_ban_report(&mut conn, ban_report).await?;

    let banned = get_banned_radios(&mut conn, Utc::now()).await?;
    assert!(!banned.contains(&expired_hotspot_pubkey));
    assert!(banned.contains(&banned_hotspot_pubkey));

    Ok(())
}

#[sqlx::test]
async fn unverified_requests_are_not_written_to_db(pool: PgPool) -> anyhow::Result<()> {
    let mut conn = pool.acquire().await?;
    let hotspot_pubkey = PublicKeyBinary::from(vec![1]);

    let ban_report = VerifiedBanReport {
        verified_timestamp: Utc::now(),
        status: VerifiedBanIngestReportStatus::InvalidBanKey,
        report: BanReport {
            received_timestamp: Utc::now(),
            report: BanRequest {
                hotspot_pubkey: hotspot_pubkey.clone(),
                timestamp: Utc::now(),
                ban_key: PublicKeyBinary::from(vec![1]),
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

    // Unverified Ban radio
    handle_verified_ban_report(&mut conn, ban_report).await?;
    let banned = get_banned_radios(&mut conn, Utc::now()).await?;
    assert!(!banned.contains(&hotspot_pubkey));

    Ok(())
}

#[sqlx::test]
async fn bans_outside_of_rewardable_period_are_not_used(pool: PgPool) -> anyhow::Result<()> {
    let mut conn = pool.acquire().await?;

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
                ban_key: PublicKeyBinary::from(vec![1]),
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
                ban_key: PublicKeyBinary::from(vec![1]),
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

    handle_verified_ban_report(&mut conn, current_ban_report).await?;
    handle_verified_ban_report(&mut conn, future_ban_report).await?;

    let banned = get_banned_radios(&mut conn, Utc::now()).await?;
    assert!(banned.contains(&current_hotspot_pubkey));
    assert!(!banned.contains(&future_hotspot_pubkey));

    Ok(())
}
