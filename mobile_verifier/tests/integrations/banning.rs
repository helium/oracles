use chrono::{DateTime, Duration, Utc};
use file_store_oracles::mobile_ban::{
    BanAction, BanDetails, BanReason, BanReport, BanRequest, BanType, UnbanDetails,
};
use helium_crypto::PublicKeyBinary;
use helium_proto::services::mobile_config::NetworkKeyRole;
use mobile_config::{
    client::{authorization_client::AuthorizationVerifier, ClientError},
    EpochInfo,
};
use mobile_verifier::banning::{ingestor::process_ban_report, BannedRadios};
use sqlx::PgPool;

struct AllVerified;

#[async_trait::async_trait]
impl AuthorizationVerifier for AllVerified {
    async fn verify_authorized_key(
        &self,
        _pubkey: &PublicKeyBinary,
        _role: NetworkKeyRole,
    ) -> Result<bool, ClientError> {
        Ok(true)
    }
}

async fn test_get_current_banned_radios(pool: &PgPool) -> anyhow::Result<BannedRadios> {
    BannedRadios::new(pool, Utc::now()).await
}

#[sqlx::test]
async fn extremities_of_banning(pool: PgPool) -> anyhow::Result<()> {
    const EPOCH_LENGTH: i64 = 60 * 60 * 24;
    let epoch = chrono::Utc::now().timestamp() / EPOCH_LENGTH;
    let epoch_info = EpochInfo::from(epoch as u64);

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
    ) -> BanReport {
        BanReport {
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
        }
    }

    fn hours(h: i64) -> Duration {
        Duration::hours(h)
    }
    let start = epoch_info.period.start;
    let end = epoch_info.period.end;

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

    let mut conn = pool.acquire().await?;
    for report in reports {
        process_ban_report(&mut conn, &AllVerified, report).await?;
    }

    let banned = BannedRadios::new(&pool, end).await?;

    assert!(banned.is_poc_banned(&banned_before), "banned before");
    assert!(banned.is_poc_banned(&banned_on_start), "banned on start");
    assert!(banned.is_poc_banned(&banned_within), "banned wthin");
    assert!(!banned.is_poc_banned(&banned_on_end), "banned on end");
    assert!(!banned.is_poc_banned(&banned_after), "banned after");

    assert!(!banned.is_poc_banned(&expired_before), "expired before");
    assert!(!banned.is_poc_banned(&expired_on_start), "expired on start");
    assert!(!banned.is_poc_banned(&expired_within), "expired within");
    assert!(banned.is_poc_banned(&expired_on_end), "expired on end");
    assert!(banned.is_poc_banned(&expired_after), "expired after");

    Ok(())
}

#[sqlx::test]
async fn ban_unban(pool: PgPool) -> anyhow::Result<()> {
    let mut conn = pool.acquire().await?;
    let hotspot_pubkey = PublicKeyBinary::from(vec![1]);

    let ban_report = BanReport {
        received_timestamp: Utc::now(),
        report: BanRequest {
            hotspot_pubkey: hotspot_pubkey.clone(),
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
    };

    let unban_report = BanReport {
        received_timestamp: Utc::now(),
        report: BanRequest {
            hotspot_pubkey: hotspot_pubkey.clone(),
            timestamp: Utc::now(),
            ban_pubkey: PublicKeyBinary::from(vec![1]),
            signature: vec![],
            ban_action: BanAction::Unban(UnbanDetails {
                hotspot_serial: "test-serial".to_string(),
                message: "test-unban".to_string(),
            }),
        },
    };

    // Ban radio
    process_ban_report(&mut conn, &AllVerified, ban_report).await?;
    let banned = test_get_current_banned_radios(&pool).await?;
    assert!(banned.is_poc_banned(&hotspot_pubkey));

    // Unban radio
    process_ban_report(&mut conn, &AllVerified, unban_report).await?;
    let banned = test_get_current_banned_radios(&pool).await?;
    assert!(!banned.is_poc_banned(&hotspot_pubkey));

    Ok(())
}

#[sqlx::test]
async fn past_ban_future_unban(pool: PgPool) -> anyhow::Result<()> {
    let mut conn = pool.acquire().await?;
    let key = PublicKeyBinary::from(vec![1]);

    let yesterday = Utc::now() - Duration::hours(12);
    let today = Utc::now();

    let ban_report = BanReport {
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
    };
    let unban_report = BanReport {
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
    };

    // Ban the radio yesterday, unban today.
    process_ban_report(&mut conn, &AllVerified, ban_report).await?;
    process_ban_report(&mut conn, &AllVerified, unban_report).await?;

    // Yesterday, radio was banned.
    let yesterday_banned = BannedRadios::new(&pool, yesterday + Duration::seconds(1)).await?;
    assert!(yesterday_banned.is_poc_banned(&key));

    // Today, not banned
    let today_banned = BannedRadios::new(&pool, today + Duration::seconds(1)).await?;
    assert!(!today_banned.is_poc_banned(&key));

    Ok(())
}

#[sqlx::test]
async fn past_data_ban_future_poc_ban(pool: PgPool) -> anyhow::Result<()> {
    let mut conn = pool.acquire().await?;
    let key = PublicKeyBinary::from(vec![1]);

    let yesterday = Utc::now() - Duration::hours(12);
    let today = Utc::now() - Duration::seconds(1);
    let now = Utc::now();

    let poc_ban_report = BanReport {
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
                ban_type: BanType::Poc,
                expiration_timestamp: None,
            }),
        },
    };
    let data_ban_report = BanReport {
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
                ban_type: BanType::Data,
                expiration_timestamp: None,
            }),
        },
    };

    // Ban the radio yesterday, unban today.
    process_ban_report(&mut conn, &AllVerified, data_ban_report).await?;
    process_ban_report(&mut conn, &AllVerified, poc_ban_report).await?;

    // Yesterday, radio was banned for today
    let yesterday_banned = BannedRadios::new(&pool, today).await?;
    assert!(yesterday_banned.is_poc_banned(&key));

    // Now, radio is not banned
    let today_banned = BannedRadios::new(&pool, now).await?;
    assert!(!today_banned.is_poc_banned(&key));

    Ok(())
}

#[sqlx::test]
async fn new_ban_replaces_old_ban(pool: PgPool) -> anyhow::Result<()> {
    let mut conn = pool.acquire().await?;
    let hotspot_pubkey = PublicKeyBinary::from(vec![1]);

    let mk_ban_report = |ban_type: BanType| BanReport {
        received_timestamp: Utc::now(),
        report: BanRequest {
            hotspot_pubkey: hotspot_pubkey.clone(),
            timestamp: Utc::now(),
            ban_pubkey: PublicKeyBinary::from(vec![1]),
            signature: vec![],
            ban_action: BanAction::Ban(BanDetails {
                hotspot_serial: "test-serial".to_string(),
                message: "test-ban".to_string(),
                reason: BanReason::LocationGaming,
                ban_type,
                expiration_timestamp: None,
            }),
        },
    };

    process_ban_report(&mut conn, &AllVerified, mk_ban_report(BanType::Poc)).await?;
    let banned = test_get_current_banned_radios(&pool).await?;
    assert!(banned.is_poc_banned(&hotspot_pubkey));

    process_ban_report(&mut conn, &AllVerified, mk_ban_report(BanType::Data)).await?;
    let banned = test_get_current_banned_radios(&pool).await?;
    assert!(!banned.is_poc_banned(&hotspot_pubkey));

    Ok(())
}

#[sqlx::test]
async fn expired_bans_are_not_used(pool: PgPool) -> anyhow::Result<()> {
    let mut conn = pool.acquire().await?;
    let expired_hotspot_pubkey = PublicKeyBinary::from(vec![1]);
    let banned_hotspot_pubkey = PublicKeyBinary::from(vec![2]);

    let expired_ban_report = BanReport {
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
    };

    let ban_report = BanReport {
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
    };

    process_ban_report(&mut conn, &AllVerified, expired_ban_report).await?;
    process_ban_report(&mut conn, &AllVerified, ban_report).await?;

    let banned = test_get_current_banned_radios(&pool).await?;
    assert!(!banned.is_poc_banned(&expired_hotspot_pubkey));
    assert!(banned.is_poc_banned(&banned_hotspot_pubkey));

    Ok(())
}

#[sqlx::test]
async fn unverified_requests_are_not_written_to_db(pool: PgPool) -> anyhow::Result<()> {
    struct NoneVerified;

    #[async_trait::async_trait]
    impl AuthorizationVerifier for NoneVerified {
        async fn verify_authorized_key(
            &self,
            _pubkey: &PublicKeyBinary,
            _role: NetworkKeyRole,
        ) -> Result<bool, ClientError> {
            Ok(false)
        }
    }

    let mut conn = pool.acquire().await?;
    let hotspot_pubkey = PublicKeyBinary::from(vec![1]);

    let ban_report = BanReport {
        received_timestamp: Utc::now(),
        report: BanRequest {
            hotspot_pubkey: hotspot_pubkey.clone(),
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
    };

    // Unverified Ban radio
    process_ban_report(&mut conn, &NoneVerified, ban_report).await?;
    let banned = test_get_current_banned_radios(&pool).await?;
    assert!(!banned.is_poc_banned(&hotspot_pubkey));

    Ok(())
}

#[sqlx::test]
async fn bans_outside_of_rewardable_period_are_not_used(pool: PgPool) -> anyhow::Result<()> {
    let mut conn = pool.acquire().await?;

    let current_hotspot_pubkey = PublicKeyBinary::from(vec![1]);
    let future_hotspot_pubkey = PublicKeyBinary::from(vec![2]);

    let current_timestamp = Utc::now();
    let future_timestamp = current_timestamp + Duration::hours(6);

    let current_ban_report = BanReport {
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
    };

    let future_ban_report = BanReport {
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
    };

    process_ban_report(&mut conn, &AllVerified, current_ban_report).await?;
    process_ban_report(&mut conn, &AllVerified, future_ban_report).await?;

    let banned = test_get_current_banned_radios(&pool).await?;
    assert!(banned.is_poc_banned(&current_hotspot_pubkey));
    assert!(!banned.is_poc_banned(&future_hotspot_pubkey));

    Ok(())
}
