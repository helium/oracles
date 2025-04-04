use chrono::{Duration, Utc};
use file_store::mobile_ban::{
    BanAction, BanDetails, BanReason, BanReport, BanRequest, BanType, UnbanDetails,
};
use helium_crypto::PublicKeyBinary;
use helium_proto::services::mobile_config::NetworkKeyRole;
use mobile_config::client::{authorization_client::AuthorizationVerifier, ClientError};
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
async fn ban_unban(pool: PgPool) -> anyhow::Result<()> {
    let mut conn = pool.acquire().await?;
    let hotspot_pubkey = PublicKeyBinary::from(vec![1]);

    let ban_report = BanReport {
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
    };

    let unban_report = BanReport {
        received_timestamp: Utc::now(),
        report: BanRequest {
            hotspot_pubkey: hotspot_pubkey.clone(),
            timestamp: Utc::now(),
            ban_key: PublicKeyBinary::from(vec![1]),
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
async fn new_ban_replaces_old_ban(pool: PgPool) -> anyhow::Result<()> {
    let mut conn = pool.acquire().await?;
    let hotspot_pubkey = PublicKeyBinary::from(vec![1]);

    let mk_ban_report = |ban_type: BanType| BanReport {
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
    };

    let ban_report = BanReport {
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
    };

    let future_ban_report = BanReport {
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
    };

    process_ban_report(&mut conn, &AllVerified, current_ban_report).await?;
    process_ban_report(&mut conn, &AllVerified, future_ban_report).await?;

    let banned = test_get_current_banned_radios(&pool).await?;
    assert!(banned.is_poc_banned(&current_hotspot_pubkey));
    assert!(!banned.is_poc_banned(&future_hotspot_pubkey));

    Ok(())
}
