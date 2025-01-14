use bs58;
use chrono::{DateTime, Duration, Utc};
use helium_crypto::PublicKeyBinary;
use helium_crypto::Sign;
use helium_crypto::{KeyTag, Keypair, PublicKey};
use helium_proto::Message;
use mobile_config::{
    gateway_service::GatewayService,
    key_cache::{CacheKeys, KeyCache},
    mobile_radio_tracker::{MobileRadioTracker, TrackedRadiosMap},
    KeyRole,
};
use sqlx::PgPool;
use std::sync::Arc;
use tokio::{net::TcpListener, sync::RwLock};
use tonic::transport;

use helium_proto::services::mobile_config::{self as proto};

pub async fn spawn_gateway_service(
    pool: PgPool,
    admin_pub_key: PublicKey,
) -> (
    String,
    tokio::task::JoinHandle<std::result::Result<(), helium_proto::services::Error>>,
    MobileRadioTracker,
) {
    let server_key = make_keypair();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    // Start the gateway server
    let keys = CacheKeys::from_iter([(admin_pub_key.to_owned(), KeyRole::Administrator)]);

    let tracked_radios_cache: Arc<RwLock<TrackedRadiosMap>> =
        Arc::new(RwLock::new(TrackedRadiosMap::new()));

    let mobile_tracker = MobileRadioTracker::new(
        pool.clone(),
        pool.clone(),
        humantime::parse_duration("1 hour").unwrap(),
        tracked_radios_cache.clone(),
    );
    mobile_tracker.track_changes().await.unwrap();

    let (_key_cache_tx, key_cache) = KeyCache::new(keys);

    let gws = GatewayService::new(key_cache, pool.clone(), server_key, tracked_radios_cache);
    let handle = tokio::spawn(
        transport::Server::builder()
            .add_service(proto::GatewayServer::new(gws))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
    );

    (format!("http://{addr}"), handle, mobile_tracker)
}

pub async fn add_mobile_tracker_record(
    pool: &PgPool,
    key: PublicKeyBinary,
    last_changed_at: DateTime<Utc>,
) {
    let b58 = bs58::decode(key.to_string()).into_vec().unwrap();

    sqlx::query(
        r#"
            INSERT INTO
"mobile_radio_tracker" ("entity_key", "hash", "last_changed_at", "last_checked_at")
            VALUES
($1, $2, $3, $4);
    "#,
    )
    .bind(b58)
    .bind("hash")
    .bind(last_changed_at)
    .bind(last_changed_at + Duration::hours(1))
    .execute(pool)
    .await
    .unwrap();
}

#[allow(clippy::too_many_arguments)]
pub async fn add_db_record(
    pool: &PgPool,
    asset: &str,
    location: i64,
    device_type: &str,
    key: PublicKeyBinary,
    created_at: DateTime<Utc>,
    refreshed_at: Option<DateTime<Utc>>,
    deployment_info: Option<&str>,
) {
    add_mobile_hotspot_infos(
        pool,
        asset,
        location,
        device_type,
        created_at,
        refreshed_at,
        deployment_info,
    )
    .await;
    add_asset_key(pool, asset, key).await;
}

pub async fn add_mobile_hotspot_infos(
    pool: &PgPool,
    asset: &str,
    location: i64,
    device_type: &str,
    created_at: DateTime<Utc>,
    refreshed_at: Option<DateTime<Utc>>,
    deployment_info: Option<&str>,
) {
    sqlx::query(
        r#"
            INSERT INTO
"mobile_hotspot_infos" ("asset", "location", "device_type", "created_at", "refreshed_at", "deployment_info")
            VALUES
($1, $2, $3::jsonb, $4, $5, $6::jsonb);
    "#,
    )
    .bind(asset)
    .bind(location)
    .bind(device_type)
    .bind(created_at)
    .bind(refreshed_at)
    .bind(deployment_info)
    .execute(pool)
    .await
    .unwrap();
}

pub async fn add_asset_key(pool: &PgPool, asset: &str, key: PublicKeyBinary) {
    let b58 = bs58::decode(key.to_string()).into_vec().unwrap();
    sqlx::query(
        r#"
    INSERT INTO
    "key_to_assets" ("asset", "entity_key")
    VALUES ($1, $2);
    "#,
    )
    .bind(asset)
    .bind(b58)
    .execute(pool)
    .await
    .unwrap();
}

pub async fn create_db_tables(pool: &PgPool) {
    sqlx::query(
        r#"
        CREATE TABLE mobile_hotspot_infos (
        asset character varying(255) NULL,
        location numeric NULL,
        device_type jsonb NOT NULL,
        created_at timestamptz NOT NULL DEFAULT NOW(),
        refreshed_at timestamptz,
        deployment_info jsonb,
        is_full_hotspot bool NULL,
        num_location_asserts integer NULL,
        is_active bool NULL,
        dc_onboarding_fee_paid numeric NULL
    );"#,
    )
    .execute(pool)
    .await
    .unwrap();

    sqlx::query(
        r#"
        CREATE TABLE key_to_assets (
            asset character varying(255) NULL,
            entity_key bytea NULL
        );"#,
    )
    .execute(pool)
    .await
    .unwrap();
}

pub fn make_keypair() -> Keypair {
    Keypair::generate(KeyTag::default(), &mut rand::rngs::OsRng)
}

pub fn make_signed_info_request(address: &PublicKey, signer: &Keypair) -> proto::GatewayInfoReqV1 {
    let mut req = proto::GatewayInfoReqV1 {
        address: address.to_vec(),
        signer: signer.public_key().to_vec(),
        signature: vec![],
    };
    req.signature = signer.sign(&req.encode_to_vec()).unwrap();
    req
}
