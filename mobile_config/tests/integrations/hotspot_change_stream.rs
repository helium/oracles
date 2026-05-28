use crate::common::{
    gateway_metadata_db::{self, GatewayInsert},
    make_keypair,
};
use chrono::{TimeZone, Utc};
use file_store::{file_info::FileInfo, file_info_poller::FileInfoStream};
use helium_crypto::PublicKeyBinary;
use mobile_config::gateway::{
    db::{Gateway, GatewayType},
    hotspot_change_stream::{HotspotChangeDaemon, MobileHotspotChange, PROCESS_NAME},
    service::info::DeviceType,
};
use sqlx::PgPool;
use task_manager::ChannelConsumer;
use tokio::sync::mpsc;

fn change(
    entity_key: PublicKeyBinary,
    device_type: DeviceType,
    location: Option<u64>,
    azimuth: u32,
    timestamp_secs: i64,
) -> MobileHotspotChange {
    MobileHotspotChange {
        entity_key,
        timestamp: Utc.timestamp_opt(timestamp_secs, 0).unwrap(),
        device_type,
        location,
        azimuth,
    }
}

fn deliver(changes: Vec<MobileHotspotChange>) -> FileInfoStream<MobileHotspotChange> {
    let file_info = FileInfo::from_maybe_dotted_prefix("mobile_hotspot_change_report", Utc::now());
    FileInfoStream::new(PROCESS_NAME.to_string(), file_info, changes)
}

fn deployment_info(antenna: u32, elevation: u32) -> String {
    format!(
        r#"{{"wifiInfoV0":{{"antenna":{},"elevation":{},"azimuth":0,"mechanicalDownTilt":0,"electricalDownTilt":0}}}}"#,
        antenna, elevation
    )
}

fn make_daemon(pool: &PgPool, metadata: &PgPool) -> HotspotChangeDaemon {
    let (_tx, rx) = mpsc::channel(1);
    HotspotChangeDaemon::new(pool.clone(), metadata.clone(), rx)
}

#[sqlx::test]
async fn creates_new_wifi_gateway(pool: PgPool) -> anyhow::Result<()> {
    gateway_metadata_db::create_tables(&pool).await;

    let pk: PublicKeyBinary = make_keypair().public_key().clone().into();
    let asset = "asset-1".to_string();

    gateway_metadata_db::insert_gateway_bulk(
        &pool,
        &[GatewayInsert {
            asset: asset.clone(),
            location: Some(1),
            device_type: "\"wifiIndoor\"".into(),
            key: pk.clone(),
            created_at: Utc::now(),
            refreshed_at: Some(Utc::now()),
            deployment_info: Some(deployment_info(11, 22)),
        }],
        100,
    )
    .await?;

    // Sanity-check the metadata DB lookup before driving the daemon.
    let deployment_lookup =
        mobile_config::gateway::metadata_db::MobileHotspotInfo::fetch_antenna_and_elevation(
            &pool, &pk,
        )
        .await?;
    assert_eq!(deployment_lookup, (Some(11), Some(22)));

    let mut daemon = make_daemon(&pool, &pool);
    daemon
        .handle(deliver(vec![change(
            pk.clone(),
            DeviceType::WifiIndoor,
            Some(0x8528347ffffffff),
            7,
            1_700_000_000,
        )]))
        .await?;

    let row = Gateway::get_by_address(&pool, &pk)
        .await?
        .expect("gateway row created");

    assert_eq!(row.gateway_type, GatewayType::WifiIndoor);
    assert_eq!(row.location, Some(0x8528347ffffffff));
    assert_eq!(row.azimuth, Some(7));
    assert_eq!(row.antenna, Some(11));
    assert_eq!(row.elevation, Some(22));
    assert_eq!(row.location_asserts, Some(1));
    assert_eq!(row.location_changed_at, Some(row.last_changed_at));
    assert_eq!(row.created_at, row.last_changed_at);
    assert_eq!(row.owner, None);
    Ok(())
}

#[sqlx::test]
async fn increments_location_asserts_on_real_move(pool: PgPool) -> anyhow::Result<()> {
    gateway_metadata_db::create_tables(&pool).await;

    let pk: PublicKeyBinary = make_keypair().public_key().clone().into();
    gateway_metadata_db::insert_gateway_bulk(
        &pool,
        &[GatewayInsert {
            asset: "a".into(),
            location: Some(1),
            device_type: "\"wifiIndoor\"".into(),
            key: pk.clone(),
            created_at: Utc::now(),
            refreshed_at: Some(Utc::now()),
            deployment_info: None,
        }],
        100,
    )
    .await?;

    let mut daemon = make_daemon(&pool, &pool);

    daemon
        .handle(deliver(vec![change(
            pk.clone(),
            DeviceType::WifiIndoor,
            Some(0x8528347ffffffff),
            0,
            1_700_000_000,
        )]))
        .await?;

    daemon
        .handle(deliver(vec![change(
            pk.clone(),
            DeviceType::WifiIndoor,
            Some(0x852834affffffff),
            0,
            1_700_000_100,
        )]))
        .await?;

    let row = Gateway::get_by_address(&pool, &pk).await?.unwrap();
    assert_eq!(row.location, Some(0x852834affffffff));
    assert_eq!(row.location_asserts, Some(2));
    assert_eq!(
        row.location_changed_at,
        Some(Utc.timestamp_opt(1_700_000_100, 0).unwrap())
    );
    Ok(())
}

#[sqlx::test]
async fn skips_when_nothing_changed(pool: PgPool) -> anyhow::Result<()> {
    gateway_metadata_db::create_tables(&pool).await;

    let pk: PublicKeyBinary = make_keypair().public_key().clone().into();
    gateway_metadata_db::insert_gateway_bulk(
        &pool,
        &[GatewayInsert {
            asset: "a".into(),
            location: Some(1),
            device_type: "\"wifiIndoor\"".into(),
            key: pk.clone(),
            created_at: Utc::now(),
            refreshed_at: Some(Utc::now()),
            deployment_info: None,
        }],
        100,
    )
    .await?;

    let mut daemon = make_daemon(&pool, &pool);

    let c = change(
        pk.clone(),
        DeviceType::WifiIndoor,
        Some(0x8528347ffffffff),
        0,
        1_700_000_000,
    );
    daemon.handle(deliver(vec![c.clone()])).await?;
    // Same payload, later wall clock — should be a no-op since the narrow
    // hash inputs are identical.
    daemon.handle(deliver(vec![c])).await?;

    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM gateways WHERE address = $1")
        .bind(pk.as_ref())
        .fetch_one(&pool)
        .await?;
    assert_eq!(count, 1);
    Ok(())
}

#[sqlx::test]
async fn cbrs_changes_are_dropped(pool: PgPool) -> anyhow::Result<()> {
    gateway_metadata_db::create_tables(&pool).await;

    let pk: PublicKeyBinary = make_keypair().public_key().clone().into();
    let mut daemon = make_daemon(&pool, &pool);

    daemon
        .handle(deliver(vec![change(
            pk.clone(),
            DeviceType::Cbrs,
            Some(1),
            0,
            1,
        )]))
        .await?;

    assert!(Gateway::get_by_address(&pool, &pk).await?.is_none());
    Ok(())
}

#[sqlx::test]
async fn missing_metadata_db_writes_null_antenna(pool: PgPool) -> anyhow::Result<()> {
    gateway_metadata_db::create_tables(&pool).await;

    let pk: PublicKeyBinary = make_keypair().public_key().clone().into();
    let mut daemon = make_daemon(&pool, &pool);

    daemon
        .handle(deliver(vec![change(
            pk.clone(),
            DeviceType::WifiOutdoor,
            Some(0x8528347ffffffff),
            5,
            1_700_000_000,
        )]))
        .await?;

    let row = Gateway::get_by_address(&pool, &pk).await?.unwrap();
    assert_eq!(row.antenna, None);
    assert_eq!(row.elevation, None);
    assert_eq!(row.azimuth, Some(5));
    Ok(())
}

#[sqlx::test]
async fn carries_owner_forward_on_subsequent_change(pool: PgPool) -> anyhow::Result<()> {
    gateway_metadata_db::create_tables(&pool).await;

    let pk: PublicKeyBinary = make_keypair().public_key().clone().into();
    let owner = "wallet-abc".to_string();
    let owner_changed_at = Utc.timestamp_opt(1_600_000_000, 0).unwrap();

    let seed = Gateway {
        address: pk.clone(),
        gateway_type: GatewayType::WifiIndoor,
        created_at: Utc.timestamp_opt(1_500_000_000, 0).unwrap(),
        inserted_at: Utc::now(),
        last_changed_at: Utc.timestamp_opt(1_600_000_000, 0).unwrap(),
        hash: String::new(),
        antenna: None,
        elevation: None,
        azimuth: Some(0),
        location: Some(0x8528347ffffffff),
        location_changed_at: Some(Utc.timestamp_opt(1_600_000_000, 0).unwrap()),
        location_asserts: Some(1),
        owner: Some(owner.clone()),
        owner_changed_at: Some(owner_changed_at),
    };
    seed.insert(&pool).await?;

    let mut daemon = make_daemon(&pool, &pool);
    daemon
        .handle(deliver(vec![change(
            pk.clone(),
            DeviceType::WifiIndoor,
            Some(0x852834affffffff), // different location to force a write
            0,
            1_700_000_000,
        )]))
        .await?;

    let row = Gateway::get_by_address(&pool, &pk).await?.unwrap();
    assert_eq!(row.owner, Some(owner));
    assert_eq!(row.owner_changed_at, Some(owner_changed_at));
    assert_eq!(row.created_at, seed.created_at);
    Ok(())
}
