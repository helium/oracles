use crate::common::make_keypair;
use chrono::{TimeZone, Utc};
use file_store::{file_info::FileInfo, file_info_poller::FileInfoStream};
use helium_crypto::PublicKeyBinary;
use helium_proto::services::chain_rewardable_entities::EntityOwnerType;
use mobile_config::gateway::{
    db::{Gateway, GatewayType},
    ownership_change_stream::{OwnershipChange, OwnershipChangeDaemon, PROCESS_NAME},
};
use sqlx::PgPool;
use task_manager::ChannelConsumer;
use tokio::sync::mpsc;

fn change(entity_key: PublicKeyBinary, owner: &str, ts_secs: i64) -> OwnershipChange {
    OwnershipChange {
        entity_key,
        timestamp: Utc.timestamp_opt(ts_secs, 0).unwrap(),
        owner: owner.to_string(),
        owner_type: EntityOwnerType::DirectOwner,
    }
}

fn deliver(changes: Vec<OwnershipChange>) -> FileInfoStream<OwnershipChange> {
    let file_info =
        FileInfo::from_maybe_dotted_prefix("entity_ownership_change_report", Utc::now());
    FileInfoStream::new(PROCESS_NAME.to_string(), file_info, changes)
}

fn make_daemon(pool: &PgPool) -> OwnershipChangeDaemon {
    let (_tx, rx) = mpsc::channel(1);
    OwnershipChangeDaemon::new(pool.clone(), rx)
}

async fn seed_gateway(
    pool: &PgPool,
    pk: &PublicKeyBinary,
    owner: Option<String>,
) -> anyhow::Result<Gateway> {
    let gw = Gateway {
        address: pk.clone(),
        gateway_type: GatewayType::WifiIndoor,
        created_at: Utc.timestamp_opt(1_500_000_000, 0).unwrap(),
        inserted_at: Utc::now(),
        last_changed_at: Utc.timestamp_opt(1_600_000_000, 0).unwrap(),
        hash: String::new(),
        antenna: Some(11),
        elevation: Some(22),
        azimuth: Some(33),
        location: Some(0x8528347ffffffff),
        location_changed_at: Some(Utc.timestamp_opt(1_600_000_000, 0).unwrap()),
        location_asserts: Some(1),
        owner,
        owner_changed_at: Some(Utc.timestamp_opt(1_600_000_000, 0).unwrap()),
    };
    gw.insert(pool).await?;
    Ok(gw)
}

#[sqlx::test]
async fn updates_owner_for_known_gateway(pool: PgPool) -> anyhow::Result<()> {
    let pk: PublicKeyBinary = make_keypair().public_key().clone().into();
    seed_gateway(&pool, &pk, Some("old-owner".into())).await?;

    let mut daemon = make_daemon(&pool);
    daemon
        .handle(deliver(vec![change(
            pk.clone(),
            "new-owner",
            1_700_000_000,
        )]))
        .await?;

    let row = Gateway::get_by_address(&pool, &pk).await?.unwrap();
    assert_eq!(row.owner.as_deref(), Some("new-owner"));
    assert_eq!(
        row.owner_changed_at,
        Some(Utc.timestamp_opt(1_700_000_000, 0).unwrap())
    );
    assert_eq!(
        row.last_changed_at,
        Utc.timestamp_opt(1_700_000_000, 0).unwrap()
    );

    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM gateways WHERE address = $1")
        .bind(pk.as_ref())
        .fetch_one(&pool)
        .await?;
    assert_eq!(count, 2, "expected one history row per owner");
    Ok(())
}

#[sqlx::test]
async fn drops_event_for_unknown_entity(pool: PgPool) -> anyhow::Result<()> {
    let pk: PublicKeyBinary = make_keypair().public_key().clone().into();

    let mut daemon = make_daemon(&pool);
    daemon
        .handle(deliver(vec![change(pk.clone(), "stranger", 1_700_000_000)]))
        .await?;

    assert!(Gateway::get_by_address(&pool, &pk).await?.is_none());
    Ok(())
}

#[sqlx::test]
async fn skips_same_owner(pool: PgPool) -> anyhow::Result<()> {
    let pk: PublicKeyBinary = make_keypair().public_key().clone().into();
    seed_gateway(&pool, &pk, Some("steady-owner".into())).await?;

    let mut daemon = make_daemon(&pool);
    daemon
        .handle(deliver(vec![change(
            pk.clone(),
            "steady-owner",
            1_700_000_000,
        )]))
        .await?;

    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM gateways WHERE address = $1")
        .bind(pk.as_ref())
        .fetch_one(&pool)
        .await?;
    assert_eq!(count, 1);
    Ok(())
}

#[sqlx::test]
async fn carries_forward_all_other_fields(pool: PgPool) -> anyhow::Result<()> {
    let pk: PublicKeyBinary = make_keypair().public_key().clone().into();
    let seed = seed_gateway(&pool, &pk, Some("old-owner".into())).await?;

    let mut daemon = make_daemon(&pool);
    daemon
        .handle(deliver(vec![change(
            pk.clone(),
            "new-owner",
            1_700_000_000,
        )]))
        .await?;

    let row = Gateway::get_by_address(&pool, &pk).await?.unwrap();
    assert_eq!(row.gateway_type, seed.gateway_type);
    assert_eq!(row.created_at, seed.created_at);
    assert_eq!(row.antenna, seed.antenna);
    assert_eq!(row.elevation, seed.elevation);
    assert_eq!(row.azimuth, seed.azimuth);
    assert_eq!(row.location, seed.location);
    assert_eq!(row.location_asserts, seed.location_asserts);
    assert_eq!(row.location_changed_at, seed.location_changed_at);
    Ok(())
}
