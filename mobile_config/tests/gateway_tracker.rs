mod common;

use crate::common::{
    gateway_metadata_db::{create_tables, insert_gateway},
    make_keypair,
};
use chrono::Utc;
use custom_tracing::Settings;
use mobile_config::gateway::{
    db::{Gateway, GatewayType},
    tracker,
};
use sqlx::PgPool;

#[sqlx::test]
async fn execute_test(pool: PgPool) -> anyhow::Result<()> {
    custom_tracing::init("mobile_config=debug,info".to_string(), Settings::default()).await?;

    let pubkey1 = make_keypair().public_key().clone();
    let hex1 = 631711281837647359_i64;
    let now = Utc::now();

    create_tables(&pool).await;
    insert_gateway(
        &pool,
        "asset1",
        Some(hex1),
        "\"wifiIndoor\"",
        pubkey1.clone().into(),
        now,
        Some(now),
        None,
    )
    .await;
    tracker::execute(&pool, &pool).await?;

    let gateway1 = Gateway::get_by_address(&pool, &pubkey1.clone().into())
        .await?
        .expect("asset1 gateway not found");

    assert_eq!(gateway1.address, pubkey1.clone().into());
    assert_eq!(gateway1.gateway_type, GatewayType::WifiIndoor);
    assert_eq!(gateway1.created_at, now);
    assert_eq!(gateway1.refreshed_at, now);
    assert_eq!(gateway1.antenna, None);
    assert_eq!(gateway1.elevation, None);
    assert_eq!(gateway1.azimuth, None);
    assert_eq!(gateway1.location, Some(hex1 as u64));
    assert_eq!(gateway1.location_changed_at, None);
    assert_eq!(gateway1.location_asserts, Some(1));

    Ok(())
}
