use crate::common::{
    gateway_metadata_db::{self, GatewayInsert},
    make_keypair,
};
use chrono::Utc;
use helium_crypto::PublicKeyBinary;
use mobile_config::gateway::tracker;
use sqlx::PgPool;

fn wifi_deployment_info(antenna: u32, elevation: u32) -> String {
    serde_json::json!({
        "wifiInfoV0": {
            "antenna": antenna,
            "elevation": elevation,
            "azimuth": 0,
            "mechanicalDownTilt": 0,
            "electricalDownTilt": 0
        }
    })
    .to_string()
}

/// Seed `mobile_hotspot_infos` / `key_to_assets` so the metadata bulk-read
/// resolves the given deployment_info for `pk`.
async fn seed_metadata_deployment(
    pool: &PgPool,
    pk: &PublicKeyBinary,
    deployment_info: Option<String>,
) -> anyhow::Result<()> {
    let now = Utc::now();
    gateway_metadata_db::insert_gateway_bulk(
        pool,
        &[GatewayInsert {
            asset: format!("asset-{pk}"),
            location: Some(1),
            device_type: "\"wifiIndoor\"".into(),
            key: pk.clone(),
            created_at: now,
            refreshed_at: Some(now),
            deployment_info,
        }],
        100,
    )
    .await?;
    Ok(())
}

async fn deployment_row(
    pool: &PgPool,
    pk: &PublicKeyBinary,
) -> anyhow::Result<Option<(Option<i32>, Option<i32>)>> {
    Ok(sqlx::query_as::<_, (Option<i32>, Option<i32>)>(
        "SELECT antenna, elevation FROM deployment_info WHERE address = $1",
    )
    .bind(pk.to_string())
    .fetch_optional(pool)
    .await?)
}

#[sqlx::test]
async fn caches_wifi_deployment_info(pool: PgPool) -> anyhow::Result<()> {
    gateway_metadata_db::create_tables(&pool).await;
    let pk: PublicKeyBinary = make_keypair().public_key().clone().into();

    seed_metadata_deployment(&pool, &pk, Some(wifi_deployment_info(11, 22))).await?;

    tracker::execute(&pool, &pool).await?;

    let row = deployment_row(&pool, &pk).await?.expect("row cached");
    assert_eq!(row, (Some(11), Some(22)));
    Ok(())
}

#[sqlx::test]
async fn refreshes_changed_deployment_info(pool: PgPool) -> anyhow::Result<()> {
    gateway_metadata_db::create_tables(&pool).await;
    let pk: PublicKeyBinary = make_keypair().public_key().clone().into();

    seed_metadata_deployment(&pool, &pk, Some(wifi_deployment_info(5, 10))).await?;
    tracker::execute(&pool, &pool).await?;
    assert_eq!(deployment_row(&pool, &pk).await?, Some((Some(5), Some(10))));

    // Metadata changes; a second run overwrites the cache.
    sqlx::query("DELETE FROM mobile_hotspot_infos")
        .execute(&pool)
        .await?;
    sqlx::query("DELETE FROM key_to_assets")
        .execute(&pool)
        .await?;
    seed_metadata_deployment(&pool, &pk, Some(wifi_deployment_info(7, 12))).await?;
    tracker::execute(&pool, &pool).await?;

    assert_eq!(deployment_row(&pool, &pk).await?, Some((Some(7), Some(12))));
    Ok(())
}

#[sqlx::test]
async fn skips_missing_deployment_info(pool: PgPool) -> anyhow::Result<()> {
    gateway_metadata_db::create_tables(&pool).await;
    let pk: PublicKeyBinary = make_keypair().public_key().clone().into();

    // Metadata row exists but carries no deployment_info -> nothing cached.
    seed_metadata_deployment(&pool, &pk, None).await?;

    tracker::execute(&pool, &pool).await?;

    assert!(deployment_row(&pool, &pk).await?.is_none());
    Ok(())
}
