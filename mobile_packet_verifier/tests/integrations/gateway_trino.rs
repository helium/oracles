use chrono::{Duration, Utc};
use helium_crypto::PublicKeyBinary;

use crate::common::{self, hotspot_inventory::MobileHotspotInventory};

#[tokio::test]
async fn snapshot_respects_gateway_inserted_at() -> anyhow::Result<()> {
    let harness = common::setup_iceberg().await?;
    let gateway = PublicKeyBinary::from(vec![1]);
    let inserted_at = Utc::now() - Duration::hours(1);

    common::hotspot_inventory::seed(
        &harness,
        vec![MobileHotspotInventory::known(&gateway, inserted_at)],
    )
    .await?;
    let resolver = common::gateway_resolver(&harness).await?;

    assert!(
        !resolver
            .is_gateway_known(&gateway, &(inserted_at - Duration::seconds(1)))
            .await
    );
    assert!(resolver.is_gateway_known(&gateway, &inserted_at).await);

    Ok(())
}

#[tokio::test]
async fn fallback_cache_respects_each_query_timestamp() -> anyhow::Result<()> {
    let harness = common::setup_iceberg().await?;
    let old_then_new = PublicKeyBinary::from(vec![1]);
    let new_then_old = PublicKeyBinary::from(vec![2]);
    let inserted_at = Utc::now() - Duration::hours(1);
    let before = inserted_at - Duration::seconds(1);

    let resolver = common::gateway_resolver(&harness).await?;
    common::hotspot_inventory::seed(
        &harness,
        vec![
            MobileHotspotInventory::known(&old_then_new, inserted_at),
            MobileHotspotInventory::known(&new_then_old, inserted_at),
        ],
    )
    .await?;

    assert!(!resolver.is_gateway_known(&old_then_new, &before).await);
    assert!(resolver.is_gateway_known(&old_then_new, &inserted_at).await);

    assert!(resolver.is_gateway_known(&new_then_old, &inserted_at).await);
    assert!(!resolver.is_gateway_known(&new_then_old, &before).await);

    Ok(())
}
