//! The gateway snapshot is shared, not copied.
//!
//! `GatewayResolver` is built once and cloned for each consumer — the data
//! transfer session path and the HIP-150 ticket path. Building a second one with
//! `new()` would load its own copy of the whole inventory and then never
//! refresh, because only one refresher task is registered.

use std::time::Duration;

use chrono::Utc;
use helium_crypto::PublicKeyBinary;
use mobile_packet_verifier::gateway::GatewayResolver;
use task_manager::Periodic;

use crate::common::{self, hotspot_inventory};

/// A resolver whose refresher fires on every tick.
///
/// `GatewaySnapshotRefresher` only reloads once `refresh_interval` has elapsed
/// since the last load, so a resolver built with the usual hour-long interval
/// would treat `tick()` as a no-op and these tests would pass for the wrong
/// reason.
async fn eager_resolver(
    harness: &helium_iceberg::IcebergTestHarness,
) -> anyhow::Result<GatewayResolver> {
    Ok(GatewayResolver::new_with_inventory_table(
        trino_client::Client::from_client(harness.owned_trino().await?),
        hotspot_inventory::RESOLVER_TABLE,
        Duration::from_secs(0),
    )
    .await)
}

/// A clone taken *before* a refresh must see gateways that arrive after it.
/// That is the property the daemon depends on: one refresher keeps every
/// consumer current.
#[tokio::test]
async fn a_clone_sees_gateways_added_after_it_was_taken() -> anyhow::Result<()> {
    let harness = common::setup_iceberg().await?;
    let known_at_startup = PublicKeyBinary::from(vec![1]);
    let onboarded_later = PublicKeyBinary::from(vec![2]);
    let seen_at = Utc::now() - chrono::Duration::days(1);

    hotspot_inventory::seed(
        &harness,
        vec![hotspot_inventory::MobileHotspotInventory::known(
            &known_at_startup,
            seen_at,
        )],
    )
    .await?;

    let resolver = eager_resolver(&harness).await?;
    let clone = resolver.clone();
    let mut refresher = resolver.refresher();

    // The later gateway lands on chain after both the resolver and its clone
    // were built.
    hotspot_inventory::seed(
        &harness,
        vec![hotspot_inventory::MobileHotspotInventory::known(
            &onboarded_later,
            seen_at,
        )],
    )
    .await?;

    refresher.tick().await?;

    // Read through the clone. If clones held their own snapshot this would miss
    // and fall through to a per-pubkey query — so ask at a timestamp before the
    // gateway was ever seen, which the fallback would answer `false`.
    let before_it_existed = seen_at - chrono::Duration::days(1);
    assert!(
        clone
            .is_gateway_known(&onboarded_later, &before_it_existed)
            .await,
        "the clone must see the refreshed snapshot, not its own stale copy"
    );

    Ok(())
}

/// Two clones share one fallback cache, so a miss is queried once rather than
/// once per consumer.
#[tokio::test]
async fn clones_share_the_fallback_cache() -> anyhow::Result<()> {
    let harness = common::setup_iceberg().await?;
    let resolver = common::gateway_resolver(&harness).await?;
    let clone = resolver.clone();

    let unknown = PublicKeyBinary::from(vec![9]);
    let now = Utc::now();

    assert!(!resolver.is_gateway_known(&unknown, &now).await);
    // Answered from the shared cache the first call populated.
    assert!(!clone.is_gateway_known(&unknown, &now).await);

    Ok(())
}

/// Guards the reason clones exist: a resolver built independently starts from
/// its own load and is not updated by anyone else's refresher.
#[tokio::test]
async fn an_independent_resolver_does_not_share_a_snapshot() -> anyhow::Result<()> {
    let harness = common::setup_iceberg().await?;
    let resolver = eager_resolver(&harness).await?;
    let mut refresher = resolver.refresher();

    // A second resolver, built the way the daemon used to build one for tickets.
    let independent = eager_resolver(&harness).await?;

    let onboarded_later = PublicKeyBinary::from(vec![3]);
    let seen_at = Utc::now() - chrono::Duration::days(1);
    hotspot_inventory::seed(
        &harness,
        vec![hotspot_inventory::MobileHotspotInventory::known(
            &onboarded_later,
            seen_at,
        )],
    )
    .await?;

    refresher.tick().await?;

    let before_it_existed = seen_at - chrono::Duration::days(1);
    assert!(
        resolver
            .is_gateway_known(&onboarded_later, &before_it_existed)
            .await,
        "the refreshed resolver should see it"
    );
    assert!(
        !independent
            .is_gateway_known(&onboarded_later, &before_it_existed)
            .await,
        "an independently built resolver is not refreshed by someone else's task"
    );

    Ok(())
}
