use std::time::Duration;

use helium_iceberg::IcebergTestHarness;
use mobile_packet_verifier::{gateway::GatewayResolver, iceberg};

pub mod hotspot_inventory;

pub async fn setup_iceberg() -> anyhow::Result<IcebergTestHarness> {
    let harness = IcebergTestHarness::new_with_tables([
        iceberg::session::table_definition()?,
        iceberg::invalid_session::table_definition()?,
        iceberg::burned_session::table_definition()?,
        iceberg::multiplier_ticket_history::table_definition()?,
        hotspot_inventory::table_definition()?,
    ])
    .await?;

    Ok(harness)
}

/// Build a real [`GatewayResolver`] backed by the harness's Trino, pointed at
/// the per-test `chain.mobile_hotspot_inventory` table. Seed the gateways that
/// should be considered known with [`hotspot_inventory::seed`] first.
pub async fn gateway_resolver(harness: &IcebergTestHarness) -> anyhow::Result<GatewayResolver> {
    let trino = trino_client::Client::from_client(harness.owned_trino().await?);
    // Tests exercise the startup snapshot + fallback only; they never build a
    // refresher, so no background refresh fires during a test.
    Ok(GatewayResolver::new_with_inventory_table(
        trino,
        hotspot_inventory::RESOLVER_TABLE,
        Duration::from_secs(3600),
    )
    .await)
}
