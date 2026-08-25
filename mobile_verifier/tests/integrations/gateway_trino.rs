//! End-to-end test of [`TrinoGatewayResolver`] against a real Trino, seeding the
//! on-chain `mobile_hotspot_inventory` table the resolver reads from.
//!
//! The production resolver targets `network.chain.mobile_hotspot_inventory`. The
//! test harness only serves iceberg tables in a per-test catalog, so we recreate
//! the table here (only the columns the resolver's SQL touches) and pass a
//! fully-qualified `<catalog>.chain.mobile_hotspot_inventory` name to
//! [`TrinoGatewayResolver::new_with_inventory_table`] — production uses
//! [`gateway::MOBILE_HOTSPOT_INVENTORY_TABLE`].

use std::time::Duration;

use chrono::{DateTime, FixedOffset, Utc};
use helium_crypto::PublicKeyBinary;
use helium_iceberg::{FieldDefinition, IcebergTestHarness, PartitionDefinition, TableDefinition};
use mobile_verifier::gateway::{
    DeviceType, GatewayResolution, GatewayResolver, TrinoGatewayResolver,
};
use serde::Serialize;

const NAMESPACE: &str = "chain";
const TABLE_NAME: &str = "mobile_hotspot_inventory";

/// A location H3 cell, written to `asserted_hex` as a lowercase hex string (the
/// on-chain encoding) and expected back as this `u64`.
const LOCATION: u64 = 0x8c2681a3064d9ff;

/// Only the columns [`TrinoGatewayResolver`] queries: `pub_key`, `device_type`,
/// `asserted_hex` and `inserted_at` (the as-of filter). `asserted_hex` is
/// nullable — unasserted gateways carry a NULL (there are ~2k of these on-chain).
#[derive(Serialize)]
struct InventoryRow {
    pub_key: String,
    device_type: String,
    asserted_hex: Option<String>,
    inserted_at: DateTime<FixedOffset>,
}

fn row(pub_key: &PublicKeyBinary, device_type: &str, asserted_hex: Option<&str>) -> InventoryRow {
    // A fixed past instant, so normal lookups resolve this row as already onboarded.
    let inserted_at: DateTime<FixedOffset> = "2024-01-01T00:00:00Z".parse().unwrap();
    InventoryRow {
        pub_key: pub_key.to_string(),
        device_type: device_type.to_string(),
        asserted_hex: asserted_hex.map(str::to_string),
        inserted_at,
    }
}

fn row_at(
    pub_key: &PublicKeyBinary,
    device_type: &str,
    asserted_hex: Option<&str>,
    inserted_at: DateTime<Utc>,
) -> InventoryRow {
    InventoryRow {
        pub_key: pub_key.to_string(),
        device_type: device_type.to_string(),
        asserted_hex: asserted_hex.map(str::to_string),
        inserted_at: inserted_at.into(),
    }
}

fn inventory_table() -> anyhow::Result<TableDefinition> {
    Ok(TableDefinition::builder(NAMESPACE, TABLE_NAME)
        .with_fields([
            FieldDefinition::required_string("pub_key"),
            FieldDefinition::required_string("device_type"),
            // Nullable, matching production (unasserted gateways have NULL here).
            FieldDefinition::optional_string("asserted_hex"),
            FieldDefinition::required_timestamptz("inserted_at"),
        ])
        .with_partition(PartitionDefinition::day("inserted_at", "inserted_at_day"))
        .build()?)
}

async fn harness() -> anyhow::Result<IcebergTestHarness> {
    Ok(IcebergTestHarness::new_with_tables([inventory_table()?]).await?)
}

async fn seed(h: &IcebergTestHarness, id: &str, rows: Vec<InventoryRow>) -> anyhow::Result<()> {
    h.get_table_writer_in::<InventoryRow>(NAMESPACE, TABLE_NAME)
        .await?
        .write_idempotent(id, rows)
        .await?;
    Ok(())
}

/// A fully-qualified `catalog.schema.table` name for this test's catalog, so the
/// resolver's query is independent of the client's default catalog.
fn inventory_table_name(h: &IcebergTestHarness) -> String {
    format!("{}.{}.{}", h.catalog_name(), NAMESPACE, TABLE_NAME)
}

async fn resolver(h: &IcebergTestHarness) -> anyhow::Result<TrinoGatewayResolver> {
    let client = trino_client::Client::from_client(h.owned_trino().await?);
    Ok(TrinoGatewayResolver::new_with_inventory_table(
        client,
        inventory_table_name(h),
        Duration::from_mins(60),
    )
    .await)
}

#[tokio::test]
async fn snapshot_resolves_each_device_type_and_location() -> anyhow::Result<()> {
    let h = harness().await?;

    let asserted = PublicKeyBinary::from(vec![1]);
    let data_only = PublicKeyBinary::from(vec![2]);
    let unasserted = PublicKeyBinary::from(vec![3]);
    let unknown = PublicKeyBinary::from(vec![4]);

    seed(
        &h,
        "initial",
        vec![
            row(&asserted, "WIFI_INDOOR", Some("8c2681a3064d9ff")),
            // Unasserted / data-only gateways carry a NULL asserted_hex on-chain.
            row(&data_only, "WIFI_DATA_ONLY", None),
            row(&unasserted, "WIFI_OUTDOOR", None),
        ],
    )
    .await?;

    let resolver = resolver(&h).await?;
    let now = Utc::now();

    // Latest row wins: asserted wifi_indoor with the parsed hex location.
    assert!(matches!(
        resolver.resolve_gateway(&asserted, &now).await?,
        GatewayResolution::AssertedLocation(loc, DeviceType::WifiIndoor) if loc == LOCATION
    ));
    assert!(matches!(
        resolver.resolve_gateway(&data_only, &now).await?,
        GatewayResolution::DataOnly
    ));
    assert!(matches!(
        resolver.resolve_gateway(&unasserted, &now).await?,
        GatewayResolution::GatewayNotAsserted
    ));
    assert!(matches!(
        resolver.resolve_gateway(&unknown, &now).await?,
        GatewayResolution::GatewayNotFound
    ));

    Ok(())
}

#[tokio::test]
async fn fallback_resolves_gateway_missing_from_snapshot() -> anyhow::Result<()> {
    let h = harness().await?;

    // Snapshot is loaded with just this gateway.
    let in_snapshot = PublicKeyBinary::from(vec![1]);
    seed(&h, "initial", vec![row(&in_snapshot, "WIFI_OUTDOOR", None)]).await?;

    let resolver = resolver(&h).await?;

    // Onboarded after the snapshot load: only the per-pubkey Trino fallback can
    // find it (the resolver's refresh interval is an hour and never fires here).
    let onboarded_later = PublicKeyBinary::from(vec![9]);
    seed(
        &h,
        "later",
        vec![row(
            &onboarded_later,
            "WIFI_INDOOR",
            Some("8c2681a3064d9ff"),
        )],
    )
    .await?;

    let now = Utc::now();
    assert!(matches!(
        resolver.resolve_gateway(&onboarded_later, &now).await?,
        GatewayResolution::AssertedLocation(loc, DeviceType::WifiIndoor) if loc == LOCATION
    ));

    // A pubkey in neither the snapshot nor the table resolves to not-found via
    // the same fallback path.
    let never_seen = PublicKeyBinary::from(vec![42]);
    assert!(matches!(
        resolver.resolve_gateway(&never_seen, &now).await?,
        GatewayResolution::GatewayNotFound
    ));

    Ok(())
}

#[tokio::test]
async fn snapshot_respects_gateway_inserted_at() -> anyhow::Result<()> {
    let h = harness().await?;
    let gateway = PublicKeyBinary::from(vec![1]);
    let inserted_at = Utc::now() - chrono::Duration::hours(1);

    seed(
        &h,
        "initial",
        vec![row_at(&gateway, "WIFI_OUTDOOR", None, inserted_at)],
    )
    .await?;
    let resolver = resolver(&h).await?;

    assert!(matches!(
        resolver
            .resolve_gateway(&gateway, &(inserted_at - chrono::Duration::seconds(1)))
            .await?,
        GatewayResolution::GatewayNotFound
    ));
    assert!(matches!(
        resolver.resolve_gateway(&gateway, &inserted_at).await?,
        GatewayResolution::GatewayNotAsserted
    ));

    Ok(())
}

#[tokio::test]
async fn fallback_cache_respects_each_query_timestamp() -> anyhow::Result<()> {
    let h = harness().await?;
    let old_then_new = PublicKeyBinary::from(vec![1]);
    let new_then_old = PublicKeyBinary::from(vec![2]);
    let inserted_at = Utc::now() - chrono::Duration::hours(1);
    let before = inserted_at - chrono::Duration::seconds(1);

    let resolver = resolver(&h).await?;
    seed(
        &h,
        "later",
        vec![
            row_at(&old_then_new, "WIFI_OUTDOOR", None, inserted_at),
            row_at(&new_then_old, "WIFI_OUTDOOR", None, inserted_at),
        ],
    )
    .await?;

    assert!(matches!(
        resolver.resolve_gateway(&old_then_new, &before).await?,
        GatewayResolution::GatewayNotFound
    ));
    assert!(matches!(
        resolver
            .resolve_gateway(&old_then_new, &inserted_at)
            .await?,
        GatewayResolution::GatewayNotAsserted
    ));

    assert!(matches!(
        resolver
            .resolve_gateway(&new_then_old, &inserted_at)
            .await?,
        GatewayResolution::GatewayNotAsserted
    ));
    assert!(matches!(
        resolver.resolve_gateway(&new_then_old, &before).await?,
        GatewayResolution::GatewayNotFound
    ));

    Ok(())
}
