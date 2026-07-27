//! Test fixture mirroring the on-chain `network.chain.mobile_hotspot_inventory`
//! Trino table so `GatewayResolver::is_gateway_known` can be exercised against a
//! real Trino query instead of a mock.
//!
//! The [`IcebergTestHarness`] registers a uniquely-named catalog per test, so
//! the production `network` catalog prefix isn't available. The table lives in
//! the `chain` namespace (Trino schema) and resolves as
//! [`RESOLVER_TABLE`] against the Trino client's default catalog — pass that to
//! `GatewayResolver::new_with_inventory_table`.

use chrono::{DateTime, FixedOffset, Utc};
use helium_crypto::PublicKeyBinary;
use helium_iceberg::{FieldDefinition, IcebergTestHarness, PartitionDefinition, TableDefinition};
use serde::{Deserialize, Serialize};
use trino_rust_client::Trino;

/// Iceberg namespace (Trino schema) that holds the inventory table.
pub const NAMESPACE: &str = "chain";
pub const TABLE_NAME: &str = "mobile_hotspot_inventory";

/// Two-part `schema.table` name the resolver should query in tests. Resolves
/// against the harness Trino client's (per-test) default catalog.
pub const RESOLVER_TABLE: &str = "chain.mobile_hotspot_inventory";

/// Row mirroring `network.chain.mobile_hotspot_inventory`. Column order matches
/// the on-chain table. `is_gateway_known` only reads `pub_key` and `inserted_at`
/// (the stable first-seen timestamp); the other columns exist so the fixture is
/// a faithful copy and get placeholder values from [`MobileHotspotInventory::known`].
#[derive(Debug, Clone, Trino, Serialize, Deserialize, PartialEq)]
pub struct MobileHotspotInventory {
    pub file_ts: u64,
    pub record_index: u64,
    pub received_timestamp: DateTime<FixedOffset>,
    /// Stable "first seen on-chain" timestamp; unchanged on reassertion.
    pub inserted_at: DateTime<FixedOffset>,
    pub block: u64,
    pub timestamp: DateTime<FixedOffset>,
    pub pub_key: String,
    pub asset: String,
    pub serial_number: String,
    pub device_type: String,
    pub asserted_hex: String,
    pub azimuth: u32,
    pub signer: String,
    pub location_changed_at: DateTime<FixedOffset>,
    pub animal_name: String,
}

impl MobileHotspotInventory {
    /// A row marking `pub_key` as first seen on-chain at `seen_at` (sets
    /// `inserted_at`, the field `is_gateway_known` compares against). Every other
    /// column gets a placeholder.
    pub fn known(pub_key: &PublicKeyBinary, seen_at: DateTime<Utc>) -> Self {
        let ts: DateTime<FixedOffset> = seen_at.into();
        Self {
            file_ts: 0,
            record_index: 0,
            received_timestamp: ts,
            inserted_at: ts,
            block: 0,
            timestamp: ts,
            pub_key: pub_key.to_string(),
            asset: String::new(),
            serial_number: String::new(),
            device_type: "wifi_indoor".to_string(),
            asserted_hex: String::new(),
            azimuth: 0,
            signer: String::new(),
            location_changed_at: ts,
            animal_name: String::new(),
        }
    }
}

pub fn table_definition() -> helium_iceberg::Result<TableDefinition> {
    TableDefinition::builder(NAMESPACE, TABLE_NAME)
        .with_fields([
            FieldDefinition::required_long("file_ts"),
            FieldDefinition::required_long("record_index"),
            FieldDefinition::required_timestamptz("received_timestamp"),
            FieldDefinition::required_timestamptz("inserted_at"),
            FieldDefinition::required_long("block"),
            FieldDefinition::required_timestamptz("timestamp"),
            FieldDefinition::required_string("pub_key"),
            FieldDefinition::required_string("asset"),
            FieldDefinition::required_string("serial_number"),
            FieldDefinition::required_string("device_type"),
            FieldDefinition::required_string("asserted_hex"),
            FieldDefinition::required_int("azimuth"),
            FieldDefinition::required_string("signer"),
            FieldDefinition::required_timestamptz("location_changed_at"),
            FieldDefinition::required_string("animal_name"),
        ])
        // The real table is unpartitioned, but the helium-iceberg writer only
        // supports partitioned tables. Partitioning is a physical detail that
        // doesn't affect the `pub_key` / `inserted_at` lookup.
        .with_partition(PartitionDefinition::day(
            "received_timestamp",
            "received_timestamp_day",
        ))
        .build()
}

/// Insert inventory rows into the harness table. No-op when `rows` is empty.
pub async fn seed(
    harness: &IcebergTestHarness,
    rows: Vec<MobileHotspotInventory>,
) -> anyhow::Result<()> {
    if rows.is_empty() {
        return Ok(());
    }
    let writer = harness
        .get_table_writer_in::<MobileHotspotInventory>(NAMESPACE, TABLE_NAME)
        .await?;
    writer.write(rows).await?;
    Ok(())
}
