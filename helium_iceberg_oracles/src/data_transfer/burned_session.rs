use chrono::{DateTime, FixedOffset};
use file_store_oracles::mobile_transfer::ValidDataTransferSession;
use helium_iceberg::{FieldDefinition, PartitionDefinition, TableDefinition};
use serde::{Deserialize, Serialize};
use trino_rust_client::Trino;

pub use super::multiplier_ticket_history::{
    MultiplierDecimal, MULTIPLIER_PRECISION, MULTIPLIER_SCALE,
};
pub use super::NAMESPACE;
pub const TABLE_NAME: &str = "burned_sessions";

#[derive(Debug, Clone, Trino, Serialize, Deserialize, PartialEq)]
pub struct IcebergBurnedDataTransferSession {
    pub pub_key: String,
    pub payer: String,
    pub upload_bytes: u64,
    pub download_bytes: u64,
    pub rewardable_bytes: u64,
    pub num_dcs: u64,
    /// HIP-150: the multiplier `num_dcs` was derived with.
    ///
    /// `num_dcs` is the post-multiplier figure — what the payer burned, and what
    /// the reward path distributes pro-rata of — so this is what makes the
    /// pre-multiplier count recoverable and the burn auditable.
    ///
    /// `None` on rows written before HIP-150. Every row written since carries an
    /// explicit value, `1` included, so absent never has to be read as "probably
    /// unmultiplied".
    pub multiplier: Option<MultiplierDecimal>,

    /// Timestamp of the first ingest file we found a data transfer session in
    pub first_timestamp: DateTime<FixedOffset>,
    /// Timestamp of the last ingest file we found a data transfer session in
    pub last_timestamp: DateTime<FixedOffset>,
    /// Timestamp of when the burn transaction was confirmed
    pub burn_timestamp: DateTime<FixedOffset>,
}

pub fn table_definition() -> helium_iceberg::Result<TableDefinition> {
    TableDefinition::builder(NAMESPACE, TABLE_NAME)
        .with_fields([
            FieldDefinition::required_string("pub_key"),
            FieldDefinition::required_string("payer"),
            FieldDefinition::required_long("upload_bytes"),
            FieldDefinition::required_long("download_bytes"),
            FieldDefinition::required_long("rewardable_bytes"),
            FieldDefinition::required_long("num_dcs"),
            // Optional so the rows already in this table stay readable: it is
            // added to a live table by hand, and nothing backfills them.
            FieldDefinition::optional_decimal("multiplier", MULTIPLIER_PRECISION, MULTIPLIER_SCALE),
            FieldDefinition::required_timestamptz("first_timestamp"),
            FieldDefinition::required_timestamptz("last_timestamp"),
            FieldDefinition::required_timestamptz("burn_timestamp"),
        ])
        .with_partition(PartitionDefinition::day(
            "burn_timestamp",
            "burn_timestamp_day",
        ))
        .build()
}

pub async fn get_all(
    trino: &trino_rust_client::Client,
) -> anyhow::Result<Vec<IcebergBurnedDataTransferSession>> {
    let all = match trino
        .get_all(format!("SELECT * from {NAMESPACE}.{TABLE_NAME}"))
        .await
    {
        Ok(all) => all.into_vec(),
        Err(err) => return Err(err.into()),
    };
    Ok(all)
}

impl From<ValidDataTransferSession> for IcebergBurnedDataTransferSession {
    fn from(value: ValidDataTransferSession) -> Self {
        IcebergBurnedDataTransferSession {
            pub_key: value.pub_key.to_string(),
            payer: value.payer.to_string(),
            upload_bytes: value.upload_bytes,
            download_bytes: value.download_bytes,
            rewardable_bytes: value.rewardable_bytes,
            num_dcs: value.num_dcs,
            // Validated on the way in and bounded well inside decimal(9,6), so
            // this cannot realistically fail; `None` would understate the burn
            // rather than misstate it.
            multiplier: MultiplierDecimal::try_from(value.multiplier.as_decimal()).ok(),
            first_timestamp: value.first_timestamp.into(),
            last_timestamp: value.last_timestamp.into(),
            burn_timestamp: value.burn_timestamp.into(),
        }
    }
}
