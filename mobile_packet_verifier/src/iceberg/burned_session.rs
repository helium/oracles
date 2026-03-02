use chrono::{DateTime, FixedOffset};
use file_store_oracles::mobile_transfer::ValidDataTransferSession;
use helium_iceberg::{FieldDefinition, PartitionDefinition, TableDefinition};
use serde::{Deserialize, Serialize};
use trino_rust_client::Trino;

pub use super::NAMESPACE;
pub const TABLE_NAME: &str = "burned_sessions";

#[derive(Debug, Clone, Trino, Serialize, Deserialize, PartialEq)]
pub struct IcebergBurnedDataTransferSession {
    pub_key: String,
    payer: String,
    upload_bytes: u64,
    download_bytes: u64,
    rewardable_bytes: u64,
    num_dcs: u64,

    /// Timestamp of the first ingest file we found a data transfer session in
    first_timestamp: DateTime<FixedOffset>,
    /// Timestamp of the last ingest file we found a data transfer session in
    last_timestamp: DateTime<FixedOffset>,
    /// Timestamp of when the burn transaction was confirmed
    burn_timestamp: DateTime<FixedOffset>,
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
            FieldDefinition::required_timestamptz("first_timestamp"),
            FieldDefinition::required_timestamptz("last_timestamp"),
            FieldDefinition::required_timestamptz("burn_timestamp"),
        ])
        .with_partition(PartitionDefinition::day(
            "burn_timestamp",
            "burn_timestamp_day",
        ))
        .wap_enabled()
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
        Err(trino_rust_client::error::Error::EmptyData) => vec![],
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
            first_timestamp: value.first_timestamp.into(),
            last_timestamp: value.last_timestamp.into(),
            burn_timestamp: value.burn_timestamp.into(),
        }
    }
}
