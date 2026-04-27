use anyhow::anyhow;
use chrono::{DateTime, FixedOffset, TimeZone, Utc};
use helium_iceberg::{FieldDefinition, PartitionDefinition, TableDefinition};
use helium_proto::PriceReportV1;
use serde::{Deserialize, Serialize};
use trino_rust_client::Trino;

use super::NAMESPACE;
pub const TABLE_NAME: &str = "price";

#[derive(Debug, Clone, Trino, Serialize, Deserialize, PartialEq)]
pub struct IcebergPriceReport {
    pub timestamp: DateTime<FixedOffset>,
    pub price: u64,
    pub token_type: String,
}

pub fn table_definition() -> helium_iceberg::Result<TableDefinition> {
    TableDefinition::builder(NAMESPACE, TABLE_NAME)
        .with_fields([
            FieldDefinition::required_timestamptz("timestamp"),
            FieldDefinition::required_long("price"),
            FieldDefinition::required_string("token_type"),
        ])
        .with_partition(PartitionDefinition::day("timestamp", "timestamp_day"))
        .build()
}

impl TryFrom<&PriceReportV1> for IcebergPriceReport {
    type Error = anyhow::Error;

    fn try_from(value: &PriceReportV1) -> Result<Self, Self::Error> {
        let timestamp = Utc
            .timestamp_opt(value.timestamp as i64, 0)
            .single()
            .ok_or_else(|| anyhow!("invalid timestamp {}", value.timestamp))?;
        Ok(Self {
            timestamp: timestamp.into(),
            price: value.price,
            token_type: value.token_type().as_str_name().to_string(),
        })
    }
}
