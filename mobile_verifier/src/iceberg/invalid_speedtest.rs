//! `poc.invalid_speedtests` — rejected speedtests.
//!
//! Same schema as [`super::speedtest`] (`poc.speedtests`) plus a `reason`
//! column recording the `SpeedtestVerificationResult` that rejected the row.

use chrono::{DateTime, FixedOffset};
use helium_iceberg::{FieldDefinition, TableDefinition};
use helium_proto::services::poc_mobile::SpeedtestVerificationResult;
use serde::{Deserialize, Serialize};
use trino_rust_client::Trino;

use super::{speedtest::IcebergSpeedtest, NAMESPACE, REASON_COLUMN};

pub const TABLE_NAME: &str = "invalid_speedtests";

#[derive(Debug, Clone, Trino, Serialize, Deserialize, PartialEq)]
pub struct IcebergInvalidSpeedtest {
    pub hotspot_pubkey: String,
    pub serial: String,
    pub received_timestamp: DateTime<FixedOffset>,
    pub timestamp: DateTime<FixedOffset>,
    pub upload_speed: u64,
    pub download_speed: u64,
    pub latency: u32,
    pub reason: String,
}

impl IcebergInvalidSpeedtest {
    /// Tag an already-converted speedtest with the result that rejected it.
    pub fn new(speedtest: IcebergSpeedtest, result: SpeedtestVerificationResult) -> Self {
        Self {
            hotspot_pubkey: speedtest.hotspot_pubkey,
            serial: speedtest.serial,
            received_timestamp: speedtest.received_timestamp,
            timestamp: speedtest.timestamp,
            upload_speed: speedtest.upload_speed,
            download_speed: speedtest.download_speed,
            latency: speedtest.latency,
            reason: super::reason_without_prefix(result.as_str_name(), "speedtest_"),
        }
    }
}

/// Reuses the `poc.speedtests` definition, renamed and with a `reason` column.
pub fn table_definition() -> helium_iceberg::Result<TableDefinition> {
    Ok(super::speedtest::table_definition()?
        .with_name(TABLE_NAME)
        .with_field(FieldDefinition::required_string(REASON_COLUMN)))
}

pub async fn get_all(
    trino: &trino_rust_client::Client,
) -> anyhow::Result<Vec<IcebergInvalidSpeedtest>> {
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
