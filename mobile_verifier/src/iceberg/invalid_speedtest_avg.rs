//! `poc.invalid_speedtest_avgs` — rejected speedtest averages.
//!
//! Same schema as [`super::speedtest_avg`] (`poc.speedtest_avgs`), including the
//! nested `speedtests` sample list, plus a `reason` column recording the
//! `SpeedtestAvgValidity` that rejected the row.

use chrono::{DateTime, FixedOffset};
use helium_iceberg::{FieldDefinition, TableDefinition};
use helium_proto::services::poc_mobile::SpeedtestAvgValidity;
use serde::{Deserialize, Serialize};
use trino_rust_client::Trino;

use super::{
    speedtest_avg::{IcebergSpeedtestAvg, IcebergSpeedtestAvgSample},
    NAMESPACE, REASON_COLUMN,
};

pub const TABLE_NAME: &str = "invalid_speedtest_avgs";

#[derive(Debug, Clone, Trino, Serialize, Deserialize, PartialEq)]
pub struct IcebergInvalidSpeedtestAvg {
    pub hotspot_pubkey: String,
    pub upload_speed_avg_bps: u64,
    pub download_speed_avg_bps: u64,
    pub latency_avg_ms: u32,
    pub reward_multiplier: f32,
    pub sample_count: u32,
    pub timestamp: DateTime<FixedOffset>,
    pub speedtests: Vec<IcebergSpeedtestAvgSample>,
    pub reason: String,
}

impl IcebergInvalidSpeedtestAvg {
    /// Tag an already-converted average with the validity that rejected it.
    pub fn new(average: IcebergSpeedtestAvg, validity: SpeedtestAvgValidity) -> Self {
        Self {
            hotspot_pubkey: average.hotspot_pubkey,
            upload_speed_avg_bps: average.upload_speed_avg_bps,
            download_speed_avg_bps: average.download_speed_avg_bps,
            latency_avg_ms: average.latency_avg_ms,
            reward_multiplier: average.reward_multiplier,
            sample_count: average.sample_count,
            timestamp: average.timestamp,
            speedtests: average.speedtests,
            reason: super::reason_without_prefix(validity.as_str_name(), "speedtest_avg_validity_"),
        }
    }
}

/// Reuses the `poc.speedtest_avgs` definition, renamed and with a `reason`
/// column.
pub fn table_definition() -> helium_iceberg::Result<TableDefinition> {
    Ok(super::speedtest_avg::table_definition()?
        .with_name(TABLE_NAME)
        .with_field(FieldDefinition::required_string(REASON_COLUMN)))
}

pub async fn get_all(
    trino: &trino_rust_client::Client,
) -> anyhow::Result<Vec<IcebergInvalidSpeedtestAvg>> {
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
