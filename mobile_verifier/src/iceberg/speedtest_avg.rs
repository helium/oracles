use chrono::{DateTime, FixedOffset, Utc};
use helium_iceberg::{
    FieldDefinition, FieldKind, PartitionDefinition, SortFieldDefinition, TableDefinition,
};
use serde::{Deserialize, Serialize};
use trino_rust_client::Trino;

use crate::speedtests_average::SpeedtestAverage as InMemorySpeedtestAverage;

pub use super::NAMESPACE;
pub const TABLE_NAME: &str = "speedtest_avgs";

#[derive(Debug, Clone, Trino, Serialize, Deserialize, PartialEq)]
pub struct IcebergSpeedtestAvgSample {
    pub upload_speed_bps: u64,
    pub download_speed_bps: u64,
    pub latency_ms: u32,
    pub timestamp: DateTime<FixedOffset>,
}

#[derive(Debug, Clone, Trino, Serialize, Deserialize, PartialEq)]
pub struct IcebergSpeedtestAvg {
    pub hotspot_pubkey: String,
    pub upload_speed_avg_bps: u64,
    pub download_speed_avg_bps: u64,
    pub latency_avg_ms: u32,
    pub reward_multiplier: f32,
    pub sample_count: u32,
    pub timestamp: DateTime<FixedOffset>,
    pub speedtests: Vec<IcebergSpeedtestAvgSample>,
}

fn sample_fields() -> [FieldDefinition; 4] {
    [
        FieldDefinition::required_long("upload_speed_bps"),
        FieldDefinition::required_long("download_speed_bps"),
        FieldDefinition::required_long("latency_ms"),
        FieldDefinition::required_timestamptz("timestamp"),
    ]
}

pub fn table_definition() -> helium_iceberg::Result<TableDefinition> {
    TableDefinition::builder(NAMESPACE, TABLE_NAME)
        .with_fields([
            FieldDefinition::required_string("hotspot_pubkey"),
            FieldDefinition::required_long("upload_speed_avg_bps"),
            FieldDefinition::required_long("download_speed_avg_bps"),
            FieldDefinition::required_long("latency_avg_ms"),
            FieldDefinition::required_float("reward_multiplier"),
            FieldDefinition::required_long("sample_count"),
            FieldDefinition::required_timestamptz("timestamp"),
            FieldDefinition::required_list("speedtests", FieldKind::struct_type(sample_fields())),
        ])
        .with_partition(PartitionDefinition::day("timestamp", "timestamp_day"))
        .with_sort_fields([
            SortFieldDefinition::ascending("hotspot_pubkey"),
            SortFieldDefinition::ascending("timestamp"),
        ])
        .build()
}

pub async fn get_all(
    trino: &trino_rust_client::Client,
) -> anyhow::Result<Vec<IcebergSpeedtestAvg>> {
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

impl From<&InMemorySpeedtestAverage> for IcebergSpeedtestAvg {
    fn from(value: &InMemorySpeedtestAverage) -> Self {
        let timestamp = Utc::now();
        let speedtests = value
            .speedtests
            .iter()
            .map(|st| IcebergSpeedtestAvgSample {
                upload_speed_bps: st.report.upload_speed,
                download_speed_bps: st.report.download_speed,
                latency_ms: st.report.latency,
                timestamp: st.report.timestamp.into(),
            })
            .collect();
        Self {
            hotspot_pubkey: value.pubkey.to_string(),
            upload_speed_avg_bps: value.upload_speed_avg_bps,
            download_speed_avg_bps: value.download_speed_avg_bps,
            latency_avg_ms: value.latency_avg_ms,
            reward_multiplier: value.reward_multiplier.try_into().unwrap_or(0.0),
            sample_count: value.speedtests.len() as u32,
            timestamp: timestamp.into(),
            speedtests,
        }
    }
}
