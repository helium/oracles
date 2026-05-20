use chrono::{DateTime, FixedOffset, Utc};
use file_store_oracles::mobile::speedtest::cli::{
    SpeedtestAverage as FileStoreSpeedtestAverage, SpeedtestAverageEntry,
};
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

pub async fn get_all(trino: &trino_client::Client) -> anyhow::Result<Vec<IcebergSpeedtestAvg>> {
    let all = match trino
        .get_all_raw(format!("SELECT * from {NAMESPACE}.{TABLE_NAME}"))
        .await
    {
        Ok(all) => all,
        Err(trino_client::Error::EmptyData) => vec![],
        Err(err) => return Err(err.into()),
    };
    Ok(all)
}

impl From<&SpeedtestAverageEntry> for IcebergSpeedtestAvgSample {
    fn from(value: &SpeedtestAverageEntry) -> Self {
        Self {
            upload_speed_bps: value.upload_speed_bps,
            download_speed_bps: value.download_speed_bps,
            latency_ms: value.latency_ms,
            timestamp: value.timestamp.into(),
        }
    }
}

impl From<&FileStoreSpeedtestAverage> for IcebergSpeedtestAvg {
    fn from(value: &FileStoreSpeedtestAverage) -> Self {
        Self {
            hotspot_pubkey: value.pub_key.to_string(),
            upload_speed_avg_bps: value.upload_speed_avg_bps,
            download_speed_avg_bps: value.download_speed_avg_bps,
            latency_avg_ms: value.latency_avg_ms,
            reward_multiplier: value.reward_multiplier,
            sample_count: value.speedtests.len() as u32,
            timestamp: value.timestamp.into(),
            speedtests: value
                .speedtests
                .iter()
                .map(IcebergSpeedtestAvgSample::from)
                .collect(),
        }
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::SubsecRound;
    use helium_crypto::PublicKeyBinary;
    use helium_proto::services::poc_mobile::SpeedtestAvgValidity;

    #[test]
    fn convert_file_store_speedtest_average() {
        let pub_key: PublicKeyBinary = "112NqN2WWMwtK29PMzRby62fDydBJfsCLkCAf392stdok48ovNT6"
            .parse()
            .unwrap();
        let now = Utc::now().trunc_subsecs(3);
        let avg = FileStoreSpeedtestAverage {
            pub_key: pub_key.clone(),
            upload_speed_avg_bps: 10_000_000,
            download_speed_avg_bps: 100_000_000,
            latency_avg_ms: 25,
            validity: SpeedtestAvgValidity::Valid,
            speedtests: vec![SpeedtestAverageEntry {
                upload_speed_bps: 11_000_000,
                download_speed_bps: 110_000_000,
                latency_ms: 20,
                timestamp: now,
            }],
            timestamp: now,
            reward_multiplier: 1.0,
        };

        let row = IcebergSpeedtestAvg::from(&avg);
        assert_eq!(row.hotspot_pubkey, pub_key.to_string());
        assert_eq!(row.upload_speed_avg_bps, 10_000_000);
        assert_eq!(row.download_speed_avg_bps, 100_000_000);
        assert_eq!(row.latency_avg_ms, 25);
        assert_eq!(row.reward_multiplier, 1.0);
        assert_eq!(row.sample_count, 1);
        assert_eq!(row.speedtests.len(), 1);
    }
}
