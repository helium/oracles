use chrono::{DateTime, FixedOffset};
use file_store_oracles::speedtest::CellSpeedtestIngestReport;
use helium_iceberg::{FieldDefinition, PartitionDefinition, SortFieldDefinition, TableDefinition};
use serde::{Deserialize, Serialize};
use trino_rust_client::Trino;

pub use super::NAMESPACE;
pub const TABLE_NAME: &str = "speedtests";

#[derive(Debug, Clone, Trino, Serialize, Deserialize, PartialEq)]
pub struct IcebergSpeedtest {
    hotspot_pubkey: String,
    serial: String,
    received_timestamp: DateTime<FixedOffset>,
    timestamp: DateTime<FixedOffset>,
    upload_speed: u64,
    download_speed: u64,
    latency: u32,
}

pub fn table_definition() -> helium_iceberg::Result<TableDefinition> {
    TableDefinition::builder(NAMESPACE, TABLE_NAME)
        .with_fields([
            FieldDefinition::required_string("hotspot_pubkey"),
            FieldDefinition::required_string("serial"),
            FieldDefinition::required_timestamptz("received_timestamp"),
            FieldDefinition::required_timestamptz("timestamp"),
            FieldDefinition::required_long("upload_speed"),
            FieldDefinition::required_long("download_speed"),
            FieldDefinition::required_long("latency"),
        ])
        .with_partition(PartitionDefinition::day(
            "received_timestamp",
            "received_timestamp_day",
        ))
        .with_sort_fields([
            SortFieldDefinition::ascending("hotspot_pubkey"),
            SortFieldDefinition::ascending("received_timestamp"),
        ])
        .build()
}

pub async fn get_all(trino: &trino_rust_client::Client) -> anyhow::Result<Vec<IcebergSpeedtest>> {
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

impl From<&CellSpeedtestIngestReport> for IcebergSpeedtest {
    fn from(value: &CellSpeedtestIngestReport) -> Self {
        Self {
            hotspot_pubkey: value.report.pubkey.to_string(),
            serial: value.report.serial.clone(),
            received_timestamp: value.received_timestamp.into(),
            timestamp: value.report.timestamp.into(),
            upload_speed: value.report.upload_speed,
            download_speed: value.report.download_speed,
            latency: value.report.latency,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use file_store_oracles::speedtest::CellSpeedtest;
    use helium_crypto::PublicKeyBinary;

    fn make_report() -> CellSpeedtestIngestReport {
        let pubkey: PublicKeyBinary = "112NqN2WWMwtK29PMzRby62fDydBJfsCLkCAf392stdok48ovNT6"
            .parse()
            .unwrap();
        let speedtest_time = Utc::now() - chrono::Duration::seconds(30);
        let received_time = Utc::now();
        CellSpeedtestIngestReport {
            received_timestamp: received_time,
            report: CellSpeedtest {
                pubkey,
                serial: "test-serial".to_string(),
                timestamp: speedtest_time,
                upload_speed: 10_000_000,
                download_speed: 100_000_000,
                latency: 25,
            },
        }
    }

    #[test]
    fn convert_cell_speedtest_ingest_report() {
        let report = make_report();
        let iceberg = IcebergSpeedtest::from(&report);

        assert_eq!(iceberg.hotspot_pubkey, report.report.pubkey.to_string());
        assert_eq!(iceberg.serial, "test-serial");
        assert_eq!(iceberg.upload_speed, 10_000_000);
        assert_eq!(iceberg.download_speed, 100_000_000);
        assert_eq!(iceberg.latency, 25);
        assert_ne!(
            iceberg.received_timestamp, iceberg.timestamp,
            "received_timestamp and measurement timestamp should differ"
        );
    }
}
