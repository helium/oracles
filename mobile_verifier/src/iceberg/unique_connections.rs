use chrono::{DateTime, FixedOffset};
use file_store_oracles::unique_connections::VerifiedUniqueConnectionsIngestReport;
use helium_iceberg::{FieldDefinition, PartitionDefinition, SortFieldDefinition, TableDefinition};
use serde::{Deserialize, Serialize};
use trino_rust_client::Trino;

pub use super::NAMESPACE;
pub const TABLE_NAME: &str = "unique_connections";

#[derive(Debug, Clone, Trino, Serialize, Deserialize, PartialEq)]
pub struct IcebergUniqueConnections {
    pubkey: String,
    start_timestamp: DateTime<FixedOffset>,
    end_timestamp: DateTime<FixedOffset>,
    unique_connections: u64,
    report_timestamp: DateTime<FixedOffset>,
    received_timestamp: DateTime<FixedOffset>,
}

pub fn table_definition() -> helium_iceberg::Result<TableDefinition> {
    TableDefinition::builder(NAMESPACE, TABLE_NAME)
        .with_fields([
            FieldDefinition::required_string("pubkey"),
            FieldDefinition::required_timestamptz("start_timestamp"),
            FieldDefinition::required_timestamptz("end_timestamp"),
            FieldDefinition::required_long("unique_connections"),
            FieldDefinition::required_timestamptz("report_timestamp"),
            FieldDefinition::required_timestamptz("received_timestamp"),
        ])
        .with_partition(PartitionDefinition::day(
            "received_timestamp",
            "received_timestamp_day",
        ))
        .with_sort_fields([
            SortFieldDefinition::ascending("pubkey"),
            SortFieldDefinition::ascending("received_timestamp"),
        ])
        .build()
}

pub async fn get_all(
    trino: &trino_rust_client::Client,
) -> anyhow::Result<Vec<IcebergUniqueConnections>> {
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

impl From<&VerifiedUniqueConnectionsIngestReport> for IcebergUniqueConnections {
    fn from(value: &VerifiedUniqueConnectionsIngestReport) -> Self {
        let req = &value.report.report;
        Self {
            pubkey: req.pubkey.to_string(),
            start_timestamp: req.start_timestamp.into(),
            end_timestamp: req.end_timestamp.into(),
            unique_connections: req.unique_connections,
            report_timestamp: req.timestamp.into(),
            received_timestamp: value.report.received_timestamp.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use file_store_oracles::unique_connections::{
        proto::VerifiedUniqueConnectionsIngestReportStatus, UniqueConnectionReq,
        UniqueConnectionsIngestReport,
    };
    use helium_crypto::PublicKeyBinary;

    fn make_report() -> VerifiedUniqueConnectionsIngestReport {
        let pubkey = PublicKeyBinary::from(vec![1, 2, 3]);
        let carrier_key = PublicKeyBinary::from(vec![4, 5, 6]);
        let now = Utc::now();
        let start = now - chrono::Duration::days(7);
        let end = now;
        VerifiedUniqueConnectionsIngestReport {
            timestamp: now,
            report: UniqueConnectionsIngestReport {
                received_timestamp: now,
                report: UniqueConnectionReq {
                    pubkey,
                    start_timestamp: start,
                    end_timestamp: end,
                    unique_connections: 42,
                    timestamp: now - chrono::Duration::minutes(5),
                    carrier_key,
                    signature: vec![],
                },
            },
            status: VerifiedUniqueConnectionsIngestReportStatus::Valid,
        }
    }

    #[test]
    fn table_definition_builds_successfully() {
        let def = table_definition().expect("table definition should build");
        assert_eq!(def.name(), TABLE_NAME);
        assert_eq!(def.namespace(), NAMESPACE);
    }

    #[test]
    fn convert_verified_unique_connections_ingest_report() {
        let report = make_report();
        let iceberg = IcebergUniqueConnections::from(&report);

        assert_eq!(iceberg.pubkey, report.report.report.pubkey.to_string());
        assert_eq!(iceberg.unique_connections, 42);
        assert_ne!(
            iceberg.report_timestamp, iceberg.received_timestamp,
            "report_timestamp and received_timestamp should differ"
        );
    }
}
