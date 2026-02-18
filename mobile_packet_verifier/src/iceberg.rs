use helium_iceberg::BoxedDataWriter;
use serde::Serialize;

use crate::iceberg::{
    burned_session::IcebergBurnedDataTransferSession, session::IcebergDataTransferSession,
};

pub type DataTransferWriter = BoxedDataWriter<IcebergDataTransferSession>;
pub type BurnedDataTransferWriter = BoxedDataWriter<IcebergBurnedDataTransferSession>;

pub async fn write<T: Serialize + Send>(
    data_writer: &BoxedDataWriter<T>,
    data: Vec<T>,
) -> anyhow::Result<()> {
    data_writer.write(data).await?;
    Ok(())
}

pub const NAMESPACE: &str = "poc";

pub mod burned_session {
    use chrono::{DateTime, FixedOffset};
    use file_store_oracles::mobile_transfer::ValidDataTransferSession;
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
        /// Timestamp of hte last ingest file we found a data transfer session in
        last_timestamp: DateTime<FixedOffset>,
        /// Timestamp of when the burn transaction was confirmed
        burn_timestamp: DateTime<FixedOffset>,
    }

    pub fn table_definition() -> helium_iceberg::TableDefinition {
        use helium_iceberg::*;

        TableDefinition::builder(NAMESPACE, TABLE_NAME)
            .with_fields([
                FieldDefinition::required("pub_key", PrimitiveType::String),
                FieldDefinition::required("payer", PrimitiveType::String),
                FieldDefinition::required("upload_bytes", PrimitiveType::Long),
                FieldDefinition::required("download_bytes", PrimitiveType::Long),
                FieldDefinition::required("rewardable_bytes", PrimitiveType::Long),
                FieldDefinition::required("num_dcs", PrimitiveType::Long),
                FieldDefinition::required("first_timestamp", PrimitiveType::Timestamptz),
                FieldDefinition::required("last_timestamp", PrimitiveType::Timestamptz),
                FieldDefinition::required("burn_timestamp", PrimitiveType::Timestamptz),
            ])
            .with_partition(PartitionDefinition::day(
                "burn_timestamp",
                "burn_timestamp_day",
            ))
            .build()
            .expect("valid burned data transfer sessions table")
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
}

pub mod session {

    use chrono::{DateTime, FixedOffset};
    use file_store_oracles::mobile_session::DataTransferSessionIngestReport;
    use serde::{Deserialize, Serialize};
    use trino_rust_client::Trino;

    pub use super::NAMESPACE;
    pub const TABLE_NAME: &str = "sessions";

    #[derive(Debug, Clone, Trino, Serialize, Deserialize, PartialEq)]
    pub struct IcebergDataTransferSession {
        report_received_timestamp: DateTime<FixedOffset>,
        // -- request
        request_pub_key: String,
        rewardable_bytes: u64,
        carrier_id: String,
        sampling: bool,
        // -- request -- data transfer usage
        data_transfer_event_pub_key: String,
        upload_bytes: u64,
        download_byte: u64,
        radio_access_thechnology: String,
        event_id: String,
        payer: String,
        timestamp: DateTime<FixedOffset>,
    }

    pub fn table_definition() -> helium_iceberg::TableDefinition {
        use helium_iceberg::*;

        TableDefinition::builder(NAMESPACE, TABLE_NAME)
            .with_fields([
                FieldDefinition::required("report_received_timestamp", PrimitiveType::Timestamptz),
                FieldDefinition::required("request_pub_key", PrimitiveType::String),
                FieldDefinition::required("rewardable_bytes", PrimitiveType::Long),
                FieldDefinition::required("carrier_id", PrimitiveType::String),
                FieldDefinition::required("sampling", PrimitiveType::Boolean),
                FieldDefinition::required("data_transfer_event_pub_key", PrimitiveType::String),
                FieldDefinition::required("upload_bytes", PrimitiveType::Long),
                FieldDefinition::required("download_byte", PrimitiveType::Long),
                FieldDefinition::required("radio_access_thechnology", PrimitiveType::String),
                FieldDefinition::required("event_id", PrimitiveType::String),
                FieldDefinition::required("payer", PrimitiveType::String),
                FieldDefinition::required("timestamp", PrimitiveType::Timestamptz),
            ])
            .with_partition(PartitionDefinition::day(
                "report_received_timestamp",
                "report_received_timestamp_day",
            ))
            .build()
            .expect("valid data transfer session table")
    }

    pub async fn get_all(
        trino: &trino_rust_client::Client,
    ) -> anyhow::Result<Vec<IcebergDataTransferSession>> {
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

    impl From<DataTransferSessionIngestReport> for IcebergDataTransferSession {
        fn from(value: DataTransferSessionIngestReport) -> Self {
            let request_pub_key = value.report.pub_key.to_string();
            let data_transfer_event_pub_key = value.report.data_transfer_usage.pub_key.to_string();

            debug_assert_eq!(
                request_pub_key, data_transfer_event_pub_key,
                "pub_keys should match for event"
            );

            Self {
                report_received_timestamp: value.received_timestamp.into(),
                request_pub_key,
                rewardable_bytes: value.report.rewardable_bytes,
                // TODO(mj): cast to simpler string
                carrier_id: value.report.carrier_id.as_str_name().to_string(),
                sampling: value.report.sampling,
                data_transfer_event_pub_key,
                upload_bytes: value.report.data_transfer_usage.upload_bytes,
                download_byte: value.report.data_transfer_usage.download_bytes,
                radio_access_thechnology: value
                    .report
                    .data_transfer_usage
                    .radio_access_technology
                    .as_str_name()
                    .to_string(),
                event_id: value.report.data_transfer_usage.event_id,
                payer: value.report.data_transfer_usage.payer.to_string(),
                timestamp: value.report.data_transfer_usage.timestamp.into(),
            }
        }
    }
}
