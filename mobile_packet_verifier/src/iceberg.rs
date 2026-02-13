pub mod burned_data_transfer {
    use chrono::{DateTime, FixedOffset};
    use file_store_oracles::mobile_transfer::ValidDataTransferSession;
    use helium_iceberg::BoxedDataWriter;
    use serde::{Deserialize, Serialize};
    use trino_rust_client::Trino;

    // TODO(mj): change to burned_sessions
    pub const TABLE_NAME: &str = "burned_data_transfer_sessions";

    #[derive(Debug, Clone, Trino, Serialize, Deserialize, PartialEq)]
    pub struct TrinoBurnedDataTransferSession {
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

        TableDefinition::builder(TABLE_NAME)
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

    pub async fn write_with(
        data_writer: BoxedDataWriter<TrinoBurnedDataTransferSession>,
        burns: Vec<TrinoBurnedDataTransferSession>,
    ) -> anyhow::Result<()> {
        data_writer.write(burns).await?;
        Ok(())
    }

    pub async fn get_all(
        trino: &trino_rust_client::Client,
    ) -> anyhow::Result<Vec<TrinoBurnedDataTransferSession>> {
        let all = trino.get_all(format!("SELECT * from {TABLE_NAME}")).await?;
        Ok(all.into_vec())
    }

    impl From<ValidDataTransferSession> for TrinoBurnedDataTransferSession {
        fn from(value: ValidDataTransferSession) -> Self {
            TrinoBurnedDataTransferSession {
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

pub mod data_transfer_session {

    use chrono::DateTime;
    use chrono::FixedOffset;
    use file_store_oracles::mobile_session::DataTransferSessionIngestReport;
    use helium_iceberg::BoxedDataWriter;
    use serde::Deserialize;
    use serde::Serialize;
    use trino_rust_client::Trino;

    // TODO(mj): change to sessions
    pub const TABLE_NAME: &str = "data_transfer_sessions";

    #[derive(Debug, Clone, Trino, Serialize, Deserialize, PartialEq)]
    pub struct TrinoDataTransferSession {
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

        TableDefinition::builder(TABLE_NAME)
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

    pub async fn write_with(
        data_writer: BoxedDataWriter<TrinoDataTransferSession>,
        valid_reports: Vec<TrinoDataTransferSession>,
    ) -> anyhow::Result<()> {
        data_writer.write(valid_reports).await?;
        Ok(())
    }

    pub async fn get_all(
        trino: &trino_rust_client::Client,
    ) -> anyhow::Result<Vec<TrinoDataTransferSession>> {
        let all = trino.get_all(format!("SELECT * from {TABLE_NAME}")).await?;
        Ok(all.into_vec())
    }

    impl From<DataTransferSessionIngestReport> for TrinoDataTransferSession {
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

    #[cfg(test)]
    mod tests {
        use trino_rust_client::ClientBuilder;

        use super::*;

        #[tokio::test]
        async fn get_from_test() -> anyhow::Result<()> {
            let trino = ClientBuilder::new("test", "localhost")
                .port(8080)
                .catalog("iceberg")
                .schema("test_b9d581eaa7804d15a96cb95b5a081cf5")
                .build()?;

            let all = get_all(&trino).await?;
            println!("found: {}", all.len());

            // for (index, item) in all.iter().enumerate() {
            //     println!("({index}): {item:?}");
            // }

            Ok(())
        }

        #[tokio::test]
        async fn writing_with_data_writer() -> anyhow::Result<()> {
            use helium_iceberg::*;

            let harness = IcebergTestHarness::new().await?;
            harness.create_table(table_definition()).await?;

            let catalog = harness.iceberg_catalog().clone();
            let exists = catalog
                .table_exists(harness.namespace(), TABLE_NAME)
                .await?;
            assert!(exists);

            let table =
                IcebergTable::from_catalog(catalog, harness.namespace(), TABLE_NAME).await?;

            let dts = (0..500)
                .map(mk_dt)
                .map(TrinoDataTransferSession::from)
                .collect::<Vec<_>>();

            use helium_iceberg::DataWriter;
            table.write(dts).await?;

            Ok(())
        }

        fn mk_dt(rewardable_bytes: u64) -> DataTransferSessionIngestReport {
            use chrono::Utc;
            use file_store_oracles::mobile_session::DataTransferSessionReq;
            use helium_crypto::PublicKeyBinary;
            use helium_proto::services::poc_mobile::CarrierIdV2;
            use helium_proto::services::poc_mobile::DataTransferRadioAccessTechnology;
            use std::str::FromStr;

            let payer_key =
                PublicKeyBinary::from_str("112c85vbMr7afNc88QhTginpDEVNC5miouLWJstsX6mCaLxf8WRa")
                    .expect("valid pubkey");

            DataTransferSessionIngestReport {
                received_timestamp: Utc::now(),
                report: DataTransferSessionReq {
                    rewardable_bytes,
                    pub_key: PublicKeyBinary::from(vec![1]),
                    signature: vec![],
                    carrier_id: CarrierIdV2::Carrier9,
                    sampling: false,
                    data_transfer_usage: file_store_oracles::mobile_session::DataTransferEvent {
                        pub_key: PublicKeyBinary::from(vec![1]),
                        upload_bytes: 0,
                        download_bytes: 0,
                        radio_access_technology: DataTransferRadioAccessTechnology::Wlan,
                        event_id: sqlx::types::Uuid::new_v4().to_string(),
                        payer: payer_key.clone(),
                        timestamp: Utc::now(),
                        signature: vec![],
                    },
                },
            }
        }
    }
}
