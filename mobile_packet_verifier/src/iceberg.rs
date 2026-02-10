pub mod data_transfer_session {

    use chrono::DateTime;
    use chrono::FixedOffset;
    use file_store_oracles::mobile_session::DataTransferSessionIngestReport;
    use helium_iceberg::BoxedDataWriter;
    use serde::Deserialize;
    use serde::Serialize;
    use trino_rust_client::Trino;

    pub const TABLE_NAME: &str = "data_transfer_sessions";
    // NOTE(mj): putting 6 decimals helps with PartialEq when doing a roundtrip,
    // but may be overkill. Does make life a lot easier though.
    const DT_FORMAT: &str = "%Y-%m-%d %H:%M:%S%.6f";

    #[derive(Debug, Clone, Trino, Serialize, Deserialize, PartialEq)]
    pub struct TrinoDataTransferSession {
        report_received_timestamp: DateTime<FixedOffset>,
        // -- request
        request_pub_key: String,
        // reward_cancelled: bool, // TODO(mj): do we want this field?
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

    pub fn table_definition(schema_name: &str) -> helium_iceberg::TableDefinition {
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
            .with_location(format!("s3://iceberg/{schema_name}/data_transfer_sessions"))
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

    pub async fn write(
        trino: &trino_rust_client::Client,
        data: &[TrinoDataTransferSession],
    ) -> anyhow::Result<()> {
        if data.is_empty() {
            return Ok(());
        }

        let data = data
            .iter()
            .map(to_trino_insert)
            .collect::<Vec<_>>()
            .join(", ");

        let query = format!(
            "
            INSERT INTO {TABLE_NAME}
            (
                report_received_timestamp, request_pub_key, rewardable_bytes, carrier_id,
                sampling, data_transfer_event_pub_key, upload_bytes, download_byte,
                radio_access_thechnology, event_id, payer, timestamp
            )
            VALUES {data}
        "
        );

        trino.execute(query).await?;

        Ok(())
    }

    fn to_trino_insert(val: &TrinoDataTransferSession) -> String {
        format!(
            "
            (
            TIMESTAMP '{report_received_timestamp}',
            '{request_pub_key}',
            {rewardable_bytes},
            '{carrier_id}',
            {sampling},
            '{data_transfer_event_pub_key}',
            {upload_bytes},
            {download_byte},
            '{radio_access_thechnology}',
            '{event_id}',
            '{payer}',
            TIMESTAMP '{timestamp}'
            )
            ",
            report_received_timestamp = val.report_received_timestamp.format(DT_FORMAT),
            request_pub_key = val.request_pub_key,
            rewardable_bytes = val.rewardable_bytes,
            carrier_id = val.carrier_id,
            sampling = val.sampling,
            data_transfer_event_pub_key = val.data_transfer_event_pub_key,
            upload_bytes = val.upload_bytes,
            download_byte = val.download_byte,
            radio_access_thechnology = val.radio_access_thechnology,
            event_id = val.event_id,
            payer = val.payer,
            timestamp = val.timestamp.format(DT_FORMAT),
        )
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

            let config = HarnessConfig::default();
            let settings = Settings {
                catalog_uri: config.iceberg_rest_url.clone(),
                catalog_name: config.catalog_name.clone(),
                warehouse: None,
                auth_token: None,
                s3_access_key: Some(config.s3_access_key.clone()),
                s3_secret_key: Some(config.s3_secret_key.clone()),
            };
            let catalog = Catalog::connect(&settings).await?;

            let namespace = "test_b9d581eaa7804d15a96cb95b5a081cf5";
            let table_name = "data_transfer_sessions";

            let exists = catalog.table_exists(namespace, table_name).await?;

            println!("table exists :: {exists}");

            let table = IcebergTable::from_catalog(catalog, namespace, table_name).await?;

            let dts = (0..500)
                .map(mk_dt)
                .map(TrinoDataTransferSession::from)
                .collect::<Vec<_>>();

            use helium_iceberg::DataWriter;
            let res = table.write(dts).await?;
            println!("!!! {res:?}");

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
