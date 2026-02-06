// NOTE(mj): putting 6 decimals helps with PartialEq when doing a roundtrip, but may be overkill.
// Does make life a lot easier though.
//first_timestamp = self.first_timestamp.format("%Y-%m-%d %H:%M:%S%.6f"),

pub mod data_transfer_session {

    use chrono::DateTime;
    use chrono::FixedOffset;
    use file_store_oracles::mobile_session::DataTransferSessionIngestReport;
    use serde::Deserialize;
    use serde::Serialize;
    use trino_rust_client::Trino;

    const TABLE_NAME: &str = "data_transfer_sessions";
    const DT_FORMAT: &str = "%Y-%m-%d %H:%M:%S%.6f";

    #[derive(Debug, Trino, Serialize, Deserialize)]
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
}
