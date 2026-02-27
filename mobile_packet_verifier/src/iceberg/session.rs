use chrono::{DateTime, FixedOffset};
use file_store_oracles::mobile_session::DataTransferSessionIngestReport;
use helium_iceberg::{FieldDefinition, PartitionDefinition, TableDefinition};
use helium_proto::services::poc_mobile::CarrierIdV2;
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

pub fn table_definition() -> helium_iceberg::Result<TableDefinition> {
    TableDefinition::builder(NAMESPACE, TABLE_NAME)
        .with_fields([
            FieldDefinition::required_timestamptz("report_received_timestamp"),
            FieldDefinition::required_string("request_pub_key"),
            FieldDefinition::required_long("rewardable_bytes"),
            FieldDefinition::required_string("carrier_id"),
            FieldDefinition::required_boolean("sampling"),
            FieldDefinition::required_string("data_transfer_event_pub_key"),
            FieldDefinition::required_long("upload_bytes"),
            FieldDefinition::required_long("download_byte"),
            FieldDefinition::required_string("radio_access_thechnology"),
            FieldDefinition::required_string("event_id"),
            FieldDefinition::required_string("payer"),
            FieldDefinition::required_timestamptz("timestamp"),
        ])
        .with_partition(PartitionDefinition::day(
            "report_received_timestamp",
            "report_received_timestamp_day",
        ))
        .wap_enabled()
        .build()
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

        Self {
            report_received_timestamp: value.received_timestamp.into(),
            request_pub_key,
            rewardable_bytes: value.report.rewardable_bytes,
            carrier_id: carrier_id_string(value.report.carrier_id),
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

// NOTE: trino doesn't support enums (yet), so we do our own mapping here.
// These strings are used to ease querying as the default str mapping for
// CarrierIdV2 is 'carrier_id_v2_carrier_x'. We don't have any cases where
// we want to map a trino result back into a proto type, so the code for
// going from these strings back into the enum hasn't been written yet. It
// might be a good candidate of functionality to upstream into helium-proto.
fn carrier_id_string(carrier_id: CarrierIdV2) -> String {
    match carrier_id {
        CarrierIdV2::Unspecified => "carrier_unspecified",
        CarrierIdV2::Carrier0 => "carrier_0",
        CarrierIdV2::Carrier1 => "carrier_1",
        CarrierIdV2::Carrier2 => "carrier_2",
        CarrierIdV2::Carrier3 => "carrier_3",
        CarrierIdV2::Carrier4 => "carrier_4",
        CarrierIdV2::Carrier5 => "carrier_5",
        CarrierIdV2::Carrier6 => "carrier_6",
        CarrierIdV2::Carrier7 => "carrier_7",
        CarrierIdV2::Carrier8 => "carrier_8",
        CarrierIdV2::Carrier9 => "carrier_9",
    }
    .to_string()
}
