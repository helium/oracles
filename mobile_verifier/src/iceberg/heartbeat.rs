use chrono::{DateTime, FixedOffset};
use helium_iceberg::{FieldDefinition, PartitionDefinition, SortFieldDefinition, TableDefinition};
use rust_decimal::prelude::ToPrimitive;
use serde::{Deserialize, Serialize};
use trino_rust_client::Trino;

use mobile_config::gateway::service::info::DeviceType;

use crate::heartbeats::ValidatedHeartbeat;

pub use super::NAMESPACE;
pub const TABLE_NAME: &str = "heartbeats";

#[derive(Debug, Clone, Trino, Serialize, Deserialize, PartialEq)]
pub struct IcebergHeartbeat {
    hotspot_pubkey: String,
    received_timestamp: DateTime<FixedOffset>,
    heartbeat_timestamp: DateTime<FixedOffset>,
    device_type: Option<String>,
    lat: f64,
    lon: f64,
    coverage_object: String,
    location_validation_timestamp: Option<DateTime<FixedOffset>>,
    distance_to_asserted: Option<i64>,
    asserted_location: Option<String>,
    location_trust_score_multiplier: f64,
    location_source: String,
}

pub fn table_definition() -> helium_iceberg::Result<TableDefinition> {
    TableDefinition::builder(NAMESPACE, TABLE_NAME)
        .with_fields([
            FieldDefinition::required_string("hotspot_pubkey"),
            FieldDefinition::required_timestamptz("received_timestamp"),
            FieldDefinition::required_timestamptz("heartbeat_timestamp"),
            FieldDefinition::optional_string("device_type"),
            FieldDefinition::required_double("lat"),
            FieldDefinition::required_double("lon"),
            FieldDefinition::required_string("coverage_object"),
            FieldDefinition::optional_timestamptz("location_validation_timestamp"),
            FieldDefinition::optional_long("distance_to_asserted"),
            FieldDefinition::optional_string("asserted_location"),
            FieldDefinition::required_double("location_trust_score_multiplier"),
            FieldDefinition::required_string("location_source"),
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

pub async fn get_all(trino: &trino_rust_client::Client) -> anyhow::Result<Vec<IcebergHeartbeat>> {
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

impl From<&ValidatedHeartbeat> for IcebergHeartbeat {
    fn from(value: &ValidatedHeartbeat) -> Self {
        Self {
            hotspot_pubkey: value.heartbeat.hotspot_key.to_string(),
            received_timestamp: value.heartbeat.timestamp.into(),
            heartbeat_timestamp: value.heartbeat.heartbeat_timestamp.into(),
            device_type: value
                .device_type
                .map(|dt| device_type_string(dt).to_string()),
            lat: value.heartbeat.lat,
            lon: value.heartbeat.lon,
            coverage_object: value
                .heartbeat
                .coverage_object
                .map(|uuid| uuid.to_string())
                .unwrap_or_default(),
            location_validation_timestamp: value
                .heartbeat
                .location_validation_timestamp
                .map(Into::into),
            distance_to_asserted: value.distance_to_asserted,
            asserted_location: value.asserted_location.map(|loc| format!("{loc:x}")),
            location_trust_score_multiplier: value
                .location_trust_score_multiplier
                .to_f64()
                .unwrap_or_default(),
            location_source: value.heartbeat.location_source.as_str_name().to_string(),
        }
    }
}

fn device_type_string(device_type: DeviceType) -> &'static str {
    match device_type {
        DeviceType::Cbrs => "cbrs",
        DeviceType::WifiIndoor => "wifi_indoor",
        DeviceType::WifiOutdoor => "wifi_outdoor",
        DeviceType::WifiDataOnly => "wifi_data_only",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        cell_type::CellType,
        heartbeats::{HbType, Heartbeat},
    };
    use chrono::Utc;
    use helium_crypto::PublicKeyBinary;
    use helium_proto::services::poc_mobile::{HeartbeatValidity, LocationSource};
    use rust_decimal_macros::dec;
    use uuid::Uuid;

    fn make_validated_heartbeat(
        coverage_object: Option<Uuid>,
        location_validation_timestamp: Option<DateTime<chrono::Utc>>,
        distance_to_asserted: Option<i64>,
        device_type: Option<DeviceType>,
        trust_score: rust_decimal::Decimal,
    ) -> ValidatedHeartbeat {
        let now = Utc::now();
        ValidatedHeartbeat {
            heartbeat: Heartbeat {
                hb_type: HbType::Wifi,
                hotspot_key: PublicKeyBinary::from(vec![1, 2, 3]),
                operation_mode: true,
                lat: 37.7749,
                lon: -122.4194,
                coverage_object,
                location_validation_timestamp,
                location_source: LocationSource::Skyhook,
                timestamp: now,
                heartbeat_timestamp: now - chrono::Duration::seconds(30),
            },
            cell_type: CellType::NovaGenericWifiIndoor,
            location_trust_score_multiplier: trust_score,
            distance_to_asserted,
            asserted_location: None,
            device_type,
            coverage_meta: None,
            validity: HeartbeatValidity::Valid,
        }
    }

    #[test]
    fn table_definition_builds_successfully() {
        let def = table_definition().expect("table definition should build");
        assert_eq!(def.name(), TABLE_NAME);
        assert_eq!(def.namespace(), NAMESPACE);
    }

    #[test]
    fn convert_validated_heartbeat_with_all_fields() {
        let coverage_uuid = Uuid::new_v4();
        let validation_ts = Utc::now() - chrono::Duration::hours(1);
        let vhb = make_validated_heartbeat(
            Some(coverage_uuid),
            Some(validation_ts),
            Some(500),
            Some(DeviceType::WifiIndoor),
            dec!(0.75),
        );

        let iceberg: IcebergHeartbeat = (&vhb).into();

        assert_eq!(
            iceberg.hotspot_pubkey,
            vhb.heartbeat.hotspot_key.to_string()
        );
        assert_eq!(iceberg.lat, 37.7749);
        assert_eq!(iceberg.lon, -122.4194);
        assert_eq!(iceberg.device_type, Some("wifi_indoor".to_string()));
        assert_eq!(iceberg.coverage_object, coverage_uuid.to_string());
        assert!(iceberg.location_validation_timestamp.is_some());
        assert_eq!(iceberg.distance_to_asserted, Some(500));
        assert_eq!(iceberg.asserted_location, None);
        assert!((iceberg.location_trust_score_multiplier - 0.75).abs() < f64::EPSILON);
        assert_eq!(iceberg.location_source, "skyhook");
        // heartbeat_timestamp should differ from received_timestamp
        assert_ne!(iceberg.heartbeat_timestamp, iceberg.received_timestamp);
    }

    #[test]
    fn convert_validated_heartbeat_with_no_optional_fields() {
        let vhb = make_validated_heartbeat(None, None, None, None, dec!(1.0));

        let iceberg: IcebergHeartbeat = (&vhb).into();

        assert_eq!(iceberg.coverage_object, "");
        assert!(iceberg.location_validation_timestamp.is_none());
        assert_eq!(iceberg.distance_to_asserted, None);
        assert_eq!(iceberg.device_type, None);
        assert!((iceberg.location_trust_score_multiplier - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn asserted_location_converts_to_hex_string() {
        let mut vhb = make_validated_heartbeat(
            Some(Uuid::new_v4()),
            None,
            None,
            Some(DeviceType::WifiIndoor),
            dec!(1.0),
        );
        vhb.asserted_location = Some(0x8a1fb466d2dffff);

        let iceberg: IcebergHeartbeat = (&vhb).into();
        assert_eq!(
            iceberg.asserted_location,
            Some("8a1fb466d2dffff".to_string())
        );
    }

    #[test]
    fn device_type_string_covers_all_variants() {
        assert_eq!(device_type_string(DeviceType::Cbrs), "cbrs");
        assert_eq!(device_type_string(DeviceType::WifiIndoor), "wifi_indoor");
        assert_eq!(device_type_string(DeviceType::WifiOutdoor), "wifi_outdoor");
        assert_eq!(
            device_type_string(DeviceType::WifiDataOnly),
            "wifi_data_only"
        );
    }
}
