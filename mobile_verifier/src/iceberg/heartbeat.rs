use chrono::{DateTime, FixedOffset};
use helium_iceberg::{FieldDefinition, PartitionDefinition, SortFieldDefinition, TableDefinition};
use helium_proto::services::poc_mobile::CellType as ProtoCellType;
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

pub async fn get_all(trino: &trino_client::Client) -> anyhow::Result<Vec<IcebergHeartbeat>> {
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

/// Build an `IcebergHeartbeat` directly from the on-wire validated-heartbeat
/// proto used for backfill. The wire format drops `heartbeat_timestamp` and
/// `asserted_location`, so those collapse to `received_timestamp` / `None`.
/// `device_type` is recovered from `cell_type` — the wire enum has no
/// `WifiDataOnly` variant, but data-only hotspots don't heartbeat so the
/// mapping is lossless for this stream.
impl From<file_store_oracles::validated_heartbeat::ValidatedHeartbeat> for IcebergHeartbeat {
    fn from(value: file_store_oracles::validated_heartbeat::ValidatedHeartbeat) -> Self {
        let received_timestamp = value.received_timestamp.into();
        let coverage_object = uuid::Uuid::from_slice(&value.coverage_object)
            .map(|u| u.to_string())
            .unwrap_or_default();
        let device_type =
            cell_type_to_device_type(value.cell_type).map(|dt| device_type_string(dt).to_string());
        Self {
            hotspot_pubkey: value.pubkey.to_string(),
            received_timestamp,
            heartbeat_timestamp: received_timestamp,
            device_type,
            lat: value.lat,
            lon: value.lon,
            coverage_object,
            location_validation_timestamp: value.location_validation_timestamp.map(Into::into),
            distance_to_asserted: Some(value.distance_to_asserted as i64),
            asserted_location: None,
            location_trust_score_multiplier: value
                .location_trust_score_multiplier
                .to_f64()
                .unwrap_or_default(),
            location_source: value.location_source.as_str_name().to_string(),
        }
    }
}

/// Recover the hotspot's `DeviceType` from the wire `cell_type` field for
/// backfill rows. `WifiDataOnly` is unreachable here — the wire enum doesn't
/// carry it and those hotspots don't emit heartbeats anyway.
fn cell_type_to_device_type(cell_type: ProtoCellType) -> Option<DeviceType> {
    match cell_type {
        ProtoCellType::Nova436h
        | ProtoCellType::Nova430i
        | ProtoCellType::Neutrino430
        | ProtoCellType::SercommIndoor
        | ProtoCellType::SercommOutdoor => Some(DeviceType::Cbrs),
        ProtoCellType::NovaGenericWifiIndoor => Some(DeviceType::WifiIndoor),
        ProtoCellType::NovaGenericWifiOutdoor => Some(DeviceType::WifiOutdoor),
        ProtoCellType::None => None,
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
    fn from_wire_validated_heartbeat_collapses_lost_fields() {
        use file_store_oracles::validated_heartbeat::ValidatedHeartbeat as WireHeartbeat;
        use helium_proto::services::poc_mobile::{
            CellType as ProtoCellType, HeartbeatValidity, LocationSource,
        };

        let received = Utc::now();
        let coverage_uuid = Uuid::new_v4();
        let validation_ts = received - chrono::Duration::minutes(5);

        let wire = WireHeartbeat {
            pubkey: PublicKeyBinary::from(vec![9, 9, 9]),
            cbsd_id: String::new(),
            cell_type: ProtoCellType::NovaGenericWifiIndoor,
            validity: HeartbeatValidity::Valid,
            received_timestamp: received,
            lat: 1.5,
            lon: -2.5,
            coverage_object: coverage_uuid.as_bytes().to_vec(),
            location_validation_timestamp: Some(validation_ts),
            distance_to_asserted: 123,
            location_trust_score_multiplier: rust_decimal::Decimal::new(500, 3),
            location_source: LocationSource::Gps,
        };

        let row: IcebergHeartbeat = wire.into();

        // Fields preserved from the wire.
        assert_eq!(
            row.hotspot_pubkey,
            PublicKeyBinary::from(vec![9, 9, 9]).to_string()
        );
        assert_eq!(row.lat, 1.5);
        assert_eq!(row.lon, -2.5);
        assert_eq!(row.coverage_object, coverage_uuid.to_string());
        assert_eq!(row.distance_to_asserted, Some(123));
        assert!((row.location_trust_score_multiplier - 0.5).abs() < f64::EPSILON);
        assert_eq!(row.location_source, "gps");
        assert!(row.location_validation_timestamp.is_some());
        // `device_type` is recovered from the wire `cell_type`.
        assert_eq!(row.device_type, Some("wifi_indoor".to_string()));

        // Fields lost on the wire collapse to received_timestamp / None.
        assert_eq!(row.heartbeat_timestamp, row.received_timestamp);
        assert!(row.asserted_location.is_none());
    }

    #[test]
    fn from_wire_validated_heartbeat_empty_coverage_yields_empty_string() {
        use file_store_oracles::validated_heartbeat::ValidatedHeartbeat as WireHeartbeat;
        use helium_proto::services::poc_mobile::{
            CellType as ProtoCellType, HeartbeatValidity, LocationSource,
        };

        let wire = WireHeartbeat {
            pubkey: PublicKeyBinary::from(vec![1]),
            cbsd_id: String::new(),
            cell_type: ProtoCellType::NovaGenericWifiIndoor,
            validity: HeartbeatValidity::Valid,
            received_timestamp: Utc::now(),
            lat: 0.0,
            lon: 0.0,
            coverage_object: vec![],
            location_validation_timestamp: None,
            distance_to_asserted: 0,
            location_trust_score_multiplier: rust_decimal::Decimal::new(1000, 3),
            location_source: LocationSource::Unknown,
        };

        let row: IcebergHeartbeat = wire.into();
        assert_eq!(row.coverage_object, "");
        assert!(row.location_validation_timestamp.is_none());
        assert_eq!(row.distance_to_asserted, Some(0));
        assert!((row.location_trust_score_multiplier - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn cell_type_to_device_type_covers_wire_variants() {
        use helium_proto::services::poc_mobile::CellType as Proto;
        assert_eq!(
            cell_type_to_device_type(Proto::Nova436h),
            Some(DeviceType::Cbrs)
        );
        assert_eq!(
            cell_type_to_device_type(Proto::Nova430i),
            Some(DeviceType::Cbrs)
        );
        assert_eq!(
            cell_type_to_device_type(Proto::Neutrino430),
            Some(DeviceType::Cbrs)
        );
        assert_eq!(
            cell_type_to_device_type(Proto::SercommIndoor),
            Some(DeviceType::Cbrs)
        );
        assert_eq!(
            cell_type_to_device_type(Proto::SercommOutdoor),
            Some(DeviceType::Cbrs)
        );
        assert_eq!(
            cell_type_to_device_type(Proto::NovaGenericWifiIndoor),
            Some(DeviceType::WifiIndoor)
        );
        assert_eq!(
            cell_type_to_device_type(Proto::NovaGenericWifiOutdoor),
            Some(DeviceType::WifiOutdoor)
        );
        assert_eq!(cell_type_to_device_type(Proto::None), None);
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
