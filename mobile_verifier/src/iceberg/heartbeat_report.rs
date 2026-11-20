//! `hotspots.heartbeat_reports`: each valid heartbeat exactly as the hotspot
//! reported it.
//!
//! Written beside `hotspots.heartbeats` from the same validated heartbeats, but
//! with none of that table's decoration: no location carried forward from the
//! last-location cache, no 24-hour filter on the validation timestamp, no
//! distance or trust score. Location trust is derived from this table in dbt
//! instead. The one value computed here is the heartbeat's centered point, the
//! center of its resolution-12 H3 cell, because Trino has no H3 functions and
//! distance is measured from that point.
//!
//! Rejected heartbeats go to `hotspots.invalid_heartbeat_reports`
//! ([`super::invalid_heartbeat_report`]).

use chrono::{DateTime, FixedOffset};
use helium_iceberg::{FieldDefinition, PartitionDefinition, SortFieldDefinition, TableDefinition};
use serde::{Deserialize, Serialize};
use trino_rust_client::Trino;

use crate::heartbeats::ValidatedHeartbeat;

pub use super::NAMESPACE;
pub const TABLE_NAME: &str = "heartbeat_reports";

#[derive(Debug, Clone, Trino, Serialize, Deserialize, PartialEq)]
pub struct IcebergHeartbeatReport {
    pub hotspot_pubkey: String,
    pub received_timestamp: DateTime<FixedOffset>,
    pub heartbeat_timestamp: DateTime<FixedOffset>,
    pub lat: f64,
    pub lon: f64,
    pub centered_lat: Option<f64>,
    pub centered_lon: Option<f64>,
    pub location_validation_timestamp: Option<DateTime<FixedOffset>>,
    pub location_source: String,
}

pub fn table_definition() -> helium_iceberg::Result<TableDefinition> {
    TableDefinition::builder(NAMESPACE, TABLE_NAME)
        .with_fields([
            FieldDefinition::required_string("hotspot_pubkey"),
            FieldDefinition::required_timestamptz("received_timestamp"),
            FieldDefinition::required_timestamptz("heartbeat_timestamp"),
            FieldDefinition::required_double("lat"),
            FieldDefinition::required_double("lon"),
            FieldDefinition::optional_double("centered_lat"),
            FieldDefinition::optional_double("centered_lon"),
            FieldDefinition::optional_timestamptz("location_validation_timestamp"),
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

pub async fn get_all(
    trino: &trino_rust_client::Client,
) -> anyhow::Result<Vec<IcebergHeartbeatReport>> {
    let all = match trino
        .get_all(format!("SELECT * from {NAMESPACE}.{TABLE_NAME}"))
        .await
    {
        Ok(all) => all.into_vec(),
        Err(err) => return Err(err.into()),
    };
    Ok(all)
}

impl From<&ValidatedHeartbeat> for IcebergHeartbeatReport {
    /// Built from [`Heartbeat::reported`](crate::heartbeats::Heartbeat::reported),
    /// never from the heartbeat's own `lat`, `lon` or validation timestamp,
    /// which validation may have replaced.
    fn from(value: &ValidatedHeartbeat) -> Self {
        let heartbeat = &value.heartbeat;
        let reported = heartbeat.reported;
        let centered = reported.centered_latlng();
        Self {
            hotspot_pubkey: heartbeat.hotspot_key.to_string(),
            received_timestamp: heartbeat.timestamp.into(),
            heartbeat_timestamp: heartbeat.heartbeat_timestamp.into(),
            lat: reported.lat,
            lon: reported.lon,
            centered_lat: centered.map(|latlng| latlng.lat()),
            centered_lon: centered.map(|latlng| latlng.lng()),
            location_validation_timestamp: reported.location_validation_timestamp.map(Into::into),
            location_source: heartbeat.location_source.as_str_name().to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        cell_type::CellType,
        heartbeats::{Heartbeat, ReportedLocation},
    };
    use chrono::{Duration, Utc};
    use h3o::{LatLng, Resolution};
    use helium_crypto::PublicKeyBinary;
    use helium_proto::services::poc_mobile::{HeartbeatValidity, LocationSource};
    use rust_decimal_macros::dec;

    fn validated(heartbeat: Heartbeat) -> ValidatedHeartbeat {
        ValidatedHeartbeat {
            heartbeat,
            cell_type: CellType::NovaGenericWifiIndoor,
            location_trust_score_multiplier: dec!(1.0),
            distance_to_asserted: Some(0),
            asserted_location: Some(0x8c2681a3064d9ff),
            device_type: None,
            validity: HeartbeatValidity::Valid,
        }
    }

    fn heartbeat(reported: ReportedLocation) -> Heartbeat {
        let now = Utc::now();
        Heartbeat {
            hotspot_key: PublicKeyBinary::from(vec![1, 2, 3]),
            operation_mode: true,
            lat: reported.lat,
            lon: reported.lon,
            coverage_object: None,
            location_validation_timestamp: reported.location_validation_timestamp,
            location_source: LocationSource::Skyhook,
            timestamp: now,
            heartbeat_timestamp: now - Duration::seconds(30),
            reported,
        }
    }

    #[test]
    fn records_the_reported_location_not_the_carried_forward_one() {
        let reported = ReportedLocation {
            lat: 40.019,
            lon: -105.272,
            location_validation_timestamp: None,
        };
        // What validation does when the cache supplies a location: the
        // heartbeat's own fields change, `reported` does not.
        let mut hb = heartbeat(reported);
        let cached_validation = Utc::now() - Duration::hours(2);
        hb.lat = 37.0;
        hb.lon = -122.0;
        hb.location_validation_timestamp = Some(cached_validation);

        let report = IcebergHeartbeatReport::from(&validated(hb));

        assert_eq!(report.lat, 40.019);
        assert_eq!(report.lon, -105.272);
        assert_eq!(report.location_validation_timestamp, None);
    }

    #[test]
    fn keeps_a_validation_timestamp_older_than_a_day() {
        // Ingest drops this from the heartbeat's own field; the report keeps it.
        let stale = Utc::now() - Duration::hours(30);
        let mut hb = heartbeat(ReportedLocation {
            lat: 40.019,
            lon: -105.272,
            location_validation_timestamp: Some(stale),
        });
        hb.location_validation_timestamp = None;

        let report = IcebergHeartbeatReport::from(&validated(hb));

        assert_eq!(report.location_validation_timestamp, Some(stale.into()));
    }

    #[test]
    fn snaps_to_the_resolution_12_cell_center() {
        let (lat, lon) = (40.0194278140, -105.272);
        let report = IcebergHeartbeatReport::from(&validated(heartbeat(ReportedLocation {
            lat,
            lon,
            location_validation_timestamp: None,
        })));

        let center: LatLng = LatLng::new(lat, lon)
            .unwrap()
            .to_cell(Resolution::Twelve)
            .into();
        assert_eq!(report.centered_lat, Some(center.lat()));
        assert_eq!(report.centered_lon, Some(center.lng()));
        assert_ne!(report.centered_lat, Some(lat));
    }

    #[test]
    fn leaves_centered_point_empty_for_impossible_coordinates() {
        // h3o accepts any finite lat/lon; only a non-finite one cannot form a
        // cell, which is also what the verifier rejects as `invalid_lat_lon`.
        let report = IcebergHeartbeatReport::from(&validated(heartbeat(ReportedLocation {
            lat: f64::NAN,
            lon: -105.272,
            location_validation_timestamp: None,
        })));

        assert!(report.lat.is_nan());
        assert_eq!(report.centered_lat, None);
        assert_eq!(report.centered_lon, None);
    }
}
