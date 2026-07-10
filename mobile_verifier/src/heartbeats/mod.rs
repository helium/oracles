pub mod last_location;
pub mod wifi;

use crate::{cell_type::CellType, geofence::GeofenceValidator, GatewayResolution, GatewayResolver};
use anyhow::Context;
use chrono::{DateTime, Duration, DurationRound, RoundingError, Utc};
use file_store::file_sink::FileSinkClient;
use file_store_oracles::wifi_heartbeat::WifiHeartbeatIngestReport;
use futures::stream::{Stream, StreamExt};
use h3o::{CellIndex, LatLng};
use helium_crypto::PublicKeyBinary;
use helium_proto::services::poc_mobile::{self as proto, LocationSource};
use mobile_config::gateway::service::info::DeviceType;
use retainer::Cache;
use rust_decimal::{prelude::ToPrimitive, Decimal};
use rust_decimal_macros::dec;
use sqlx::PgTransaction;
use std::{ops::Range, pin::pin, time};
use uuid::Uuid;

use self::last_location::{LastLocation, LocationCache};

#[derive(Clone)]
pub struct Heartbeat {
    pub hotspot_key: PublicKeyBinary,
    pub operation_mode: bool,
    pub lat: f64,
    pub lon: f64,
    pub coverage_object: Option<Uuid>,
    pub location_validation_timestamp: Option<DateTime<Utc>>,
    pub location_source: LocationSource,
    pub timestamp: DateTime<Utc>,
    pub heartbeat_timestamp: DateTime<Utc>,
}

impl Heartbeat {
    pub fn truncated_timestamp(&self) -> Result<DateTime<Utc>, RoundingError> {
        self.timestamp.duration_trunc(Duration::hours(1))
    }

    pub fn key(&self) -> &PublicKeyBinary {
        &self.hotspot_key
    }

    pub fn id(&self) -> anyhow::Result<(String, DateTime<Utc>)> {
        let ts = self.truncated_timestamp()?;
        Ok((self.hotspot_key.to_string(), ts))
    }

    fn centered_latlng(&self) -> anyhow::Result<LatLng> {
        Ok(LatLng::new(self.lat, self.lon)?
            .to_cell(h3o::Resolution::Twelve)
            .into())
    }
}

impl From<WifiHeartbeatIngestReport> for Heartbeat {
    fn from(value: WifiHeartbeatIngestReport) -> Self {
        let received_timestamp = value.received_timestamp;
        let heartbeat_timestamp = value.report.timestamp;
        let location_validation_timestamp = value
            .report
            .location_validation_timestamp
            .filter(|ts| received_timestamp.signed_duration_since(ts) <= Duration::hours(24));
        Self {
            coverage_object: value.report.coverage_object(),
            hotspot_key: value.report.pubkey,
            operation_mode: value.report.operation_mode,
            lat: value.report.lat,
            lon: value.report.lon,
            location_validation_timestamp,
            location_source: value.report.location_source,
            timestamp: received_timestamp,
            heartbeat_timestamp,
        }
    }
}

#[derive(Clone)]
pub struct ValidatedHeartbeat {
    pub heartbeat: Heartbeat,
    pub cell_type: CellType,
    pub location_trust_score_multiplier: Decimal,
    pub distance_to_asserted: Option<i64>,
    pub asserted_location: Option<u64>,
    pub device_type: Option<DeviceType>,
    pub validity: proto::HeartbeatValidity,
}

impl ValidatedHeartbeat {
    pub fn is_valid(&self) -> bool {
        self.validity == proto::HeartbeatValidity::Valid
    }

    pub fn truncated_timestamp(&self) -> Result<DateTime<Utc>, RoundingError> {
        self.heartbeat.timestamp.duration_trunc(Duration::hours(1))
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new(
        heartbeat: Heartbeat,
        cell_type: CellType,
        location_trust_score_multiplier: Decimal,
        distance_to_asserted: Option<i64>,
        asserted_location: Option<u64>,
        device_type: Option<DeviceType>,
        validity: proto::HeartbeatValidity,
    ) -> Self {
        Self {
            heartbeat,
            cell_type,
            location_trust_score_multiplier,
            distance_to_asserted,
            asserted_location,
            device_type,
            validity,
        }
    }

    fn new_invalid(
        heartbeat: Heartbeat,
        cell_type: CellType,
        validity: proto::HeartbeatValidity,
    ) -> Self {
        Self {
            heartbeat,
            cell_type,
            location_trust_score_multiplier: dec!(0),
            distance_to_asserted: None,
            asserted_location: None,
            device_type: None,
            validity,
        }
    }

    /// Validate a heartbeat in the given epoch.
    pub async fn validate(
        mut heartbeat: Heartbeat,
        gateway_info_resolver: &impl GatewayResolver,
        last_location_cache: &LocationCache,
        epoch: &Range<DateTime<Utc>>,
        geofence: &impl GeofenceValidator,
    ) -> anyhow::Result<Self> {
        // The gateway's device type (from mobile-config) is the source of the
        // cell/radio type; heartbeats are no longer validated against a coverage
        // object.
        //
        let gw_info = gateway_info_resolver
            .resolve_gateway(&heartbeat.hotspot_key, &heartbeat.timestamp)
            .await?;

        let (asserted_location, device_type) = match gw_info {
            GatewayResolution::AssertedLocation(location, device_type) => (location, device_type),
            GatewayResolution::GatewayNotFound => {
                return Ok(Self::new_invalid(
                    heartbeat,
                    CellType::CellTypeNone,
                    proto::HeartbeatValidity::GatewayNotFound,
                ))
            }
            GatewayResolution::GatewayNotAsserted => {
                return Ok(Self::new_invalid(
                    heartbeat,
                    CellType::CellTypeNone,
                    proto::HeartbeatValidity::GatewayNotAsserted,
                ))
            }
            GatewayResolution::DataOnly => {
                return Ok(Self::new_invalid(
                    heartbeat,
                    CellType::CellTypeNone,
                    proto::HeartbeatValidity::InvalidDeviceType,
                ))
            }
        };

        let (cell_type, radio_type) = match device_type {
            DeviceType::WifiIndoor => (
                CellType::NovaGenericWifiIndoor,
                coverage_point_calculator::RadioType::IndoorWifi,
            ),
            DeviceType::WifiOutdoor => (
                CellType::NovaGenericWifiOutdoor,
                coverage_point_calculator::RadioType::OutdoorWifi,
            ),
            // Data-only gateways are rejected above; CBRS is deprecated.
            DeviceType::WifiDataOnly | DeviceType::Cbrs => {
                return Ok(Self::new_invalid(
                    heartbeat,
                    CellType::CellTypeNone,
                    proto::HeartbeatValidity::InvalidDeviceType,
                ))
            }
        };

        if !heartbeat.operation_mode {
            return Ok(Self::new_invalid(
                heartbeat,
                cell_type,
                proto::HeartbeatValidity::NotOperational,
            ));
        }

        if !epoch.contains(&heartbeat.timestamp) {
            return Ok(Self::new_invalid(
                heartbeat,
                cell_type,
                proto::HeartbeatValidity::HeartbeatOutsideRange,
            ));
        }

        let Ok(mut hb_latlng) = heartbeat.centered_latlng() else {
            return Ok(Self::new_invalid(
                heartbeat,
                cell_type,
                proto::HeartbeatValidity::InvalidLatLon,
            ));
        };

        if !geofence.in_valid_region(&heartbeat) {
            return Ok(Self::new_invalid(
                heartbeat,
                cell_type,
                proto::HeartbeatValidity::UnsupportedLocation,
            ));
        }

        let asserted_latlng: LatLng = CellIndex::try_from(asserted_location)?.into();
        let is_valid = match heartbeat.location_validation_timestamp {
            None => {
                if let Some(last_location) = last_location_cache
                    .get(&heartbeat.hotspot_key, heartbeat.timestamp)
                    .await?
                {
                    heartbeat.lat = last_location.lat;
                    heartbeat.lon = last_location.lon;
                    heartbeat.location_validation_timestamp =
                        Some(last_location.location_validation_timestamp);
                    // Can't panic, previous lat and lon must be valid.
                    hb_latlng = heartbeat.centered_latlng().unwrap();
                    true
                } else {
                    false
                }
            }
            Some(validation_ts) => {
                last_location_cache
                    .set(
                        &heartbeat.hotspot_key,
                        LastLocation::from_heartbeat(&heartbeat, validation_ts),
                    )
                    .await;
                true
            }
        };

        let distance_to_asserted = asserted_latlng.distance_m(hb_latlng).round() as i64;

        let location_trust_score_multiplier = if !is_valid {
            dec!(0)
        } else {
            // HIP-119 maximum asserted distance check
            coverage_point_calculator::asserted_distance_to_trust_multiplier(
                radio_type,
                distance_to_asserted as u32,
            )
        };

        Ok(Self::new(
            heartbeat,
            cell_type,
            location_trust_score_multiplier,
            Some(distance_to_asserted),
            Some(asserted_location),
            Some(device_type),
            proto::HeartbeatValidity::Valid,
        ))
    }

    pub fn validate_heartbeats<'a>(
        heartbeats: impl Stream<Item = Heartbeat> + 'a,
        gateway_info_resolver: &'a impl GatewayResolver,
        last_location_cache: &'a LocationCache,
        epoch: &'a Range<DateTime<Utc>>,
        geofence: &'a impl GeofenceValidator,
    ) -> impl Stream<Item = anyhow::Result<Self>> + 'a {
        heartbeats.then(move |heartbeat| async move {
            Self::validate(
                heartbeat,
                gateway_info_resolver,
                last_location_cache,
                epoch,
                geofence,
            )
            .await
        })
    }

    pub async fn write(&self, heartbeats: &FileSinkClient<proto::Heartbeat>) -> file_store::Result {
        heartbeats
            .write(
                proto::Heartbeat {
                    cbsd_id: String::default(),
                    pub_key: self.heartbeat.hotspot_key.as_ref().into(),
                    cell_type: self.cell_type as i32,
                    validity: self.validity as i32,
                    timestamp: self.heartbeat.timestamp.timestamp() as u64,
                    location_trust_score_multiplier: (self.location_trust_score_multiplier
                        * dec!(1000))
                    .to_u32()
                    .unwrap_or_default(),
                    coverage_object: self
                        .heartbeat
                        .coverage_object
                        .map(|x| Vec::from(x.into_bytes()))
                        .unwrap_or_default(),
                    lat: self.heartbeat.lat,
                    lon: self.heartbeat.lon,
                    location_validation_timestamp: self
                        .heartbeat
                        .location_validation_timestamp
                        .map_or(0, |v| v.timestamp() as u64),
                    distance_to_asserted: self.distance_to_asserted.map_or(0, |v| v as u64),
                    location_source: self.heartbeat.location_source.into(),
                    ..Default::default()
                },
                &[("validity", self.validity.as_str_name())],
            )
            .await?;
        Ok(())
    }

    pub async fn save(self, exec: &mut PgTransaction<'_>) -> anyhow::Result<()> {
        let truncated_timestamp = self.truncated_timestamp()?;
        sqlx::query(
            r#"
            INSERT INTO wifi_heartbeats (hotspot_key, cell_type, first_timestamp, truncated_timestamp, coverage_object, location_trust_score_multiplier, distance_to_asserted, location_validation_timestamp, lat, lon)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            ON CONFLICT (hotspot_key, truncated_timestamp) DO UPDATE SET
            first_timestamp = EXCLUDED.first_timestamp,
            coverage_object = EXCLUDED.coverage_object
            "#,
        )
        .bind(self.heartbeat.hotspot_key)
        .bind(self.cell_type)
        .bind(self.heartbeat.timestamp)
        .bind(truncated_timestamp)
        .bind(self.heartbeat.coverage_object)
        .bind(self.location_trust_score_multiplier)
        .bind(self.distance_to_asserted)
        .bind(self.heartbeat.location_validation_timestamp)
        .bind(self.heartbeat.lat)
        .bind(self.heartbeat.lon)
        .execute(&mut **exec)
        .await?;
        Ok(())
    }
}

pub(crate) async fn process_validated_heartbeats(
    validated_heartbeats: impl Stream<Item = anyhow::Result<ValidatedHeartbeat>>,
    heartbeat_cache: &Cache<(String, DateTime<Utc>), ()>,
    heartbeat_sink: &FileSinkClient<proto::Heartbeat>,
    transaction: &mut PgTransaction<'_>,
    iceberg_ctx: Option<(&crate::iceberg::HeartbeatWriter, &str)>,
) -> anyhow::Result<()> {
    let mut iceberg_records = Vec::new();
    let mut invalid_iceberg_records = Vec::new();
    let mut validated_heartbeats = pin!(validated_heartbeats);
    while let Some(validated_heartbeat) = validated_heartbeats.next().await.transpose()? {
        validated_heartbeat.write(heartbeat_sink).await?;

        if iceberg_ctx.is_some() {
            let record = crate::iceberg::IcebergHeartbeat::from(&validated_heartbeat);
            if validated_heartbeat.is_valid() {
                iceberg_records.push(record);
            } else {
                invalid_iceberg_records.push(crate::iceberg::IcebergInvalidHeartbeat::new(
                    record,
                    validated_heartbeat.validity,
                ));
            }
        }

        if !validated_heartbeat.is_valid() {
            continue;
        }

        let key = validated_heartbeat.heartbeat.id()?;
        if heartbeat_cache.get(&key).await.is_none() {
            validated_heartbeat.save(&mut *transaction).await?;
            heartbeat_cache
                .insert(key, (), time::Duration::from_secs(60 * 60 * 2))
                .await;
        }
    }

    if let Some((writer, id)) = iceberg_ctx {
        writer
            .write(id, iceberg_records, invalid_iceberg_records)
            .await
            .context("writing heartbeats idempotently")?;
    }

    Ok(())
}

pub async fn clear_heartbeats(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    timestamp: &DateTime<Utc>,
) -> Result<(), sqlx::Error> {
    sqlx::query("DELETE FROM wifi_heartbeats WHERE truncated_timestamp < $1;")
        .bind(timestamp)
        .execute(&mut **tx)
        .await?;

    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;
    use file_store_oracles::wifi_heartbeat::WifiHeartbeat;

    #[test]
    fn wifi_heartbeat_cannot_contain_validation_timestamp() {
        fn make_report(
            received: DateTime<Utc>,
            valid_at: Option<DateTime<Utc>>,
        ) -> WifiHeartbeatIngestReport {
            WifiHeartbeatIngestReport {
                received_timestamp: received,
                report: WifiHeartbeat {
                    pubkey: PublicKeyBinary::from(vec![0]),
                    lat: 0.0,
                    lon: 0.0,
                    operation_mode: true,
                    location_validation_timestamp: valid_at,
                    coverage_object: vec![],
                    timestamp: Utc::now(),
                    location_source: LocationSource::Skyhook,
                },
            }
        }

        let received = Utc::now();

        // validation timestamp is invalid after 24 hours
        let valid_at = received - Duration::hours(25);
        let hb = Heartbeat::from(make_report(received, Some(valid_at)));
        assert_eq!(None, hb.location_validation_timestamp);

        // 24 hours is inclusive
        let valid_at = received - Duration::hours(24);
        let hb = Heartbeat::from(make_report(received, Some(valid_at)));
        assert_eq!(Some(valid_at), hb.location_validation_timestamp);

        // sanity check
        let valid_at = received - Duration::hours(0);
        let hb = Heartbeat::from(make_report(received, Some(valid_at)));
        assert_eq!(Some(valid_at), hb.location_validation_timestamp);

        let hb = Heartbeat::from(make_report(received, None));
        assert_eq!(None, hb.location_validation_timestamp);
    }
}
