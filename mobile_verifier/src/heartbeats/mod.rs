pub mod last_location;
pub mod wifi;

use crate::{
    cell_type::CellType, coverage::CoverageObjectMeta, geofence::GeofenceValidator,
    GatewayResolution, GatewayResolver,
};
use anyhow::{anyhow, Context};
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

/// Minimum number of heartbeats required to give a reward to the hotspot.
pub const MINIMUM_HEARTBEAT_COUNT: i64 = 12;

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

#[derive(Debug, Clone, PartialEq, sqlx::FromRow)]
pub struct HeartbeatReward {
    pub hotspot_key: PublicKeyBinary,
    pub distances_to_asserted: Vec<i64>,
    pub trust_score_multipliers: Vec<Decimal>,
    pub coverage_object: Uuid,
}

impl HeartbeatReward {
    pub fn key(&self) -> &PublicKeyBinary {
        &self.hotspot_key
    }

    pub fn id(&self) -> String {
        self.hotspot_key.to_string()
    }

    pub fn validated<'a>(
        exec: impl sqlx::PgExecutor<'a> + Copy + 'a,
        epoch: &'a Range<DateTime<Utc>>,
    ) -> impl Stream<Item = Result<HeartbeatReward, sqlx::Error>> + 'a {
        sqlx::query_as::<_, HeartbeatReward>(include_str!("valid_radios.sql"))
            .bind(epoch.start)
            .bind(epoch.end)
            .bind(MINIMUM_HEARTBEAT_COUNT)
            .fetch(exec)
    }

    pub fn iter_distances_and_scores(
        &self,
    ) -> anyhow::Result<impl Iterator<Item = (i64, Decimal)>> {
        // This should never happen if valid_radio.sql is not touched
        if self.trust_score_multipliers.len() != self.distances_to_asserted.len() {
            return Err(anyhow!(
                "Mismatched lengths between distances_to_asserted and trust_score_multipliers"
            ));
        }

        Ok(self
            .distances_to_asserted
            .clone()
            .into_iter()
            .zip(self.trust_score_multipliers.clone()))
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
    pub coverage_meta: Option<CoverageObjectMeta>,
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
            coverage_meta: None,
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
            coverage_meta: None,
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
    use crate::seniority::{Seniority, SeniorityUpdate, SeniorityUpdateAction};

    use super::*;
    use file_store_oracles::wifi_heartbeat::WifiHeartbeat;
    use proto::SeniorityUpdateReason::*;

    #[test]
    fn test_iter_distances_and_scores_with_matching_lengths() {
        let reward = HeartbeatReward {
            hotspot_key: PublicKeyBinary::from(vec![1, 2, 3]),
            distances_to_asserted: vec![10, 20, 30],
            trust_score_multipliers: vec![dec!(0.25), dec!(0.5), dec!(1.0)],
            coverage_object: uuid::Uuid::new_v4(),
        };

        let result = reward.iter_distances_and_scores();
        assert!(result.is_ok(), "Expected successful iteration");

        let pairs: Vec<(i64, Decimal)> = result.unwrap().collect();
        assert_eq!(pairs.len(), 3, "Expected 3 pairs in the iterator");
        assert_eq!(pairs[0], (10, dec!(0.25)));
        assert_eq!(pairs[1], (20, dec!(0.5)));
        assert_eq!(pairs[2], (30, dec!(1.0)));
    }

    #[test]
    fn test_iter_distances_and_scores_with_mismatched_lengths() {
        let reward = HeartbeatReward {
            hotspot_key: PublicKeyBinary::from(vec![1, 2, 3]),
            distances_to_asserted: vec![10, 20], // Only 2 elements
            trust_score_multipliers: vec![dec!(0.25), dec!(0.5), dec!(1.0)], // 3 elements
            coverage_object: uuid::Uuid::new_v4(),
        };

        let result = reward.iter_distances_and_scores();
        assert!(result.is_err(), "Expected error due to mismatched lengths");
    }

    fn heartbeat(timestamp: DateTime<Utc>, coverage_object: Uuid) -> ValidatedHeartbeat {
        ValidatedHeartbeat {
            cell_type: CellType::CellTypeNone,
            heartbeat: Heartbeat {
                hotspot_key: PublicKeyBinary::from(Vec::new()),
                timestamp,
                lon: 0.0,
                lat: 0.0,
                operation_mode: false,
                coverage_object: Some(coverage_object),
                location_validation_timestamp: None,
                location_source: LocationSource::Skyhook,
                heartbeat_timestamp: timestamp,
            },
            validity: Default::default(),
            location_trust_score_multiplier: dec!(1.0),
            distance_to_asserted: None,
            asserted_location: None,
            device_type: None,
            coverage_meta: None,
        }
    }

    #[test]
    fn ensure_first_seniority_causes_update() -> anyhow::Result<()> {
        let coverage_claim_time: DateTime<Utc> =
            "2023-08-22 00:00:00.000000000 UTC".parse().unwrap();
        let coverage_object = Uuid::new_v4();

        let received_timestamp: DateTime<Utc> =
            "2023-08-23 00:00:00.000000000 UTC".parse().unwrap();
        let new_heartbeat = heartbeat(received_timestamp, coverage_object);
        let seniority_action =
            SeniorityUpdate::determine_update_action(&new_heartbeat, coverage_claim_time, None)?;

        assert_eq!(
            seniority_action.action,
            SeniorityUpdateAction::Insert {
                new_seniority: coverage_claim_time,
                update_reason: NewCoverageClaimTime,
            }
        );
        Ok(())
    }

    #[test]
    fn ensure_seniority_updates_on_new_coverage_object() -> anyhow::Result<()> {
        let coverage_claim_time: DateTime<Utc> =
            "2023-08-22 00:00:00.000000000 UTC".parse().unwrap();
        let coverage_object = Uuid::new_v4();
        let latest_seniority = Seniority {
            uuid: coverage_object,
            seniority_ts: coverage_claim_time,
            last_heartbeat: coverage_claim_time,
            inserted_at: coverage_claim_time,
            update_reason: NewCoverageClaimTime as i32,
        };

        let new_coverage_object = Uuid::new_v4();
        let new_coverage_claim_time = "2023-08-25 00:00:00.000000000 UTC".parse().unwrap();
        let received_timestamp: DateTime<Utc> =
            "2023-08-23 00:00:00.000000000 UTC".parse().unwrap();
        let new_heartbeat = heartbeat(received_timestamp, new_coverage_object);
        let seniority_action = SeniorityUpdate::determine_update_action(
            &new_heartbeat,
            new_coverage_claim_time,
            Some(latest_seniority.clone()),
        )?;

        assert_eq!(
            seniority_action.action,
            SeniorityUpdateAction::Insert {
                new_seniority: new_coverage_claim_time,
                update_reason: NewCoverageClaimTime,
            }
        );
        Ok(())
    }

    #[test]
    fn ensure_last_heartbeat_updates_on_same_coverage_object() -> anyhow::Result<()> {
        let coverage_claim_time: DateTime<Utc> =
            "2023-08-22 00:00:00.000000000 UTC".parse().unwrap();
        let coverage_object = Uuid::new_v4();
        let latest_seniority = Seniority {
            uuid: coverage_object,
            seniority_ts: coverage_claim_time,
            last_heartbeat: coverage_claim_time,
            inserted_at: coverage_claim_time,
            update_reason: NewCoverageClaimTime as i32,
        };

        let received_timestamp: DateTime<Utc> =
            "2023-08-23 00:00:00.000000000 UTC".parse().unwrap();
        let new_heartbeat = heartbeat(received_timestamp, coverage_object);
        let seniority_action = SeniorityUpdate::determine_update_action(
            &new_heartbeat,
            coverage_claim_time,
            Some(latest_seniority.clone()),
        )?;

        assert_eq!(
            seniority_action.action,
            SeniorityUpdateAction::Update {
                curr_seniority: coverage_claim_time,
            }
        );
        Ok(())
    }

    #[test]
    fn ensure_seniority_updates_after_72_hours() -> anyhow::Result<()> {
        let coverage_claim_time: DateTime<Utc> =
            "2023-08-22 00:00:00.000000000 UTC".parse().unwrap();
        let last_heartbeat: DateTime<Utc> = "2023-08-23 00:00:00.000000000 UTC".parse().unwrap();
        let coverage_object = Uuid::new_v4();
        let latest_seniority = Seniority {
            uuid: coverage_object,
            seniority_ts: coverage_claim_time,
            last_heartbeat,
            inserted_at: last_heartbeat,
            update_reason: NewCoverageClaimTime as i32,
        };
        let received_timestamp = "2023-08-26 00:00:01.000000000 UTC".parse().unwrap();
        let new_heartbeat = heartbeat(received_timestamp, coverage_object);
        let seniority_action = SeniorityUpdate::determine_update_action(
            &new_heartbeat,
            coverage_claim_time,
            Some(latest_seniority),
        )?;
        assert_eq!(
            seniority_action.action,
            SeniorityUpdateAction::Insert {
                new_seniority: received_timestamp,
                update_reason: HeartbeatNotSeen,
            }
        );
        Ok(())
    }

    #[test]
    fn ensure_seniority_updates_after_not_seen_if_in_future() -> anyhow::Result<()> {
        let coverage_claim_time: DateTime<Utc> =
            "2023-08-22 00:00:00.000000000 UTC".parse().unwrap();
        let coverage_object = Uuid::new_v4();
        let latest_seniority = Seniority {
            uuid: coverage_object,
            seniority_ts: coverage_claim_time,
            last_heartbeat: coverage_claim_time,
            inserted_at: coverage_claim_time,
            update_reason: HeartbeatNotSeen as i32,
        };

        let new_coverage_object = Uuid::new_v4();
        let new_coverage_claim_time = "2023-08-25 00:00:00.000000000 UTC".parse().unwrap();
        let received_timestamp: DateTime<Utc> =
            "2023-08-23 00:00:00.000000000 UTC".parse().unwrap();
        let new_heartbeat = heartbeat(received_timestamp, new_coverage_object);
        let seniority_action = SeniorityUpdate::determine_update_action(
            &new_heartbeat,
            new_coverage_claim_time,
            Some(latest_seniority.clone()),
        )?;
        assert_eq!(
            seniority_action.action,
            SeniorityUpdateAction::Insert {
                new_seniority: new_coverage_claim_time,
                update_reason: NewCoverageClaimTime,
            }
        );

        // If the new coverage claim time is in the past, we do not want to update
        let new_coverage_claim_time = "2023-08-21 00:00:00.000000000 UTC".parse().unwrap();
        let received_timestamp: DateTime<Utc> =
            "2023-08-23 00:00:00.000000000 UTC".parse().unwrap();
        let new_heartbeat = heartbeat(received_timestamp, new_coverage_object);
        let seniority_action = SeniorityUpdate::determine_update_action(
            &new_heartbeat,
            new_coverage_claim_time,
            Some(latest_seniority),
        )?;
        assert_eq!(seniority_action.action, SeniorityUpdateAction::NoAction);
        Ok(())
    }

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
