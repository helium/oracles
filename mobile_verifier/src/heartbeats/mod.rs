pub mod last_location;
pub mod wifi;

use crate::{
    cell_type::CellType,
    coverage::{self, CoverageClaimTimeCache, CoverageObjectCache, CoverageObjectMeta},
    geofence::GeofenceValidator,
    seniority::{Seniority, SeniorityUpdate},
    GatewayResolution, GatewayResolver,
};
use anyhow::anyhow;
use chrono::{DateTime, Duration, DurationRound, RoundingError, Utc};
use file_store::{file_sink::FileSinkClient, wifi_heartbeat::WifiHeartbeatIngestReport};
use futures::stream::{Stream, StreamExt};
use h3o::{CellIndex, LatLng};
use helium_crypto::PublicKeyBinary;
use helium_proto::services::poc_mobile::{self as proto, LocationSource};
use retainer::Cache;
use rust_decimal::{prelude::ToPrimitive, Decimal};
use rust_decimal_macros::dec;
use sqlx::{
    postgres::{PgArgumentBuffer, PgTypeInfo, PgValueRef},
    Decode, Encode, Postgres, Transaction, Type,
};
use std::{ops::Range, pin::pin, time};
use uuid::Uuid;

use self::last_location::{LastLocation, LocationCache};

/// Minimum number of heartbeats required to give a reward to the hotspot.
const MINIMUM_HEARTBEAT_COUNT: i64 = 12;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, sqlx::Type, Debug)]
#[sqlx(type_name = "radio_type")]
#[sqlx(rename_all = "lowercase")]
pub enum HbType {
    Wifi,
}

#[derive(Debug, Copy, Clone)]
pub enum KeyType<'a> {
    Wifi(&'a PublicKeyBinary),
}

impl From<KeyType<'_>> for proto::seniority_update::KeyType {
    fn from(kt: KeyType<'_>) -> Self {
        match kt {
            KeyType::Wifi(key) => proto::seniority_update::KeyType::HotspotKey(key.clone().into()),
        }
    }
}

impl KeyType<'_> {
    pub fn to_owned(self) -> OwnedKeyType {
        match self {
            Self::Wifi(key) => OwnedKeyType::Wifi(key.to_owned()),
        }
    }

    pub fn to_id(self) -> (String, HbType) {
        match self {
            Self::Wifi(wifi) => (wifi.to_string(), HbType::Wifi),
        }
    }

    pub fn hb_type(self) -> HbType {
        match self {
            Self::Wifi(_) => HbType::Wifi,
        }
    }
}

impl<'a> From<&'a PublicKeyBinary> for KeyType<'a> {
    fn from(wifi: &'a PublicKeyBinary) -> Self {
        Self::Wifi(wifi)
    }
}

impl Type<Postgres> for KeyType<'_> {
    fn type_info() -> PgTypeInfo {
        PgTypeInfo::with_name("TEXT")
    }
}

impl<'a> Encode<'a, Postgres> for KeyType<'a> {
    fn encode_by_ref(
        &self,
        buf: &mut PgArgumentBuffer,
    ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
        match self {
            Self::Wifi(wifi) => wifi.encode_by_ref(buf),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum OwnedKeyType {
    Wifi(PublicKeyBinary),
}

impl OwnedKeyType {
    pub fn to_ref(&self) -> KeyType {
        match self {
            OwnedKeyType::Wifi(pubkey) => KeyType::Wifi(pubkey),
        }
    }
}

impl From<PublicKeyBinary> for OwnedKeyType {
    fn from(w: PublicKeyBinary) -> Self {
        Self::Wifi(w)
    }
}

impl PartialEq<KeyType<'_>> for OwnedKeyType {
    fn eq(&self, rhs: &KeyType<'_>) -> bool {
        match (self, rhs) {
            (Self::Wifi(lhs), KeyType::Wifi(rhs)) => lhs == *rhs,
        }
    }
}

impl Type<Postgres> for OwnedKeyType {
    fn type_info() -> PgTypeInfo {
        PgTypeInfo::with_name("TEXT")
    }
}

impl<'a> Encode<'a, Postgres> for OwnedKeyType {
    fn encode_by_ref(
        &self,
        buf: &mut PgArgumentBuffer,
    ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
        match self {
            Self::Wifi(wifi) => wifi.encode_by_ref(buf),
        }
    }
}

impl<'r> Decode<'r, Postgres> for OwnedKeyType {
    fn decode(value: PgValueRef<'r>) -> Result<Self, sqlx::error::BoxDynError> {
        let text = <&str as Decode<Postgres>>::decode(value)?;
        // Try decoding to a public key binary
        match text.parse() {
            Ok(pubkey) => Ok(OwnedKeyType::Wifi(pubkey)),
            Err(e) => Err(e)?,
        }
    }
}

#[derive(Clone)]
pub struct Heartbeat {
    pub hb_type: HbType,
    pub hotspot_key: PublicKeyBinary,
    pub operation_mode: bool,
    pub lat: f64,
    pub lon: f64,
    pub coverage_object: Option<Uuid>,
    pub location_validation_timestamp: Option<DateTime<Utc>>,
    pub location_source: LocationSource,
    pub timestamp: DateTime<Utc>,
}

impl Heartbeat {
    pub fn truncated_timestamp(&self) -> Result<DateTime<Utc>, RoundingError> {
        self.timestamp.duration_trunc(Duration::hours(1))
    }

    pub fn key(&self) -> KeyType<'_> {
        KeyType::from(&self.hotspot_key)
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
        let location_validation_timestamp = value
            .report
            .location_validation_timestamp
            .filter(|ts| received_timestamp.signed_duration_since(ts) <= Duration::hours(24));
        Self {
            hb_type: HbType::Wifi,
            coverage_object: value.report.coverage_object(),
            hotspot_key: value.report.pubkey,
            operation_mode: value.report.operation_mode,
            lat: value.report.lat,
            lon: value.report.lon,
            location_validation_timestamp,
            location_source: value.report.location_source,
            timestamp: received_timestamp,
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
    pub fn key(&self) -> KeyType<'_> {
        KeyType::Wifi(&self.hotspot_key)
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

    pub fn new(
        heartbeat: Heartbeat,
        cell_type: CellType,
        location_trust_score_multiplier: Decimal,
        distance_to_asserted: Option<i64>,
        coverage_meta: Option<CoverageObjectMeta>,
        validity: proto::HeartbeatValidity,
    ) -> Self {
        Self {
            heartbeat,
            cell_type,
            location_trust_score_multiplier,
            distance_to_asserted,
            coverage_meta,
            validity,
        }
    }

    /// Validate a heartbeat in the given epoch.
    #[allow(clippy::too_many_arguments)]
    pub async fn validate(
        mut heartbeat: Heartbeat,
        gateway_info_resolver: &impl GatewayResolver,
        coverage_object_cache: &CoverageObjectCache,
        last_location_cache: &LocationCache,
        max_distance_to_coverage: u32,
        epoch: &Range<DateTime<Utc>>,
        geofence: &impl GeofenceValidator,
    ) -> anyhow::Result<Self> {
        let Some(coverage_object) = heartbeat.coverage_object else {
            return Ok(Self::new(
                heartbeat,
                CellType::CellTypeNone,
                dec!(0),
                None,
                None,
                proto::HeartbeatValidity::BadCoverageObject,
            ));
        };

        let Some(coverage_object) = coverage_object_cache
            .fetch_coverage_object(&coverage_object, heartbeat.key())
            .await?
        else {
            return Ok(Self::new(
                heartbeat,
                CellType::CellTypeNone,
                dec!(0),
                None,
                None,
                proto::HeartbeatValidity::NoSuchCoverageObject,
            ));
        };

        let cell_type = match heartbeat.hb_type {
            HbType::Wifi => {
                if coverage_object.meta.indoor {
                    CellType::NovaGenericWifiIndoor
                } else {
                    CellType::NovaGenericWifiOutdoor
                }
            }
        };

        if !heartbeat.operation_mode {
            return Ok(Self::new(
                heartbeat,
                cell_type,
                dec!(0),
                None,
                Some(coverage_object.meta),
                proto::HeartbeatValidity::NotOperational,
            ));
        }

        if !epoch.contains(&heartbeat.timestamp) {
            return Ok(Self::new(
                heartbeat,
                cell_type,
                dec!(0),
                None,
                Some(coverage_object.meta),
                proto::HeartbeatValidity::HeartbeatOutsideRange,
            ));
        }

        let Ok(mut hb_latlng) = heartbeat.centered_latlng() else {
            return Ok(Self::new(
                heartbeat,
                cell_type,
                dec!(0),
                None,
                Some(coverage_object.meta),
                proto::HeartbeatValidity::InvalidLatLon,
            ));
        };

        if !geofence.in_valid_region(&heartbeat) {
            return Ok(Self::new(
                heartbeat,
                cell_type,
                dec!(0),
                None,
                Some(coverage_object.meta),
                proto::HeartbeatValidity::UnsupportedLocation,
            ));
        }

        match gateway_info_resolver
            .resolve_gateway(&heartbeat.hotspot_key)
            .await?
        {
            GatewayResolution::DataOnly => Ok(Self::new(
                heartbeat,
                cell_type,
                dec!(0),
                None,
                Some(coverage_object.meta),
                proto::HeartbeatValidity::InvalidDeviceType,
            )),
            GatewayResolution::GatewayNotFound => Ok(Self::new(
                heartbeat,
                cell_type,
                dec!(0),
                None,
                Some(coverage_object.meta),
                proto::HeartbeatValidity::GatewayNotFound,
            )),
            GatewayResolution::GatewayNotAsserted if heartbeat.hb_type == HbType::Wifi => {
                Ok(Self::new(
                    heartbeat,
                    cell_type,
                    dec!(0),
                    None,
                    Some(coverage_object.meta),
                    proto::HeartbeatValidity::GatewayNotAsserted,
                ))
            }
            GatewayResolution::AssertedLocation(location) if heartbeat.hb_type == HbType::Wifi => {
                let asserted_latlng: LatLng = CellIndex::try_from(location)?.into();
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
                    Some(location_validation_timestamp) => {
                        last_location_cache
                            .set(
                                &heartbeat.hotspot_key,
                                LastLocation::from_heartbeat(
                                    &heartbeat,
                                    location_validation_timestamp,
                                ),
                            )
                            .await;
                        true
                    }
                };

                let distance_to_asserted = asserted_latlng.distance_m(hb_latlng).round() as i64;
                let max_distance = coverage_object.max_distance_m(hb_latlng).round() as u32;

                let location_trust_score_multiplier = if !is_valid {
                    dec!(0)
                } else if max_distance >= max_distance_to_coverage {
                    // Furthest hex in Heartbeat exceeds allowed coverage distance
                    dec!(0)
                } else {
                    // HIP-119 maximum asserted distance check
                    use coverage_point_calculator::{
                        asserted_distance_to_trust_multiplier, RadioType,
                    };
                    let radio_type = match (heartbeat.hb_type, coverage_object.meta.indoor) {
                        (HbType::Wifi, true) => RadioType::IndoorWifi,
                        (HbType::Wifi, false) => RadioType::OutdoorWifi,
                    };
                    asserted_distance_to_trust_multiplier(radio_type, distance_to_asserted as u32)
                };

                Ok(Self::new(
                    heartbeat,
                    cell_type,
                    location_trust_score_multiplier,
                    Some(distance_to_asserted),
                    Some(coverage_object.meta),
                    proto::HeartbeatValidity::Valid,
                ))
            }
            _ => Ok(Self::new(
                heartbeat,
                cell_type,
                dec!(1.0),
                None,
                Some(coverage_object.meta),
                proto::HeartbeatValidity::Valid,
            )),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn validate_heartbeats<'a>(
        heartbeats: impl Stream<Item = Heartbeat> + 'a,
        gateway_info_resolver: &'a impl GatewayResolver,
        coverage_object_cache: &'a CoverageObjectCache,
        last_location_cache: &'a LocationCache,
        max_distance_to_coverage: u32,
        epoch: &'a Range<DateTime<Utc>>,
        geofence: &'a impl GeofenceValidator,
    ) -> impl Stream<Item = anyhow::Result<Self>> + 'a {
        heartbeats.then(move |heartbeat| async move {
            Self::validate(
                heartbeat,
                gateway_info_resolver,
                coverage_object_cache,
                last_location_cache,
                max_distance_to_coverage,
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

    pub async fn save(self, exec: &mut Transaction<'_, Postgres>) -> anyhow::Result<()> {
        coverage::set_invalidated_at(
            exec,
            self.heartbeat.timestamp,
            self.coverage_meta.as_ref().map(|x| x.inserted_at),
            self.heartbeat.key(),
            self.heartbeat.coverage_object,
        )
        .await?;
        // Save the heartbeat
        match self.heartbeat.hb_type {
            HbType::Wifi => self.save_wifi_hb(exec).await,
        }
    }

    async fn save_wifi_hb(self, exec: &mut Transaction<'_, Postgres>) -> anyhow::Result<()> {
        let truncated_timestamp = self.truncated_timestamp()?;
        sqlx::query(
            r#"
            INSERT INTO wifi_heartbeats (hotspot_key, cell_type, latest_timestamp, truncated_timestamp, coverage_object, location_trust_score_multiplier, distance_to_asserted, location_validation_timestamp, lat, lon)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            ON CONFLICT (hotspot_key, truncated_timestamp) DO UPDATE SET
            latest_timestamp = EXCLUDED.latest_timestamp,
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

#[allow(clippy::too_many_arguments)]
pub(crate) async fn process_validated_heartbeats(
    validated_heartbeats: impl Stream<Item = anyhow::Result<ValidatedHeartbeat>>,
    heartbeat_cache: &Cache<(String, DateTime<Utc>), ()>,
    coverage_claim_time_cache: &CoverageClaimTimeCache,
    heartbeat_sink: &FileSinkClient<proto::Heartbeat>,
    seniority_sink: &FileSinkClient<proto::SeniorityUpdate>,
    transaction: &mut Transaction<'_, Postgres>,
) -> anyhow::Result<()> {
    let mut validated_heartbeats = pin!(validated_heartbeats);
    while let Some(validated_heartbeat) = validated_heartbeats.next().await.transpose()? {
        validated_heartbeat.write(heartbeat_sink).await?;

        if !validated_heartbeat.is_valid() {
            continue;
        }
        if let Some(coverage_claim_time) = coverage_claim_time_cache
            .fetch_coverage_claim_time(
                validated_heartbeat.heartbeat.key(),
                &validated_heartbeat.heartbeat.coverage_object,
                &mut *transaction,
            )
            .await?
        {
            let latest_seniority =
                Seniority::fetch_latest(validated_heartbeat.heartbeat.key(), &mut *transaction)
                    .await?;
            let seniority_update = SeniorityUpdate::determine_update_action(
                &validated_heartbeat,
                coverage_claim_time,
                latest_seniority,
            )?;
            seniority_update.write(seniority_sink).await?;
            seniority_update.execute(&mut *transaction).await?;
        }

        let key = validated_heartbeat.heartbeat.id()?;
        if heartbeat_cache.get(&key).await.is_none() {
            validated_heartbeat.save(&mut *transaction).await?;
            heartbeat_cache
                .insert(key, (), time::Duration::from_secs(60 * 60 * 2))
                .await;
        }
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
    use crate::seniority::SeniorityUpdateAction;

    use super::*;
    use file_store::wifi_heartbeat::WifiHeartbeat;
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
                hb_type: HbType::Wifi,
                hotspot_key: PublicKeyBinary::from(Vec::new()),
                timestamp,
                lon: 0.0,
                lat: 0.0,
                operation_mode: false,
                coverage_object: Some(coverage_object),
                location_validation_timestamp: None,
                location_source: LocationSource::Skyhook,
            },
            validity: Default::default(),
            location_trust_score_multiplier: dec!(1.0),
            distance_to_asserted: None,
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
