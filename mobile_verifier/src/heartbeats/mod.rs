pub mod cbrs;
pub mod wifi;

use crate::{
    cell_type::{CellType, CellTypeLabel},
    coverage::{CoverageClaimTimeCache, CoveredHexCache, Seniority},
    GatewayResolution, GatewayResolver,
};
use anyhow::anyhow;
use chrono::{DateTime, Duration, DurationRound, RoundingError, Utc};
use file_store::{
    file_sink::FileSinkClient, heartbeat::CbrsHeartbeatIngestReport,
    wifi_heartbeat::WifiHeartbeatIngestReport,
};
use futures::stream::{Stream, StreamExt, TryStreamExt};
use h3o::{CellIndex, LatLng};
use helium_crypto::PublicKeyBinary;
use helium_proto::services::poc_mobile as proto;
use retainer::Cache;
use rust_decimal::{prelude::ToPrimitive, Decimal};
use sqlx::{postgres::PgTypeInfo, Decode, Encode, Postgres, Transaction, Type};
use std::{collections::HashMap, ops::Range, pin::pin, time};
use uuid::Uuid;

/// Minimum number of heartbeats required to give a reward to the hotspot.
const MINIMUM_HEARTBEAT_COUNT: i64 = 12;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, sqlx::Type)]
#[sqlx(type_name = "radio_type")]
#[sqlx(rename_all = "lowercase")]
pub enum HbType {
    Cbrs,
    Wifi,
}

#[derive(Copy, Clone)]
pub enum KeyType<'a> {
    Cbrs(&'a str),
    Wifi(&'a PublicKeyBinary),
}

impl From<KeyType<'_>> for proto::seniority_update::KeyType {
    fn from(kt: KeyType<'_>) -> Self {
        match kt {
            KeyType::Cbrs(id) => proto::seniority_update::KeyType::CbsdId(id.to_string()),
            KeyType::Wifi(key) => proto::seniority_update::KeyType::HotspotKey(key.clone().into()),
        }
    }
}

impl KeyType<'_> {
    pub fn to_owned(self) -> OwnedKeyType {
        match self {
            Self::Cbrs(cbrs) => OwnedKeyType::Cbrs(cbrs.to_owned()),
            Self::Wifi(key) => OwnedKeyType::Wifi(key.to_owned()),
        }
    }

    pub fn to_id(self) -> (String, HbType) {
        match self {
            Self::Cbrs(cbrs) => (cbrs.to_string(), HbType::Cbrs),
            Self::Wifi(wifi) => (wifi.to_string(), HbType::Wifi),
        }
    }

    pub fn hb_type(self) -> HbType {
        match self {
            Self::Cbrs(_) => HbType::Cbrs,
            Self::Wifi(_) => HbType::Wifi,
        }
    }
}

impl<'a> From<&'a str> for KeyType<'a> {
    fn from(cbrs: &'a str) -> Self {
        Self::Cbrs(cbrs)
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
        buf: &mut <Postgres as sqlx::database::HasArguments<'a>>::ArgumentBuffer,
    ) -> sqlx::encode::IsNull {
        match self {
            Self::Cbrs(cbrs) => cbrs.encode_by_ref(buf),
            Self::Wifi(wifi) => wifi.encode_by_ref(buf),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum OwnedKeyType {
    Cbrs(String),
    Wifi(PublicKeyBinary),
}

impl Type<Postgres> for OwnedKeyType {
    fn type_info() -> PgTypeInfo {
        PgTypeInfo::with_name("TEXT")
    }
}

impl<'a> Encode<'a, Postgres> for OwnedKeyType {
    fn encode_by_ref(
        &self,
        buf: &mut <Postgres as sqlx::database::HasArguments<'a>>::ArgumentBuffer,
    ) -> sqlx::encode::IsNull {
        match self {
            Self::Cbrs(cbrs) => cbrs.encode_by_ref(buf),
            Self::Wifi(wifi) => wifi.encode_by_ref(buf),
        }
    }
}

impl<'r> Decode<'r, Postgres> for OwnedKeyType {
    fn decode(
        value: <Postgres as sqlx::database::HasValueRef<'r>>::ValueRef,
    ) -> Result<Self, sqlx::error::BoxDynError> {
        let text = <&str as Decode<Postgres>>::decode(value)?;
        // Try decoding to a public key binary, otherwise it's a cbrs string
        match text.parse() {
            Ok(pubkey) => Ok(OwnedKeyType::Wifi(pubkey)),
            Err(_) => Ok(OwnedKeyType::Cbrs(text.to_string())),
        }
    }
}

#[derive(Clone)]
pub struct Heartbeat {
    pub hb_type: HbType,
    pub hotspot_key: PublicKeyBinary,
    pub cbsd_id: Option<String>,
    pub operation_mode: bool,
    pub lat: f64,
    pub lon: f64,
    pub coverage_object: Option<Uuid>,
    pub location_validation_timestamp: Option<DateTime<Utc>>,
    pub timestamp: DateTime<Utc>,
}

impl Heartbeat {
    pub fn truncated_timestamp(&self) -> Result<DateTime<Utc>, RoundingError> {
        self.timestamp.duration_trunc(Duration::hours(1))
    }

    pub fn key(&self) -> KeyType<'_> {
        match self.hb_type {
            HbType::Cbrs => KeyType::from(self.cbsd_id.as_deref().unwrap()),
            HbType::Wifi => KeyType::from(&self.hotspot_key),
        }
    }

    pub fn id(&self) -> anyhow::Result<(String, DateTime<Utc>)> {
        let ts = self.truncated_timestamp()?;
        match self.hb_type {
            HbType::Cbrs => {
                let cbsd_id = self
                    .cbsd_id
                    .clone()
                    .ok_or_else(|| anyhow!("expected cbsd_id, found none"))?;
                Ok((cbsd_id, ts))
            }
            HbType::Wifi => Ok((self.hotspot_key.to_string(), ts)),
        }
    }

    pub fn asserted_distance(&self, asserted_location: u64) -> anyhow::Result<i64> {
        let asserted_latlng: LatLng = CellIndex::try_from(asserted_location)?.into();
        let hb_latlng = LatLng::new(self.lat, self.lon)?;
        Ok(asserted_latlng.distance_m(hb_latlng).round() as i64)
    }
}

impl From<CbrsHeartbeatIngestReport> for Heartbeat {
    fn from(value: CbrsHeartbeatIngestReport) -> Self {
        Self {
            hb_type: HbType::Cbrs,
            coverage_object: value.report.coverage_object(),
            hotspot_key: value.report.pubkey,
            cbsd_id: Some(value.report.cbsd_id),
            operation_mode: value.report.operation_mode,
            lat: value.report.lat,
            lon: value.report.lon,
            location_validation_timestamp: None,
            timestamp: value.received_timestamp,
        }
    }
}

impl From<WifiHeartbeatIngestReport> for Heartbeat {
    fn from(value: WifiHeartbeatIngestReport) -> Self {
        Self {
            hb_type: HbType::Wifi,
            hotspot_key: value.report.pubkey,
            cbsd_id: None,
            operation_mode: value.report.operation_mode,
            lat: value.report.lat,
            lon: value.report.lon,
            location_validation_timestamp: value.report.location_validation_timestamp,
            timestamp: value.received_timestamp,
            coverage_object: None,
        }
    }
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct HeartbeatRow {
    pub hotspot_key: PublicKeyBinary,
    // cell hb only
    pub cbsd_id: Option<String>,
    pub cell_type: CellType,
    // wifi hb only
    pub location_validation_timestamp: Option<DateTime<Utc>>,
    pub distance_to_asserted: Option<i64>,
    pub coverage_object: Option<Uuid>,
    pub latest_timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct HeartbeatReward {
    pub hotspot_key: PublicKeyBinary,
    pub cell_type: CellType,
    // cell hb only
    pub cbsd_id: Option<String>,
    pub reward_weight: Decimal,
    pub coverage_object: Option<Uuid>,
    pub latest_timestamp: DateTime<Utc>,
}

impl HeartbeatReward {
    pub fn id(&self) -> anyhow::Result<String> {
        match self.cell_type.to_label() {
            CellTypeLabel::CBRS => Ok(self
                .cbsd_id
                .clone()
                .ok_or_else(|| anyhow!("expected cbsd_id, found none"))?),
            CellTypeLabel::Wifi => Ok(self.hotspot_key.to_string()),
            _ => Err(anyhow!("failed to derive label from cell type")),
        }
    }

    pub fn reward_weight(&self) -> Decimal {
        self.reward_weight
    }

    pub async fn validated<'a>(
        exec: impl sqlx::PgExecutor<'a> + Copy + 'a,
        epoch: &'a Range<DateTime<Utc>>,
        max_distance_to_asserted: u32,
    ) -> anyhow::Result<impl Stream<Item = HeartbeatReward> + 'a> {
        let heartbeat_rows =
            sqlx::query_as::<_, HeartbeatRow>(include_str!("valid_heartbeats.sql"))
                .bind(epoch.start)
                .bind(epoch.end)
                .bind(MINIMUM_HEARTBEAT_COUNT)
                .fetch(exec)
                .try_fold(
                    HashMap::<(PublicKeyBinary, Option<String>), Vec<HeartbeatRow>>::new(),
                    |mut map, row| async move {
                        map.entry((row.hotspot_key.clone(), row.cbsd_id.clone()))
                            .or_default()
                            .push(row);

                        Ok(map)
                    },
                )
                .await?;

        Ok(
            futures::stream::iter(heartbeat_rows).map(move |((hotspot_key, cbsd_id), rows)| {
                let first = rows.first().unwrap();
                let average_location_trust_score = rows
                    .iter()
                    .map(|row| {
                        row.cell_type.location_weight(
                            row.location_validation_timestamp,
                            row.distance_to_asserted,
                            max_distance_to_asserted,
                        )
                    })
                    .sum::<Decimal>()
                    / Decimal::new(rows.len() as i64, 0);

                HeartbeatReward {
                    hotspot_key,
                    cell_type: first.cell_type,
                    cbsd_id,
                    reward_weight: first.cell_type.reward_weight() * average_location_trust_score,
                    coverage_object: first.coverage_object,
                    latest_timestamp: first.latest_timestamp,
                }
            }),
        )
    }

    pub fn from_heartbeat_row(value: HeartbeatRow, max_distance_to_asserted: u32) -> Self {
        Self {
            hotspot_key: value.hotspot_key,
            cell_type: value.cell_type,
            cbsd_id: value.cbsd_id,
            reward_weight: value.cell_type.reward_weight()
                * value.cell_type.location_weight(
                    value.location_validation_timestamp,
                    value.distance_to_asserted,
                    max_distance_to_asserted,
                ),
            coverage_object: value.coverage_object,
            latest_timestamp: value.latest_timestamp,
        }
    }
}

#[derive(Clone)]
pub struct ValidatedHeartbeat {
    pub heartbeat: Heartbeat,
    pub cell_type: CellType,
    pub distance_to_asserted: Option<i64>,
    pub coverage_object_insertion_time: Option<DateTime<Utc>>,
    pub validity: proto::HeartbeatValidity,
}

impl ValidatedHeartbeat {
    pub fn is_valid(&self) -> bool {
        self.validity == proto::HeartbeatValidity::Valid
    }

    pub fn truncated_timestamp(&self) -> Result<DateTime<Utc>, RoundingError> {
        self.heartbeat.timestamp.duration_trunc(Duration::hours(1))
    }

    pub fn validate_heartbeats<'a>(
        heartbeats: impl Stream<Item = Heartbeat> + 'a,
        gateway_client: &'a impl GatewayResolver,
        coverage_cache: &'a CoveredHexCache,
        max_distance: f64,
        epoch: &'a Range<DateTime<Utc>>,
    ) -> impl Stream<Item = anyhow::Result<Self>> + 'a {
        heartbeats.then(move |heartbeat| async move {
            let (cell_type, distance_to_asserted, coverage_object_insertion_time, validity) =
                validate_heartbeat(
                    &heartbeat,
                    gateway_client,
                    coverage_cache,
                    epoch,
                    max_distance,
                )
                .await?;

            Ok(Self {
                heartbeat,
                cell_type,
                distance_to_asserted,
                coverage_object_insertion_time,
                validity,
            })
        })
    }

    pub async fn write(&self, heartbeats: &FileSinkClient) -> file_store::Result {
        heartbeats
            .write(
                proto::Heartbeat {
                    cbsd_id: self.heartbeat.cbsd_id.clone().unwrap_or_default(),
                    pub_key: self.heartbeat.hotspot_key.as_ref().into(),
                    reward_multiplier: self.cell_type.reward_weight().to_f32().unwrap_or_default(),
                    cell_type: self.cell_type as i32,
                    validity: self.validity as i32,
                    timestamp: self.heartbeat.timestamp.timestamp() as u64,
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
                },
                &[("validity", self.validity.as_str_name())],
            )
            .await?;
        Ok(())
    }

    pub async fn save(self, exec: &mut Transaction<'_, Postgres>) -> anyhow::Result<()> {
        // Invalidate all of the previous coverage objects
        sqlx::query(
            "UPDATE hex_coverage SET invalidated_at = $1 WHERE inserted_at < $2 AND invalidated_at IS NULL AND radio_key = $3 AND uuid != $4"
        )
        .bind(self.heartbeat.timestamp)
        .bind(self.coverage_object_insertion_time)
        .bind(self.heartbeat.key())
        .bind(self.heartbeat.coverage_object)
        .execute(&mut *exec)
        .await?;
        // Save the heartbeat
        match self.heartbeat.hb_type {
            HbType::Cbrs => self.save_cbrs_hb(exec).await,
            HbType::Wifi => self.save_wifi_hb(exec).await,
        }
    }

    async fn save_cbrs_hb(self, exec: &mut Transaction<'_, Postgres>) -> anyhow::Result<()> {
        let truncated_timestamp = self.truncated_timestamp()?;
        sqlx::query(
            r#"
            INSERT INTO cbrs_heartbeats (cbsd_id, hotspot_key, cell_type, latest_timestamp, truncated_timestamp, coverage_object)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (cbsd_id, truncated_timestamp) DO UPDATE SET
            latest_timestamp = EXCLUDED.latest_timestamp,
            coverage_object = EXCLUDED.coverage_object
            "#
        )
        .bind(self.heartbeat.cbsd_id)
        .bind(self.heartbeat.hotspot_key)
        .bind(self.cell_type)
        .bind(self.heartbeat.timestamp)
        .bind(truncated_timestamp)
        .bind(self.heartbeat.coverage_object)
        .execute(&mut *exec)
        .await?;
        Ok(())
    }

    async fn save_wifi_hb(self, exec: &mut Transaction<'_, Postgres>) -> anyhow::Result<()> {
        let truncated_timestamp = self.truncated_timestamp()?;
        sqlx::query(
            r#"
            INSERT INTO wifi_heartbeats (hotspot_key, cell_type, location_validation_timestamp, distance_to_asserted,
                latest_timestamp, truncated_timestamp, coverage_object)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (hotspot_key, truncated_timestamp) DO UPDATE SET
            latest_timestamp = EXCLUDED.latest_timestamp,
            coverage_object = EXCLUDED.coverage_object
            "#,
        )
        .bind(self.heartbeat.hotspot_key)
        .bind(self.cell_type)
        .bind(self.heartbeat.location_validation_timestamp)
        .bind(self.distance_to_asserted)
        .bind(self.heartbeat.timestamp)
        .bind(truncated_timestamp)
        .bind(self.heartbeat.coverage_object)
        .execute(&mut *exec)
        .await?;
        Ok(())
    }
}

/// Validate a heartbeat in the given epoch.
pub async fn validate_heartbeat(
    heartbeat: &Heartbeat,
    gateway_resolver: &impl GatewayResolver,
    _coverage_cache: &CoveredHexCache,
    epoch: &Range<DateTime<Utc>>,
    _max_distance: f64,
) -> anyhow::Result<(
    CellType,
    Option<i64>,
    Option<DateTime<Utc>>,
    proto::HeartbeatValidity,
)> {
    let cell_type = match heartbeat.hb_type {
        HbType::Cbrs => match heartbeat.cbsd_id.as_ref() {
            Some(cbsd_id) => match CellType::from_cbsd_id(cbsd_id) {
                Some(ty) => ty,
                _ => {
                    return Ok((
                        CellType::CellTypeNone,
                        None,
                        None,
                        proto::HeartbeatValidity::BadCbsdId,
                    ))
                }
            },
            None => {
                return Ok((
                    CellType::CellTypeNone,
                    None,
                    None,
                    proto::HeartbeatValidity::BadCbsdId,
                ))
            }
        },
        // for wifi HBs temporary assume we have an indoor wifi spot
        // this will be better/properly handled when coverage reports are live
        HbType::Wifi => CellType::NovaGenericWifiIndoor,
    };

    if !heartbeat.operation_mode {
        return Ok((
            cell_type,
            None,
            None,
            proto::HeartbeatValidity::NotOperational,
        ));
    }

    if !epoch.contains(&heartbeat.timestamp) {
        return Ok((
            cell_type,
            None,
            None,
            proto::HeartbeatValidity::HeartbeatOutsideRange,
        ));
    }

    let distance_to_asserted = match gateway_resolver
        .resolve_gateway(&heartbeat.hotspot_key)
        .await?
    {
        GatewayResolution::GatewayNotFound => {
            return Ok((
                cell_type,
                None,
                None,
                proto::HeartbeatValidity::GatewayNotFound,
            ))
        }
        GatewayResolution::GatewayNotAsserted if heartbeat.hb_type == HbType::Wifi => {
            return Ok((
                cell_type,
                None,
                None,
                proto::HeartbeatValidity::GatewayNotAsserted,
            ))
        }
        GatewayResolution::AssertedLocation(location) if heartbeat.hb_type == HbType::Wifi => {
            Some(heartbeat.asserted_distance(location)?)
        }
        _ => None,
    };

    /*
    let Some(coverage_object) = heartbeat.report.coverage_object() else {
        return Ok((cell_type, None, proto::HeartbeatValidity::BadCoverageObject));
    };

    let Some(coverage) = coverage_cache.fetch_coverage(&coverage_object).await? else {
        return Ok((
            cell_type,
            None,
            proto::HeartbeatValidity::NoSuchCoverageObject,
        ));
    };

    if coverage.cbsd_id != heartbeat.report.cbsd_id {
        return Ok((cell_type, None, proto::HeartbeatValidity::BadCoverageObject));
    }

    let Ok(latlng) = LatLng::new(heartbeat.report.lat, heartbeat.report.lon) else {
        return Ok((cell_type, None, proto::HeartbeatValidity::InvalidLatLon));
    };

    if coverage.max_distance_km(latlng) > max_distance {
        return Ok((
            cell_type,
            None,
            proto::HeartbeatValidity::TooFarFromCoverage,
        ));
    }
     */

    Ok((
        cell_type,
        distance_to_asserted,
        None,
        proto::HeartbeatValidity::Valid,
    ))
}

pub(crate) async fn process_validated_heartbeats(
    validated_heartbeats: impl Stream<Item = anyhow::Result<ValidatedHeartbeat>>,
    heartbeat_cache: &Cache<(String, DateTime<Utc>), ()>,
    coverage_claim_time_cache: &CoverageClaimTimeCache,
    modeled_coverage_start: DateTime<Utc>,
    heartbeat_sink: &FileSinkClient,
    seniority_sink: &FileSinkClient,
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
                modeled_coverage_start,
                latest_seniority,
            );
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
    sqlx::query("DELETE FROM cbrs_heartbeats WHERE truncated_timestamp < $1;")
        .bind(timestamp)
        .execute(&mut *tx)
        .await?;

    sqlx::query("DELETE FROM wifi_heartbeats WHERE truncated_timestamp < $1;")
        .bind(timestamp)
        .execute(&mut *tx)
        .await?;

    Ok(())
}

pub struct SeniorityUpdate<'a> {
    heartbeat: &'a ValidatedHeartbeat,
    action: SeniorityUpdateAction,
}

#[derive(Debug, PartialEq)]
pub enum SeniorityUpdateAction {
    NoAction,
    Insert {
        new_seniority: DateTime<Utc>,
        update_reason: proto::SeniorityUpdateReason,
    },
    Update {
        curr_seniority: DateTime<Utc>,
    },
}

impl<'a> SeniorityUpdate<'a> {
    pub fn new(heartbeat: &'a ValidatedHeartbeat, action: SeniorityUpdateAction) -> Self {
        Self { heartbeat, action }
    }

    pub fn determine_update_action(
        heartbeat: &'a ValidatedHeartbeat,
        coverage_claim_time: DateTime<Utc>,
        modeled_coverage_start: DateTime<Utc>,
        latest_seniority: Option<Seniority>,
    ) -> Self {
        use proto::SeniorityUpdateReason::*;

        if let Some(prev_seniority) = latest_seniority {
            if heartbeat.heartbeat.coverage_object != Some(prev_seniority.uuid) {
                if prev_seniority.update_reason == HeartbeatNotSeen as i32
                    && coverage_claim_time < prev_seniority.seniority_ts
                {
                    Self::new(heartbeat, SeniorityUpdateAction::NoAction)
                } else {
                    Self::new(
                        heartbeat,
                        SeniorityUpdateAction::Insert {
                            new_seniority: coverage_claim_time,
                            update_reason: NewCoverageClaimTime,
                        },
                    )
                }
            } else if heartbeat.heartbeat.timestamp - prev_seniority.last_heartbeat
                > Duration::days(3)
                && coverage_claim_time < heartbeat.heartbeat.timestamp
            {
                Self::new(
                    heartbeat,
                    SeniorityUpdateAction::Insert {
                        new_seniority: heartbeat.heartbeat.timestamp,
                        update_reason: HeartbeatNotSeen,
                    },
                )
            } else {
                Self::new(
                    heartbeat,
                    SeniorityUpdateAction::Update {
                        curr_seniority: prev_seniority.seniority_ts,
                    },
                )
            }
        } else if heartbeat.heartbeat.timestamp - modeled_coverage_start > Duration::days(3) {
            // This will become the default case 72 hours after we launch modeled coverage
            Self::new(
                heartbeat,
                SeniorityUpdateAction::Insert {
                    new_seniority: heartbeat.heartbeat.timestamp,
                    update_reason: HeartbeatNotSeen,
                },
            )
        } else {
            Self::new(
                heartbeat,
                SeniorityUpdateAction::Insert {
                    new_seniority: coverage_claim_time,
                    update_reason: NewCoverageClaimTime,
                },
            )
        }
    }
}

impl SeniorityUpdate<'_> {
    pub async fn write(&self, seniorities: &FileSinkClient) -> anyhow::Result<()> {
        if let SeniorityUpdateAction::Insert {
            new_seniority,
            update_reason,
        } = self.action
        {
            seniorities
                .write(
                    proto::SeniorityUpdate {
                        key_type: Some(self.heartbeat.heartbeat.key().into()),
                        new_seniority_timestamp: new_seniority.timestamp() as u64,
                        reason: update_reason as i32,
                    },
                    [],
                )
                .await?;
        }
        Ok(())
    }

    pub async fn execute(self, exec: &mut Transaction<'_, Postgres>) -> anyhow::Result<()> {
        match self.action {
            SeniorityUpdateAction::NoAction => (),
            SeniorityUpdateAction::Insert {
                new_seniority,
                update_reason,
            } => {
                sqlx::query(
                    r#"
                    INSERT INTO seniority
                      (radio_key, last_heartbeat, uuid, seniority_ts, inserted_at, update_reason, radio_type)
                    VALUES
                      ($1, $2, $3, $4, $5, $6, $7)
                    ON CONFLICT (radio_key, radio_type, seniority_ts) DO UPDATE SET
                      uuid = EXCLUDED.uuid,
                      last_heartbeat = EXCLUDED.last_heartbeat,
                      update_reason = EXCLUDED.update_reason
                    "#,
                )
                .bind(self.heartbeat.heartbeat.key())
                .bind(self.heartbeat.heartbeat.timestamp)
                .bind(self.heartbeat.heartbeat.coverage_object)
                .bind(new_seniority)
                .bind(self.heartbeat.heartbeat.timestamp)
                .bind(update_reason as i32)
                .bind(self.heartbeat.heartbeat.hb_type)
                .execute(&mut *exec)
                .await?;
            }
            SeniorityUpdateAction::Update { curr_seniority } => {
                sqlx::query(
                    r#"
                    UPDATE seniority
                    SET last_heartbeat = $1
                    WHERE
                      radio_key = $2 AND
                      seniority_ts = $3
                    "#,
                )
                .bind(self.heartbeat.heartbeat.timestamp)
                .bind(self.heartbeat.heartbeat.key())
                .bind(curr_seniority)
                .execute(&mut *exec)
                .await?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use proto::SeniorityUpdateReason::*;

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
                cbsd_id: None,
                coverage_object: Some(coverage_object),
                location_validation_timestamp: None,
            },
            validity: Default::default(),
            distance_to_asserted: None,
            coverage_object_insertion_time: None,
        }
    }

    #[test]
    fn ensure_first_seniority_causes_update() {
        let modeled_coverage_start = "2023-08-20 00:00:00.000000000 UTC".parse().unwrap();
        let coverage_claim_time: DateTime<Utc> =
            "2023-08-22 00:00:00.000000000 UTC".parse().unwrap();
        let coverage_object = Uuid::new_v4();

        let received_timestamp: DateTime<Utc> =
            "2023-08-23 00:00:00.000000000 UTC".parse().unwrap();
        let new_heartbeat = heartbeat(received_timestamp, coverage_object);
        let seniority_action = SeniorityUpdate::determine_update_action(
            &new_heartbeat,
            coverage_claim_time,
            modeled_coverage_start,
            None,
        );

        assert_eq!(
            seniority_action.action,
            SeniorityUpdateAction::Insert {
                new_seniority: coverage_claim_time,
                update_reason: NewCoverageClaimTime,
            }
        );
    }

    #[test]
    fn ensure_first_seniority_72_hours_after_start_resets_coverage_claim_time() {
        let modeled_coverage_start = "2023-08-20 00:00:00.000000000 UTC".parse().unwrap();
        let coverage_claim_time: DateTime<Utc> =
            "2023-08-22 00:00:00.000000000 UTC".parse().unwrap();
        let coverage_object = Uuid::new_v4();

        let received_timestamp: DateTime<Utc> =
            "2023-08-23 00:00:01.000000000 UTC".parse().unwrap();
        let new_heartbeat = heartbeat(received_timestamp, coverage_object);
        let seniority_action = SeniorityUpdate::determine_update_action(
            &new_heartbeat,
            coverage_claim_time,
            modeled_coverage_start,
            None,
        );

        assert_eq!(
            seniority_action.action,
            SeniorityUpdateAction::Insert {
                new_seniority: received_timestamp,
                update_reason: HeartbeatNotSeen,
            }
        );
    }

    #[test]
    fn ensure_seniority_updates_on_new_coverage_object() {
        let modeled_coverage_start = "2023-08-20 00:00:00.000000000 UTC".parse().unwrap();
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
            modeled_coverage_start,
            Some(latest_seniority.clone()),
        );

        assert_eq!(
            seniority_action.action,
            SeniorityUpdateAction::Insert {
                new_seniority: new_coverage_claim_time,
                update_reason: NewCoverageClaimTime,
            }
        );
    }

    #[test]
    fn ensure_last_heartbeat_updates_on_same_coverage_object() {
        let modeled_coverage_start = "2023-08-20 00:00:00.000000000 UTC".parse().unwrap();
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
            modeled_coverage_start,
            Some(latest_seniority.clone()),
        );

        assert_eq!(
            seniority_action.action,
            SeniorityUpdateAction::Update {
                curr_seniority: coverage_claim_time,
            }
        );
    }

    #[test]
    fn ensure_seniority_updates_after_72_hours() {
        let modeled_coverage_start = "2023-08-20 00:00:00.000000000 UTC".parse().unwrap();
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
            modeled_coverage_start,
            Some(latest_seniority),
        );
        assert_eq!(
            seniority_action.action,
            SeniorityUpdateAction::Insert {
                new_seniority: received_timestamp,
                update_reason: HeartbeatNotSeen,
            }
        );
    }

    #[test]
    fn ensure_seniority_updates_after_not_seen_if_in_future() {
        let modeled_coverage_start = "2023-08-20 00:00:00.000000000 UTC".parse().unwrap();
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
            modeled_coverage_start,
            Some(latest_seniority.clone()),
        );
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
            modeled_coverage_start,
            Some(latest_seniority),
        );
        assert_eq!(seniority_action.action, SeniorityUpdateAction::NoAction);
    }
}
