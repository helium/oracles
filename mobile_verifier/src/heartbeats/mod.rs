pub mod cbrs;
pub mod wifi;

use crate::cell_type::{CellType, CellTypeLabel};
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
use mobile_config::client::gateway_client::GatewayInfoResolver;
use retainer::Cache;
use rust_decimal::{prelude::ToPrimitive, Decimal};
use sqlx::{Postgres, Transaction};
use std::{ops::Range, pin::pin, time};

/// Minimum number of heartbeats required to give a reward to the hotspot.
const MINIMUM_HEARTBEAT_COUNT: i64 = 12;

#[derive(Clone, PartialEq)]
pub enum HBType {
    Cbrs = 0,
    Wifi = 1,
}

#[derive(Clone)]
pub struct Heartbeat {
    hb_type: HBType,
    hotspot_key: PublicKeyBinary,
    cbsd_id: Option<String>,
    operation_mode: bool,
    lat: f64,
    lon: f64,
    location_validation_timestamp: Option<DateTime<Utc>>,
    timestamp: DateTime<Utc>,
}

impl Heartbeat {
    pub fn truncated_timestamp(&self) -> Result<DateTime<Utc>, RoundingError> {
        self.timestamp.duration_trunc(Duration::hours(1))
    }

    pub fn id(&self) -> anyhow::Result<(String, DateTime<Utc>)> {
        let ts = self.truncated_timestamp()?;
        match self.hb_type {
            HBType::Cbrs => {
                let cbsd_id = self
                    .cbsd_id
                    .clone()
                    .ok_or_else(|| anyhow!("expected cbsd_id, found none"))?;
                Ok((cbsd_id, ts))
            }
            HBType::Wifi => Ok((self.hotspot_key.to_string(), ts)),
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
            hb_type: HBType::Cbrs,
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
            hb_type: HBType::Wifi,
            hotspot_key: value.report.pubkey,
            cbsd_id: None,
            operation_mode: value.report.operation_mode,
            lat: value.report.lat,
            lon: value.report.lon,
            location_validation_timestamp: value.report.location_validation_timestamp,
            timestamp: value.received_timestamp,
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
}

#[derive(Debug, Clone, PartialEq)]
pub struct HeartbeatReward {
    pub hotspot_key: PublicKeyBinary,
    pub cell_type: CellType,
    // cell hb only
    pub cbsd_id: Option<String>,
    pub reward_weight: Decimal,
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

    pub fn validated<'a>(
        exec: impl sqlx::PgExecutor<'a> + Copy + 'a,
        epoch: &'a Range<DateTime<Utc>>,
        max_distance_to_asserted: u32,
    ) -> impl Stream<Item = Result<HeartbeatReward, sqlx::Error>> + 'a {
        sqlx::query_as::<_, HeartbeatRow>(
            r#"
            (WITH latest_hotspots AS (
                SELECT t1.cbsd_id, t1.hotspot_key, t1.latest_timestamp
                FROM cbrs_heartbeats t1
                WHERE t1.latest_timestamp = (
                      SELECT MAX(t2.latest_timestamp)
                      FROM cbrs_heartbeats t2
                      WHERE t2.cbsd_id = t1.cbsd_id
                      AND truncated_timestamp >= $1
                      AND truncated_timestamp < $2
               )
           )
           SELECT
             latest_hotspots.hotspot_key,
             cbrs_heartbeats.cbsd_id,
             cell_type,
             NULL as location_validation_timestamp,
             NULL as distance_to_asserted
           FROM cbrs_heartbeats
           JOIN latest_hotspots ON cbrs_heartbeats.cbsd_id = latest_hotspots.cbsd_id
           WHERE truncated_timestamp >= $1
             AND truncated_timestamp < $2
           GROUP BY
             cbrs_heartbeats.cbsd_id,
             latest_hotspots.hotspot_key,
             cell_type
           HAVING count(*) >= $3)
           UNION
           SELECT
                grouped.hotspot_key,
                NULL as cbsd_id,
                grouped.cell_type,
                b.location_validation_timestamp,
                b.distance_to_asserted
           FROM
           (
                SELECT
                hotspot_key,
                cell_type
                FROM wifi_heartbeats
                WHERE truncated_timestamp >= $1
                AND truncated_timestamp < $2
                GROUP BY hotspot_key, cell_type
                HAVING count(*) >= $3
            ) as grouped
            LEFT JOIN (
                select hotspot_key,
                location_validation_timestamp,
                distance_to_asserted
                from wifi_heartbeats
                WHERE wifi_heartbeats.truncated_timestamp >= $1
                AND wifi_heartbeats.truncated_timestamp < $2
            ) as b on b.hotspot_key = grouped.hotspot_key
            "#,
        )
        .bind(epoch.start)
        .bind(epoch.end)
        .bind(MINIMUM_HEARTBEAT_COUNT)
        .fetch(exec)
        .map_ok(move |row| Self::from_heartbeat_row(row, max_distance_to_asserted))
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
        }
    }
}

#[derive(sqlx::FromRow)]
pub struct HeartbeatSaveResult {
    inserted: bool,
}

#[derive(Clone)]
pub struct ValidatedHeartbeat {
    pub report: Heartbeat,
    cell_type: CellType,
    validity: proto::HeartbeatValidity,
    distance_to_asserted: Option<i64>,
}

impl ValidatedHeartbeat {
    pub fn is_valid(&self) -> bool {
        self.validity == proto::HeartbeatValidity::Valid
    }

    pub fn truncated_timestamp(&self) -> Result<DateTime<Utc>, RoundingError> {
        self.report.timestamp.duration_trunc(Duration::hours(1))
    }

    pub async fn validate_heartbeats<'a, GIR>(
        gateway_info_resolver: &'a GIR,
        heartbeats: impl Stream<Item = Heartbeat> + 'a,
        epoch: &'a Range<DateTime<Utc>>,
    ) -> impl Stream<Item = anyhow::Result<Self>> + 'a
    where
        GIR: GatewayInfoResolver,
    {
        heartbeats.then(move |report| {
            let mut gateway_info_resolver = gateway_info_resolver.clone();
            async move {
                let (cell_type, validity, distance_to_asserted) =
                    validate_heartbeat(&report, &mut gateway_info_resolver, epoch).await?;

                Ok(Self {
                    report,
                    cell_type,
                    validity,
                    distance_to_asserted,
                })
            }
        })
    }

    pub async fn write(&self, heartbeats: &FileSinkClient) -> file_store::Result {
        heartbeats
            .write(
                proto::Heartbeat {
                    cbsd_id: self.report.cbsd_id.clone().unwrap_or_default(),
                    pub_key: self.report.hotspot_key.as_ref().into(),
                    reward_multiplier: self.cell_type.reward_weight().to_f32().unwrap_or_default(),
                    cell_type: self.cell_type as i32,
                    validity: self.validity as i32,
                    timestamp: self.report.timestamp.timestamp() as u64,
                    coverage_object: Vec::with_capacity(0), // Placeholder so the project compiles
                    lat: 0.0,
                    lon: 0.0,
                    location_validation_timestamp: self
                        .report
                        .location_validation_timestamp
                        .map_or(0, |v| v.timestamp() as u64),
                    distance_to_asserted: self.distance_to_asserted.map_or(0, |v| v as u64),
                },
                &[("validity", self.validity.as_str_name())],
            )
            .await?;
        Ok(())
    }

    pub async fn save(self, exec: &mut Transaction<'_, Postgres>) -> anyhow::Result<bool> {
        match self.report.hb_type {
            HBType::Cbrs => self.save_cbrs_hb(exec).await,
            HBType::Wifi => self.save_wifi_hb(exec).await,
        }
    }

    async fn save_cbrs_hb(self, exec: &mut Transaction<'_, Postgres>) -> anyhow::Result<bool> {
        let cbsd_id = self
            .report
            .cbsd_id
            .as_ref()
            .ok_or_else(|| anyhow!("failed to save cbrs heartbeat, invalid cbsd_id"))?;

        let truncated_timestamp = self.truncated_timestamp()?;
        Ok(
            sqlx::query_as::<_, HeartbeatSaveResult>(
                r#"
                INSERT INTO cbrs_heartbeats (cbsd_id, hotspot_key, cell_type, latest_timestamp, truncated_timestamp)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (cbsd_id, truncated_timestamp) DO UPDATE SET
                latest_timestamp = EXCLUDED.latest_timestamp
                RETURNING (xmax = 0) as inserted
                "#
            )
            .bind(cbsd_id)
            .bind(self.report.hotspot_key)
            .bind(self.cell_type)
            .bind(self.report.timestamp)
            .bind(truncated_timestamp)
            .fetch_one(&mut *exec)
            .await?
            .inserted
        )
    }

    async fn save_wifi_hb(self, exec: &mut Transaction<'_, Postgres>) -> anyhow::Result<bool> {
        let truncated_timestamp = self.truncated_timestamp()?;
        Ok(sqlx::query_as::<_, HeartbeatSaveResult>(
            r#"
                INSERT INTO wifi_heartbeats (hotspot_key, cell_type, location_validation_timestamp, distance_to_asserted,
                    latest_timestamp, truncated_timestamp)
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (hotspot_key, truncated_timestamp) DO UPDATE SET
                latest_timestamp = EXCLUDED.latest_timestamp
                RETURNING (xmax = 0) as inserted
                "#,
        )
        .bind(self.report.hotspot_key)
        .bind(self.cell_type)
        .bind(self.report.location_validation_timestamp)
        .bind(self.distance_to_asserted)
        .bind(self.report.timestamp)
        .bind(truncated_timestamp)
        .fetch_one(&mut *exec)
        .await?
        .inserted)
    }
}

/// Validate a heartbeat in the given epoch.
pub async fn validate_heartbeat<GIR>(
    heartbeat: &Heartbeat,
    gateway_info_resolver: &mut GIR,
    epoch: &Range<DateTime<Utc>>,
) -> anyhow::Result<(CellType, proto::HeartbeatValidity, Option<i64>)>
where
    GIR: GatewayInfoResolver,
{
    let cell_type = match heartbeat.hb_type {
        HBType::Cbrs => match heartbeat.cbsd_id.as_ref() {
            Some(cbsd_id) => match CellType::from_cbsd_id(cbsd_id) {
                Some(ty) => ty,
                _ => {
                    return Ok((
                        CellType::CellTypeNone,
                        proto::HeartbeatValidity::BadCbsdId,
                        None,
                    ))
                }
            },
            None => {
                return Ok((
                    CellType::CellTypeNone,
                    proto::HeartbeatValidity::BadCbsdId,
                    None,
                ))
            }
        },
        // for wifi HBs temporary assume we have an indoor wifi spot
        // this will be better/properly handled when coverage reports are live
        HBType::Wifi => CellType::NovaGenericWifiIndoor,
    };

    if !heartbeat.operation_mode {
        return Ok((cell_type, proto::HeartbeatValidity::NotOperational, None));
    }

    if !epoch.contains(&heartbeat.timestamp) {
        return Ok((
            cell_type,
            proto::HeartbeatValidity::HeartbeatOutsideRange,
            None,
        ));
    }

    let Some(gateway_info) = gateway_info_resolver
        .resolve_gateway_info(&heartbeat.hotspot_key)
        .await?
    else {
        return Ok((cell_type, proto::HeartbeatValidity::GatewayNotFound, None));
    };

    let Some(metadata) = gateway_info.metadata else {
        return Ok((
            cell_type,
            proto::HeartbeatValidity::GatewayNotAsserted,
            None,
        ));
    };

    let distance_to_asserted = if heartbeat.hb_type == HBType::Wifi {
        Some(heartbeat.asserted_distance(metadata.location)?)
    } else {
        None
    };

    Ok((
        cell_type,
        proto::HeartbeatValidity::Valid,
        distance_to_asserted,
    ))
}

pub(crate) async fn process_heartbeat_stream<'a, GIR>(
    reports: impl Stream<Item = Heartbeat> + 'a,
    gateway_info_resolver: &'a GIR,
    file_sink: &FileSinkClient,
    cache: &Cache<(String, DateTime<Utc>), ()>,
    mut transaction: Transaction<'_, Postgres>,
    epoch: &'a Range<DateTime<Utc>>,
) -> anyhow::Result<()>
where
    GIR: GatewayInfoResolver,
{
    let mut validated_heartbeats =
        pin!(ValidatedHeartbeat::validate_heartbeats(gateway_info_resolver, reports, epoch).await);

    while let Some(validated_heartbeat) = validated_heartbeats.next().await.transpose()? {
        validated_heartbeat.write(file_sink).await?;

        if !validated_heartbeat.is_valid() {
            continue;
        }

        let key = validated_heartbeat.report.id()?;
        if cache.get(&key).await.is_none() {
            validated_heartbeat.save(&mut transaction).await?;
            cache
                .insert(key, (), time::Duration::from_secs(60 * 60 * 2))
                .await;
        }
    }
    file_sink.commit().await?;
    transaction.commit().await?;
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
