//! Heartbeat storage

use crate::{
    cell_type::CellType,
    coverage::{CoverageClaimTimeCache, CoveredHexCache},
};
use chrono::{DateTime, Duration, DurationRound, RoundingError, Utc};
use file_store::{
    file_info_poller::FileInfoStream, file_sink::FileSinkClient,
    heartbeat::CellHeartbeatIngestReport,
};
use futures::{
    stream::{Stream, StreamExt, TryStreamExt},
    TryFutureExt,
};
use h3o::LatLng;
use helium_crypto::PublicKeyBinary;
use helium_proto::services::poc_mobile as proto;
use mobile_config::{gateway_info::GatewayInfoResolver, GatewayClient};
use retainer::Cache;
use rust_decimal::{prelude::ToPrimitive, Decimal};
use sqlx::{Postgres, Transaction};
use std::{ops::Range, pin::pin, sync::Arc, time};
use tokio::sync::mpsc::Receiver;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq, Hash, sqlx::FromRow)]
pub struct HeartbeatKey {
    hotspot_key: PublicKeyBinary,
    cbsd_id: String,
    cell_type: CellType,
}

impl From<HeartbeatKey> for HeartbeatReward {
    fn from(value: HeartbeatKey) -> Self {
        Self {
            hotspot_key: value.hotspot_key,
            cbsd_id: value.cbsd_id,
            reward_weight: value.cell_type.reward_weight(),
        }
    }
}

pub struct HeartbeatDaemon {
    pool: sqlx::Pool<sqlx::Postgres>,
    gateway_client: GatewayClient,
    heartbeats: Receiver<FileInfoStream<CellHeartbeatIngestReport>>,
    heartbeat_sink: FileSinkClient,
    seniority_sink: FileSinkClient,
    max_distance: f64,
}

impl HeartbeatDaemon {
    pub fn new(
        pool: sqlx::Pool<sqlx::Postgres>,
        gateway_client: GatewayClient,
        heartbeats: Receiver<FileInfoStream<CellHeartbeatIngestReport>>,
        heartbeat_sink: FileSinkClient,
        seniority_sink: FileSinkClient,
        max_distance: f64,
    ) -> Self {
        Self {
            pool,
            gateway_client,
            heartbeats,
            heartbeat_sink,
            seniority_sink,
            max_distance,
        }
    }

    pub async fn run(mut self, shutdown: triggered::Listener) -> anyhow::Result<()> {
        tokio::spawn(async move {
            let heartbeat_cache =
                Arc::new(Cache::<(String, DateTime<Utc>, Option<Uuid>), ()>::new());

            let heartbeat_cache_clone = heartbeat_cache.clone();
            tokio::spawn(async move {
                heartbeat_cache_clone
                    .monitor(4, 0.25, std::time::Duration::from_secs(60 * 60 * 3))
                    .await
            });

            let coverage_claim_time_cache = CoverageClaimTimeCache::new();
            let covered_hex_cache = CoveredHexCache::new(&self.pool);

            loop {
                tokio::select! {
                    _ = shutdown.clone() => {
                        tracing::info!("HeartbeatDaemon shutting down");
                        break;
                    }
                    Some(file) = self.heartbeats.recv() => {
                        self.process_file(
                            file,
                            &heartbeat_cache,
                            &coverage_claim_time_cache,
                            &covered_hex_cache
                        ).await?;
                    }
                }
            }

            Ok(())
        })
        .map_err(anyhow::Error::from)
        .and_then(|result| async move { result })
        .await
    }

    async fn process_file(
        &self,
        file: FileInfoStream<CellHeartbeatIngestReport>,
        heartbeat_cache: &Cache<(String, DateTime<Utc>, Option<Uuid>), ()>,
        coverage_claim_time_cache: &CoverageClaimTimeCache,
        covered_hex_cache: &CoveredHexCache,
    ) -> anyhow::Result<()> {
        tracing::info!("Processing heartbeat file {}", file.file_info.key);

        let epoch = (file.file_info.timestamp - Duration::hours(3))
            ..(file.file_info.timestamp + Duration::minutes(30));
        let mut transaction = self.pool.begin().await?;
        let reports = file.into_stream(&mut transaction).await?;

        let mut validated_heartbeats = pin!(Heartbeat::validate_heartbeats(
            &self.gateway_client,
            covered_hex_cache,
            reports,
            &epoch,
            self.max_distance,
        ));

        while let Some(heartbeat) = validated_heartbeats.next().await.transpose()? {
            if heartbeat.coverage_object.is_some() {
                let coverage_claim_time = coverage_claim_time_cache
                    .fetch_coverage_claim_time(
                        &heartbeat.cbsd_id,
                        &heartbeat.coverage_object,
                        &mut transaction,
                    )
                    .await?;
                heartbeat
                    .update_seniority(coverage_claim_time, &self.seniority_sink, &mut transaction)
                    .await?;
            }

            heartbeat.write(&self.heartbeat_sink).await?;

            let key = (
                heartbeat.cbsd_id.clone(),
                heartbeat.truncated_timestamp()?,
                heartbeat.coverage_object,
            );
            if heartbeat_cache.get(&key).await.is_none() {
                heartbeat.save(&mut transaction).await?;
                heartbeat_cache
                    .insert(key, (), time::Duration::from_secs(60 * 60 * 2))
                    .await;
            }
        }

        self.heartbeat_sink.commit().await?;
        self.seniority_sink.commit().await?;
        transaction.commit().await?;

        Ok(())
    }
}

pub struct HeartbeatReward {
    pub hotspot_key: PublicKeyBinary,
    pub cbsd_id: String,
    pub reward_weight: Decimal,
}

/// Minimum number of heartbeats required to give a reward to the hotspot.
pub const MINIMUM_HEARTBEAT_COUNT: i64 = 12;

impl HeartbeatReward {
    pub fn validated<'a>(
        exec: impl sqlx::PgExecutor<'a> + Copy + 'a,
        epoch: &'a Range<DateTime<Utc>>,
    ) -> impl Stream<Item = Result<HeartbeatReward, sqlx::Error>> + 'a {
        sqlx::query_as::<_, HeartbeatKey>(
            r#"
            SELECT
              hotspot_key,
              heartbeats.cbsd_id,
              cell_type
            FROM
              heartbeats
            WHERE truncated_timestamp >= $1
            	AND truncated_timestamp < $2
            GROUP BY
              heartbeats.cbsd_id,
              hotspot_key,
              cell_type,
            HAVING count(*) >= $3
            "#,
        )
        .bind(epoch.start)
        .bind(epoch.end)
        .bind(MINIMUM_HEARTBEAT_COUNT)
        .fetch(exec)
        .map_ok(HeartbeatReward::from)
    }
}

#[derive(Clone)]
pub struct Heartbeat {
    pub cbsd_id: String,
    pub cell_type: Option<CellType>,
    pub hotspot_key: PublicKeyBinary,
    pub timestamp: DateTime<Utc>,
    pub coverage_object: Option<Uuid>,
    pub lat: f64,
    pub lon: f64,
    pub validity: proto::HeartbeatValidity,
}

impl Heartbeat {
    pub fn truncated_timestamp(&self) -> Result<DateTime<Utc>, RoundingError> {
        self.timestamp.duration_trunc(Duration::hours(1))
    }

    pub fn validate_heartbeats<'a>(
        gateway_client: &'a GatewayClient,
        covered_hex_cache: &'a CoveredHexCache,
        heartbeats: impl Stream<Item = CellHeartbeatIngestReport> + 'a,
        epoch: &'a Range<DateTime<Utc>>,
        max_distance: f64,
    ) -> impl Stream<Item = anyhow::Result<Self>> + 'a {
        heartbeats.then(move |heartbeat_report| async move {
            let (cell_type, validity) = validate_heartbeat(
                &heartbeat_report,
                gateway_client,
                covered_hex_cache,
                epoch,
                max_distance,
            )
            .await?;
            Ok(Heartbeat {
                coverage_object: heartbeat_report.report.coverage_object(),
                hotspot_key: heartbeat_report.report.pubkey,
                cbsd_id: heartbeat_report.report.cbsd_id,
                timestamp: heartbeat_report.received_timestamp,
                lat: heartbeat_report.report.lat,
                lon: heartbeat_report.report.lon,
                cell_type,
                validity,
            })
        })
    }

    pub async fn write(&self, heartbeats: &FileSinkClient) -> file_store::Result {
        heartbeats
            .write(
                proto::Heartbeat {
                    cbsd_id: self.cbsd_id.clone(),
                    pub_key: self.hotspot_key.clone().into(),
                    reward_multiplier: self
                        .cell_type
                        .map_or(0.0, |ct| ct.reward_weight().to_f32().unwrap_or(0.0)),
                    cell_type: self.cell_type.unwrap_or(CellType::Neutrino430) as i32, // Is this the right default?
                    validity: self.validity as i32,
                    timestamp: self.timestamp.timestamp() as u64,
                    lat: self.lat,
                    lon: self.lon,
                    coverage_object: self
                        .coverage_object
                        .map(|x| Vec::from(x.into_bytes()))
                        .unwrap_or_default(),
                },
                &[("validity", self.validity.as_str_name())],
            )
            .await?;
        Ok(())
    }

    pub async fn update_seniority(
        &self,
        coverage_claim_time: DateTime<Utc>,
        seniorities: &FileSinkClient,
        exec: &mut Transaction<'_, Postgres>,
    ) -> anyhow::Result<()> {
        enum InsertOrUpdate {
            Insert(proto::SeniorityUpdateReason),
            Update(DateTime<Utc>),
        }

        let (seniority_ts, update_reason) = if let Some(prev_seniority) =
            sqlx::query_as::<_, crate::coverage::Seniority>(
                "SELECT * FROM seniority WHERE cbsd_id = $1 ORDER BY last_heartbeat DESC LIMIT 1",
            )
            .bind(&self.cbsd_id)
            .fetch_optional(&mut *exec)
            .await?
        {
            if self.coverage_object != Some(prev_seniority.uuid) {
                (
                    coverage_claim_time,
                    InsertOrUpdate::Insert(proto::SeniorityUpdateReason::NewCoverageClaimTime),
                )
            } else if self.timestamp - prev_seniority.last_heartbeat > Duration::days(3)
                && coverage_claim_time < self.timestamp
            {
                (
                    self.timestamp,
                    InsertOrUpdate::Insert(proto::SeniorityUpdateReason::HeartbeatNotSeen),
                )
            } else {
                (
                    coverage_claim_time,
                    InsertOrUpdate::Update(prev_seniority.seniority_ts),
                )
            }
        } else {
            (
                coverage_claim_time,
                InsertOrUpdate::Insert(proto::SeniorityUpdateReason::NewCoverageClaimTime),
            )
        };

        match update_reason {
            InsertOrUpdate::Insert(update_reason) => {
                sqlx::query(
                    r#"
                    INSERT INTO seniority
                      (cbsd_id, last_heartbeat, uuid, seniority_ts, inserted_at)
                    VALUES
                      ($1, $2, $3, $4, $5)
                    "#,
                )
                .bind(&self.cbsd_id)
                .bind(self.timestamp)
                .bind(self.coverage_object)
                .bind(seniority_ts)
                .bind(self.timestamp)
                .execute(&mut *exec)
                .await?;
                seniorities
                    .write(
                        proto::SeniorityUpdate {
                            cbsd_id: self.cbsd_id.to_string(),
                            new_seniority_timestamp: seniority_ts.timestamp() as u64,
                            reason: update_reason as i32,
                        },
                        [],
                    )
                    .await?;
            }
            InsertOrUpdate::Update(seniority_ts) => {
                sqlx::query(
                    r#"
                    UPDATE seniority
                    SET last_heartbeat = $1
                    WHERE
                      cbsd_id = $2 AND
                      seniority_ts = $3
                    "#,
                )
                .bind(self.timestamp)
                .bind(&self.cbsd_id)
                .bind(seniority_ts)
                .execute(&mut *exec)
                .await?;
            }
        }

        Ok(())
    }

    pub async fn save(self, exec: &mut Transaction<'_, Postgres>) -> anyhow::Result<bool> {
        // If the heartbeat is not valid, do not save it
        if self.validity != proto::HeartbeatValidity::Valid {
            return Ok(false);
        }

        sqlx::query("DELETE FROM heartbeats WHERE cbsd_id = $1 AND hotspot_key != $2")
            .bind(&self.cbsd_id)
            .bind(&self.hotspot_key)
            .execute(&mut *exec)
            .await?;

        let truncated_timestamp = self.truncated_timestamp()?;
        Ok(
            sqlx::query_scalar(
                r#"
                INSERT INTO heartbeats (cbsd_id, hotspot_key, cell_type, latest_timestamp, truncated_timestamp, coverage_object)
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (cbsd_id, truncated_timestamp) DO UPDATE SET
                latest_timestamp = EXCLUDED.latest_timestamp
                coverage_object = EXCLUDED.coverage_object
                RETURNING (xmax = 0) as inserted
                "#
            )
            .bind(self.cbsd_id)
            .bind(self.hotspot_key)
            .bind(self.cell_type.unwrap())
            .bind(self.timestamp)
            .bind(truncated_timestamp)
            .bind(self.coverage_object)
            .fetch_one(&mut *exec)
            .await?
        )
    }
}

/// Validate a heartbeat in the given epoch.
async fn validate_heartbeat(
    heartbeat: &CellHeartbeatIngestReport,
    gateway_client: &GatewayClient,
    coverage_cache: &CoveredHexCache,
    epoch: &Range<DateTime<Utc>>,
    max_distance: f64,
) -> anyhow::Result<(Option<CellType>, proto::HeartbeatValidity)> {
    let cell_type = match CellType::from_cbsd_id(&heartbeat.report.cbsd_id) {
        Some(ty) => Some(ty),
        _ => return Ok((None, proto::HeartbeatValidity::BadCbsdId)),
    };

    if !heartbeat.report.operation_mode {
        return Ok((cell_type, proto::HeartbeatValidity::NotOperational));
    }

    if !epoch.contains(&heartbeat.received_timestamp) {
        return Ok((cell_type, proto::HeartbeatValidity::HeartbeatOutsideRange));
    }

    if gateway_client
        .resolve_gateway_info(&heartbeat.report.pubkey)
        .await?
        .is_none()
    {
        return Ok((cell_type, proto::HeartbeatValidity::GatewayOwnerNotFound));
    }

    let Some(coverage_object) = heartbeat.report.coverage_object() else {
        return Ok((cell_type, proto::HeartbeatValidity::Valid));
        // return Ok((cell_type, proto::HeartbeatValidity::BadCoverageObject));
    };

    let Some(coverage) = coverage_cache.fetch_coverage(&coverage_object).await? else {
        return Ok((cell_type, proto::HeartbeatValidity::NoSuchCoverageObject));
    };

    if coverage.cbsd_id != heartbeat.report.cbsd_id {
        return Ok((cell_type, proto::HeartbeatValidity::BadCoverageObject));
    }

    let Ok(latlng) = LatLng::new(heartbeat.report.lat, heartbeat.report.lon) else {
        return Ok((cell_type, proto::HeartbeatValidity::InvalidLatLon));
    };

    if coverage.max_distance_km(latlng) > max_distance {
        return Ok((cell_type, proto::HeartbeatValidity::TooFarFromCoverage));
    }

    Ok((cell_type, proto::HeartbeatValidity::Valid))
}
