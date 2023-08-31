//! Heartbeat storage

use crate::{
    cell_type::CellType,
    coverage::{CoverageClaimTimeCache, CoveredHexCache, Seniority},
    HasOwner,
};
use chrono::{DateTime, Duration, DurationRound, RoundingError, Utc};
use file_store::{
    file_info_poller::FileInfoStream,
    file_sink::FileSinkClient,
    heartbeat::{CellHeartbeat, CellHeartbeatIngestReport},
};
use futures::{
    stream::{Stream, StreamExt, TryStreamExt},
    TryFutureExt,
};
use h3o::LatLng;
use helium_crypto::PublicKeyBinary;
use helium_proto::services::poc_mobile as proto;
use mobile_config::GatewayClient;
use retainer::Cache;
use rust_decimal::{prelude::ToPrimitive, Decimal};
use sqlx::{Postgres, Transaction};
use std::{ops::Range, pin::pin, sync::Arc, time};
use tokio::sync::mpsc::Receiver;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq, Hash, sqlx::FromRow)]
pub struct HeartbeatKey {
    coverage_object: Uuid,
    hotspot_key: PublicKeyBinary,
    cbsd_id: String,
    cell_type: CellType,
    latest_timestamp: DateTime<Utc>,
}

impl From<HeartbeatKey> for HeartbeatReward {
    fn from(value: HeartbeatKey) -> Self {
        Self {
            coverage_object: value.coverage_object,
            hotspot_key: value.hotspot_key,
            cbsd_id: value.cbsd_id,
            reward_weight: value.cell_type.reward_weight(),
            latest_timestamp: value.latest_timestamp,
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
    modeled_coverage_start: DateTime<Utc>,
}

impl HeartbeatDaemon {
    pub fn new(
        pool: sqlx::Pool<sqlx::Postgres>,
        gateway_client: GatewayClient,
        heartbeats: Receiver<FileInfoStream<CellHeartbeatIngestReport>>,
        heartbeat_sink: FileSinkClient,
        seniority_sink: FileSinkClient,
        max_distance: f64,
        modeled_coverage_start: DateTime<Utc>,
    ) -> Self {
        Self {
            pool,
            gateway_client,
            heartbeats,
            heartbeat_sink,
            seniority_sink,
            max_distance,
            modeled_coverage_start,
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
            heartbeat.write(&self.heartbeat_sink).await?;

            if !heartbeat.is_valid() {
                continue;
            }

            if let Some(coverage_claim_time) = coverage_claim_time_cache
                .fetch_coverage_claim_time(
                    &heartbeat.heartbeat.cbsd_id,
                    &heartbeat.coverage_object,
                    &mut transaction,
                )
                .await?
            {
                let latest_seniority =
                    Seniority::fetch_latest(&mut transaction, &heartbeat.heartbeat.cbsd_id).await?;
                let seniority_update = SeniorityUpdate::determine_update_action(
                    &heartbeat,
                    coverage_claim_time,
                    self.modeled_coverage_start,
                    latest_seniority,
                );
                seniority_update.write(&self.seniority_sink).await?;
                seniority_update.execute(&mut transaction).await?;
            }

            let key = (
                heartbeat.heartbeat.cbsd_id.clone(),
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

#[derive(Clone, Debug, PartialEq)]
pub struct HeartbeatReward {
    pub coverage_object: Uuid,
    pub hotspot_key: PublicKeyBinary,
    pub cbsd_id: String,
    pub reward_weight: Decimal,
    pub latest_timestamp: DateTime<Utc>,
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
            WITH coverage_objs AS (
                SELECT t1.cbsd_id, t1.coverage_object, t1.latest_timestamp
                FROM heartbeats t1
                WHERE t1.latest_timestamp = (
                       SELECT MAX(t2.latest_timestamp)
                       FROM heartbeats t2
                       WHERE t2.cbsd_id = t1.cbsd_id
                         AND truncated_timestamp >= $1
                         AND truncated_timestamp < $2
                )
            ), latest_hotspots AS (
                 SELECT t1.cbsd_id, t1.hotspot_key, t1.latest_timestamp
                 FROM heartbeats t1
                 WHERE t1.latest_timestamp = (
                       SELECT MAX(t2.latest_timestamp)
                       FROM heartbeats t2
                       WHERE t2.cbsd_id = t1.cbsd_id
                         AND truncated_timestamp >= $1
                         AND truncated_timestamp < $2
                )
            )
            SELECT
              latest_hotspots.hotspot_key,
              heartbeats.cbsd_id,
              cell_type,
              coverage_objs.coverage_object,
              coverage_objs.latest_timestamp
            FROM heartbeats
              LEFT JOIN latest_hotspots ON heartbeats.cbsd_id = latest_hotspots.cbsd_id
              LEFT JOIN coverage_objs ON heartbeats.cbsd_id = coverage_objs.cbsd_id 
            WHERE truncated_timestamp >= $1
            	AND truncated_timestamp < $2
            GROUP BY
              heartbeats.cbsd_id,
              latest_hotspots.hotspot_key,
              cell_type,
              coverage_objs.coverage_object,
              coverage_objs.latest_timestamp
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
    pub heartbeat: CellHeartbeat,
    pub cell_type: Option<CellType>,
    pub coverage_object: Option<Uuid>,
    pub received_timestamp: DateTime<Utc>,
    pub validity: proto::HeartbeatValidity,
}

impl Heartbeat {
    pub fn is_valid(&self) -> bool {
        self.validity == proto::HeartbeatValidity::Valid
    }

    pub fn truncated_timestamp(&self) -> Result<DateTime<Utc>, RoundingError> {
        self.received_timestamp.duration_trunc(Duration::hours(1))
    }

    pub fn validate_heartbeats<'a>(
        gateway_client: &'a impl HasOwner,
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
                heartbeat: heartbeat_report.report,
                received_timestamp: heartbeat_report.received_timestamp,
                cell_type,
                validity,
            })
        })
    }

    pub async fn write(&self, heartbeats: &FileSinkClient) -> file_store::Result {
        heartbeats
            .write(
                proto::Heartbeat {
                    cbsd_id: self.heartbeat.cbsd_id.clone(),
                    pub_key: self.heartbeat.pubkey.clone().into(),
                    reward_multiplier: self
                        .cell_type
                        .map_or(0.0, |ct| ct.reward_weight().to_f32().unwrap_or(0.0)),
                    cell_type: self.cell_type.unwrap_or(CellType::Neutrino430) as i32, // Is this the right default?
                    validity: self.validity as i32,
                    timestamp: self.received_timestamp.timestamp() as u64,
                    lat: self.heartbeat.lat,
                    lon: self.heartbeat.lon,
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

    pub async fn save(self, exec: &mut Transaction<'_, Postgres>) -> anyhow::Result<bool> {
        let truncated_timestamp = self.truncated_timestamp()?;
        Ok(
            sqlx::query_scalar(
                r#"
                INSERT INTO heartbeats (cbsd_id, hotspot_key, cell_type, latest_timestamp, truncated_timestamp, coverage_object)
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (cbsd_id, truncated_timestamp) DO UPDATE SET
                latest_timestamp = EXCLUDED.latest_timestamp,
                coverage_object = EXCLUDED.coverage_object
                RETURNING (xmax = 0) as inserted
                "#
            )
            .bind(self.heartbeat.cbsd_id)
            .bind(self.heartbeat.pubkey)
            .bind(self.cell_type.unwrap())
            .bind(self.received_timestamp)
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
    gateway_client: &impl HasOwner,
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

    if gateway_client.has_owner(&heartbeat.report.pubkey).await? {
        return Ok((cell_type, proto::HeartbeatValidity::GatewayOwnerNotFound));
    }

    let Some(coverage_object) = heartbeat.report.coverage_object() else {
        return Ok((cell_type, proto::HeartbeatValidity::BadCoverageObject));
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

pub async fn clear_heartbeats(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    timestamp: &DateTime<Utc>,
) -> Result<(), sqlx::Error> {
    sqlx::query("DELETE FROM heartbeats WHERE truncated_timestamp < $1")
        .bind(timestamp)
        .execute(&mut *tx)
        .await?;
    Ok(())
}

pub struct SeniorityUpdate<'a> {
    heartbeat: &'a Heartbeat,
    action: SeniorityUpdateAction,
}

#[derive(Debug, PartialEq)]
enum SeniorityUpdateAction {
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
    fn new(heartbeat: &'a Heartbeat, action: SeniorityUpdateAction) -> Self {
        Self { heartbeat, action }
    }

    pub fn determine_update_action(
        heartbeat: &'a Heartbeat,
        coverage_claim_time: DateTime<Utc>,
        modeled_coverage_start: DateTime<Utc>,
        latest_seniority: Option<Seniority>,
    ) -> Self {
        use proto::SeniorityUpdateReason::*;

        if let Some(prev_seniority) = latest_seniority {
            if heartbeat.coverage_object != Some(prev_seniority.uuid) {
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
            } else if heartbeat.received_timestamp - prev_seniority.last_heartbeat
                > Duration::days(3)
                && coverage_claim_time < heartbeat.received_timestamp
            {
                Self::new(
                    heartbeat,
                    SeniorityUpdateAction::Insert {
                        new_seniority: heartbeat.received_timestamp,
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
        } else if heartbeat.received_timestamp - modeled_coverage_start > Duration::days(3) {
            // This will become the default case 72 hours after we launch modeled coverage
            Self::new(
                heartbeat,
                SeniorityUpdateAction::Insert {
                    new_seniority: heartbeat.received_timestamp,
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
                        cbsd_id: self.heartbeat.heartbeat.cbsd_id.to_string(),
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
                      (cbsd_id, last_heartbeat, uuid, seniority_ts, inserted_at, update_reason)
                    VALUES
                      ($1, $2, $3, $4, $5, $6)
                    "#,
                )
                .bind(&self.heartbeat.heartbeat.cbsd_id)
                .bind(self.heartbeat.received_timestamp)
                .bind(self.heartbeat.coverage_object)
                .bind(new_seniority)
                .bind(self.heartbeat.received_timestamp)
                .bind(update_reason as i32)
                .execute(&mut *exec)
                .await?;
            }
            SeniorityUpdateAction::Update { curr_seniority } => {
                sqlx::query(
                    r#"
                    UPDATE seniority
                    SET last_heartbeat = $1
                    WHERE
                      cbsd_id = $2 AND
                      seniority_ts = $3
                    "#,
                )
                .bind(self.heartbeat.received_timestamp)
                .bind(&self.heartbeat.heartbeat.cbsd_id)
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

    fn heartbeat(timestamp: DateTime<Utc>, coverage_object: Uuid) -> Heartbeat {
        Heartbeat {
            received_timestamp: timestamp,
            cell_type: None,
            coverage_object: Some(coverage_object),
            heartbeat: CellHeartbeat {
                pubkey: PublicKeyBinary::from(Vec::new()),
                hotspot_type: "".to_string(),
                cell_id: 0,
                timestamp,
                lon: 0.0,
                lat: 0.0,
                operation_mode: false,
                cbsd_category: "".to_string(),
                cbsd_id: "".to_string(),
                coverage_object: Vec::new(),
            },
            validity: Default::default(),
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
