//! Heartbeat storage

use crate::cell_type::CellType;
use chrono::{DateTime, Duration, DurationRound, RoundingError, Utc};
use file_store::{
    file_info_poller::FileInfoStream, file_sink::FileSinkClient,
    heartbeat::CellHeartbeatIngestReport,
};
use futures::{
    stream::{Stream, StreamExt, TryStreamExt},
    TryFutureExt,
};
use helium_crypto::PublicKeyBinary;
use helium_proto::services::poc_mobile as proto;
use mobile_config::{client::ClientError, gateway_info::GatewayInfoResolver, Client};
use retainer::Cache;
use rust_decimal::{prelude::ToPrimitive, Decimal};
use sqlx::{Postgres, Transaction};
use std::{ops::Range, pin::pin, sync::Arc, time};
use tokio::sync::mpsc::Receiver;

#[derive(Debug, Clone, PartialEq, Eq, Hash, sqlx::FromRow)]
pub struct HeartbeatKey {
    hotspot_key: PublicKeyBinary,
    cbsd_id: String,
    cell_type: CellType,
}

pub struct HeartbeatReward {
    pub hotspot_key: PublicKeyBinary,
    pub cbsd_id: String,
    pub reward_weight: Decimal,
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
    config_client: Client,
    heartbeats: Receiver<FileInfoStream<CellHeartbeatIngestReport>>,
    file_sink: FileSinkClient,
}

impl HeartbeatDaemon {
    pub fn new(
        pool: sqlx::Pool<sqlx::Postgres>,
        config_client: Client,
        heartbeats: Receiver<FileInfoStream<CellHeartbeatIngestReport>>,
        file_sink: FileSinkClient,
    ) -> Self {
        Self {
            pool,
            config_client,
            heartbeats,
            file_sink,
        }
    }

    pub async fn run(mut self, shutdown: triggered::Listener) -> anyhow::Result<()> {
        tokio::spawn(async move {
            let cache = Arc::new(Cache::<(String, DateTime<Utc>), ()>::new());

            let cache_clone = cache.clone();
            tokio::spawn(async move {
                cache_clone
                    .monitor(4, 0.25, std::time::Duration::from_secs(60 * 60 * 3))
                    .await
            });

            loop {
                tokio::select! {
                    _ = shutdown.clone() => {
                        tracing::info!("HeartbeatDaemon shutting down");
                        break;
                    }
                    Some(file) = self.heartbeats.recv() => self.process_file(file, &cache).await?,
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
        cache: &Cache<(String, DateTime<Utc>), ()>,
    ) -> anyhow::Result<()> {
        tracing::info!("Processing heartbeat file {}", file.file_info.key);

        let epoch = (file.file_info.timestamp - Duration::hours(3))
            ..(file.file_info.timestamp + Duration::minutes(30));
        let mut transaction = self.pool.begin().await?;
        let reports = file.into_stream(&mut transaction).await?;

        let mut validated_heartbeats =
            pin!(Heartbeat::validate_heartbeats(&self.config_client, reports, &epoch).await);

        while let Some(heartbeat) = validated_heartbeats.next().await.transpose()? {
            heartbeat.write(&self.file_sink).await?;
            let key = (heartbeat.cbsd_id.clone(), heartbeat.truncated_timestamp()?);

            if cache.get(&key).await.is_none() {
                heartbeat.save(&mut transaction).await?;
                cache
                    .insert(key, (), time::Duration::from_secs(60 * 60 * 2))
                    .await;
            }
        }

        self.file_sink.commit().await?;
        transaction.commit().await?;

        Ok(())
    }
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
            SELECT hotspot_key, cbsd_id, cell_type
            FROM heartbeats
            WHERE truncated_timestamp >= $1
            	and truncated_timestamp < $2
            GROUP BY cbsd_id, hotspot_key, cell_type
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
    pub validity: proto::HeartbeatValidity,
}

#[derive(sqlx::FromRow)]
struct HeartbeatSaveResult {
    inserted: bool,
}

#[derive(thiserror::Error, Debug)]
pub enum SaveHeartbeatError {
    #[error("rounding error: {0}")]
    RoundingError(#[from] RoundingError),
    #[error("sql error: {0}")]
    SqlError(#[from] sqlx::Error),
}

impl Heartbeat {
    pub fn truncated_timestamp(&self) -> Result<DateTime<Utc>, RoundingError> {
        self.timestamp.duration_trunc(Duration::hours(1))
    }

    pub async fn validate_heartbeats<'a>(
        config_client: &'a Client,
        heartbeats: impl Stream<Item = CellHeartbeatIngestReport> + 'a,
        epoch: &'a Range<DateTime<Utc>>,
    ) -> impl Stream<Item = Result<Self, ClientError>> + 'a {
        heartbeats.then(move |heartbeat_report| {
            let mut config_client = config_client.clone();
            async move {
                let (cell_type, validity) =
                    validate_heartbeat(&heartbeat_report, &mut config_client, epoch).await?;
                Ok(Heartbeat {
                    hotspot_key: heartbeat_report.report.pubkey,
                    cbsd_id: heartbeat_report.report.cbsd_id,
                    timestamp: heartbeat_report.received_timestamp,
                    cell_type,
                    validity,
                })
            }
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
                },
                [],
            )
            .await?;
        Ok(())
    }

    pub async fn save(
        self,
        exec: &mut Transaction<'_, Postgres>,
    ) -> Result<bool, SaveHeartbeatError> {
        // If the heartbeat is not valid, do not save it
        if self.validity != proto::HeartbeatValidity::Valid {
            return Ok(false);
        }
        let truncated_timestamp = self.truncated_timestamp()?;
        Ok(
            sqlx::query_as::<_, HeartbeatSaveResult>(
                r#"
                INSERT INTO heartbeats (cbsd_id, hotspot_key, cell_type, latest_timestamp, truncated_timestamp)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (cbsd_id, truncated_timestamp) DO UPDATE SET
                latest_timestamp = EXCLUDED.latest_timestamp
                RETURNING (xmax = 0) as inserted
                "#
            )
            .bind(self.cbsd_id)
            .bind(self.hotspot_key)
            .bind(self.cell_type.unwrap())
            .bind(self.timestamp)
            .bind(truncated_timestamp)
            .fetch_one(&mut *exec)
            .await?
            .inserted
        )
    }
}

/// Validate a heartbeat in the given epoch.
async fn validate_heartbeat(
    heartbeat: &CellHeartbeatIngestReport,
    config_client: &mut Client,
    epoch: &Range<DateTime<Utc>>,
) -> Result<(Option<CellType>, proto::HeartbeatValidity), ClientError> {
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

    if config_client
        .resolve_gateway_info(&heartbeat.report.pubkey)
        .await?
        .is_none()
    {
        return Ok((cell_type, proto::HeartbeatValidity::GatewayOwnerNotFound));
    }

    Ok((cell_type, proto::HeartbeatValidity::Valid))
}
