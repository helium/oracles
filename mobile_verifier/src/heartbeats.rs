use crate::cell_type::{CellType, CellTypeLabel};
use anyhow::anyhow;
use chrono::{DateTime, Duration, DurationRound, RoundingError, Utc};
use file_store::{
    file_info_poller::FileInfoStream, file_sink::FileSinkClient,
    heartbeat::CellHeartbeatIngestReport, wifi_heartbeat::WifiHeartbeatIngestReport,
};
use futures::{
    stream::{Stream, StreamExt, TryStreamExt},
    TryFutureExt,
};
use helium_crypto::PublicKeyBinary;
use helium_proto::services::poc_mobile as proto;
use mobile_config::{client::ClientError, gateway_info::GatewayInfoResolver, GatewayClient};
use retainer::Cache;
use rust_decimal::{prelude::ToPrimitive, Decimal};
use sqlx::{Postgres, Transaction};
use std::{ops::Range, pin::pin, sync::Arc, time};
use tokio::sync::mpsc::Receiver;

/// Minimum number of heartbeats required to give a reward to the hotspot.
const MINIMUM_CELL_HEARTBEAT_COUNT: i64 = 12;
const MINIMUM_WIFI_HEARTBEAT_COUNT: i64 = 12;

#[derive(Clone)]
pub enum HBType {
    Cell = 0,
    Wifi = 1,
}

#[derive(Clone)]
pub struct ValidatedHeartbeat {
    pub report: Heartbeat,
    pub cell_type: CellType,
    pub validity: proto::HeartbeatValidity,
}

#[derive(Clone)]
pub struct Heartbeat {
    pub hb_type: HBType,
    pub hotspot_key: PublicKeyBinary,
    pub cbsd_id: Option<String>,
    pub operation_mode: bool,
    pub location_validation_timestamp: Option<DateTime<Utc>>,
    pub timestamp: DateTime<Utc>,
}

impl Heartbeat {
    fn truncated_timestamp(&self) -> Result<DateTime<Utc>, RoundingError> {
        self.timestamp.duration_trunc(Duration::hours(1))
    }

    fn id(&self) -> anyhow::Result<(String, DateTime<Utc>)> {
        let cbsd_id = self
            .cbsd_id
            .clone()
            .ok_or_else(|| anyhow!("expected cbsd_id, found none"))?;
        let ts = self.truncated_timestamp()?;
        match self.hb_type {
            HBType::Cell => Ok((cbsd_id, ts)),
            HBType::Wifi => Ok((self.hotspot_key.to_string(), ts)),
        }
    }
}

impl From<CellHeartbeatIngestReport> for Heartbeat {
    fn from(value: CellHeartbeatIngestReport) -> Self {
        Self {
            hb_type: HBType::Cell,
            hotspot_key: value.report.pubkey,
            cbsd_id: Some(value.report.cbsd_id),
            operation_mode: value.report.operation_mode,
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
            location_validation_timestamp: value.report.location_validation_timestamp,
            timestamp: value.received_timestamp,
        }
    }
}

pub struct HeartbeatDaemon {
    pool: sqlx::Pool<sqlx::Postgres>,
    gateway_client: GatewayClient,
    heartbeats: Receiver<FileInfoStream<CellHeartbeatIngestReport>>,
    wifi_heartbeats: Receiver<FileInfoStream<WifiHeartbeatIngestReport>>,
    file_sink: FileSinkClient,
}

impl HeartbeatDaemon {
    pub fn new(
        pool: sqlx::Pool<sqlx::Postgres>,
        gateway_client: GatewayClient,
        heartbeats: Receiver<FileInfoStream<CellHeartbeatIngestReport>>,
        wifi_heartbeats: Receiver<FileInfoStream<WifiHeartbeatIngestReport>>,
        file_sink: FileSinkClient,
    ) -> Self {
        Self {
            pool,
            gateway_client,
            heartbeats,
            wifi_heartbeats,
            file_sink,
        }
    }

    pub async fn run(mut self, shutdown: triggered::Listener) -> anyhow::Result<()> {
        tokio::spawn(async move {
            let cache = Arc::new(Cache::<(String, DateTime<Utc>), ()>::new());

            let cache_clone = cache.clone();
            tokio::spawn(async move {
                cache_clone
                    .monitor(4, 0.25, time::Duration::from_secs(60 * 60 * 3))
                    .await
            });

            loop {
                tokio::select! {
                    biased;
                    _ = shutdown.clone() => {
                        tracing::info!("HeartbeatDaemon shutting down");
                        break;
                    }
                    Some(file) = self.heartbeats.recv() => self.process_cell_hb_file(file, &cache).await?,
                    Some(file) = self.wifi_heartbeats.recv() => self.process_wifi_hb_file(file, &cache).await?,
                }
            }

            Ok(())
        })
        .map_err(anyhow::Error::from)
        .and_then(|result| async move { result })
        .await
    }

    async fn process_cell_hb_file(
        &self,
        file: FileInfoStream<CellHeartbeatIngestReport>,
        cache: &Cache<(String, DateTime<Utc>), ()>,
    ) -> anyhow::Result<()> {
        tracing::info!("Processing cell heartbeat file {}", file.file_info.key);
        let mut transaction = self.pool.begin().await?;
        let epoch = (file.file_info.timestamp - Duration::hours(3))
            ..(file.file_info.timestamp + Duration::minutes(30));
        // map the ingest reports to our generic heartbeat  type
        let reports = file
            .into_stream(&mut transaction)
            .await?
            .map(Heartbeat::from);
        self.process_hbs(reports, cache, transaction, &epoch).await
    }

    async fn process_wifi_hb_file(
        &self,
        file: FileInfoStream<WifiHeartbeatIngestReport>,
        cache: &Cache<(String, DateTime<Utc>), ()>,
    ) -> anyhow::Result<()> {
        tracing::info!("Processing wifi heartbeat file {}", file.file_info.key);
        let mut transaction = self.pool.begin().await?;
        let epoch = (file.file_info.timestamp - Duration::hours(3))
            ..(file.file_info.timestamp + Duration::minutes(30));
        // map the ingest reports to our generic heartbeat  type
        let reports = file
            .into_stream(&mut transaction)
            .await?
            .map(Heartbeat::from);
        self.process_hbs(reports, cache, transaction, &epoch).await
    }

    async fn process_hbs<'a>(
        &self,
        reports: impl Stream<Item = Heartbeat> + 'a,
        cache: &Cache<(String, DateTime<Utc>), ()>,
        mut transaction: Transaction<'_, Postgres>,
        epoch: &'a Range<DateTime<Utc>>,
    ) -> anyhow::Result<()> {
        let mut validated_heartbeats = pin!(
            ValidatedHeartbeat::validate_heartbeats(&self.gateway_client, reports, epoch).await
        );

        while let Some(validated_heartbeat) = validated_heartbeats.next().await.transpose()? {
            validated_heartbeat.write(&self.file_sink).await?;

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
        self.file_sink.commit().await?;
        transaction.commit().await?;
        Ok(())
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
}

#[derive(Debug, Clone, PartialEq)]
pub struct HeartbeatReward {
    pub hotspot_key: PublicKeyBinary,
    pub cell_type: CellType,
    // cell hb only
    pub cbsd_id: Option<String>,
    pub reward_weight: Decimal,
}

impl From<HeartbeatRow> for HeartbeatReward {
    fn from(value: HeartbeatRow) -> Self {
        Self {
            hotspot_key: value.hotspot_key,
            cell_type: value.cell_type,
            cbsd_id: value.cbsd_id,
            reward_weight: value.cell_type.reward_weight()
                * value
                    .cell_type
                    .location_weight(value.location_validation_timestamp),
        }
    }
}

impl HeartbeatReward {
    pub fn id(&self) -> anyhow::Result<String> {
        match self.cell_type.to_label() {
            CellTypeLabel::Cell => Ok(self
                .cbsd_id
                .clone()
                .ok_or_else(|| anyhow!("expected cbsd_id, found none"))?),
            CellTypeLabel::Wifi => Ok(self.hotspot_key.to_string()),
            _ => Err(anyhow!("failed to derive label from cell type")),
        }
    }

    pub fn validated<'a>(
        exec: impl sqlx::PgExecutor<'a> + Copy + 'a,
        epoch: &'a Range<DateTime<Utc>>,
    ) -> impl Stream<Item = Result<HeartbeatReward, sqlx::Error>> + 'a {
        sqlx::query_as::<_, HeartbeatRow>(
            r#"
            WITH latest_hotspots AS (
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
              NULL as location_validation_timestamp
            FROM heartbeats
            JOIN latest_hotspots ON heartbeats.cbsd_id = latest_hotspots.cbsd_id
            WHERE truncated_timestamp >= $1
            	AND truncated_timestamp < $2
            GROUP BY
              heartbeats.cbsd_id,
              latest_hotspots.hotspot_key,
              cell_type
            HAVING count(*) >= $3
            UNION
            SELECT hotspot_key, NULL as cbsd_id, cell_type, location_validation_timestamp
            FROM wifi_heartbeats
            WHERE truncated_timestamp >= $1
            	and truncated_timestamp < $2
            GROUP BY hotspot_key, location_validation_timestamp, cell_type
            HAVING count(hotspot_key) >= $4
            "#,
        )
        .bind(epoch.start)
        .bind(epoch.end)
        .bind(MINIMUM_CELL_HEARTBEAT_COUNT)
        .bind(MINIMUM_WIFI_HEARTBEAT_COUNT)
        .fetch(exec)
        .map_ok(HeartbeatReward::from)
    }
}

#[derive(sqlx::FromRow)]
struct HeartbeatSaveResult {
    inserted: bool,
}

impl ValidatedHeartbeat {
    pub fn is_valid(&self) -> bool {
        self.validity == proto::HeartbeatValidity::Valid
    }

    fn truncated_timestamp(&self) -> Result<DateTime<Utc>, RoundingError> {
        self.report.timestamp.duration_trunc(Duration::hours(1))
    }

    async fn validate_heartbeats<'a>(
        gateway_client: &'a GatewayClient,
        heartbeats: impl Stream<Item = Heartbeat> + 'a,
        epoch: &'a Range<DateTime<Utc>>,
    ) -> impl Stream<Item = Result<Self, ClientError>> + 'a {
        heartbeats.then(move |report| {
            let mut gateway_client = gateway_client.clone();
            async move {
                let (cell_type, validity) =
                    validate_heartbeat(&report, &mut gateway_client, epoch).await?;
                Ok(Self {
                    report,
                    cell_type,
                    validity,
                })
            }
        })
    }

    async fn write(&self, heartbeats: &FileSinkClient) -> file_store::Result {
        heartbeats
            .write(
                proto::Heartbeat {
                    cbsd_id: self.report.cbsd_id.clone().unwrap_or(String::new()),
                    pub_key: self.report.hotspot_key.as_ref().into(),
                    reward_multiplier: self.cell_type.reward_weight().to_f32().unwrap_or(0.0),
                    // NOTE: previously cell_type would default to `Neutrino430`, if no cell type value was set
                    // cell type is now always set to a value
                    // and will carry through a None value to the proto
                    // TODO: Verify this change doesnt have any unconsidered side effects
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
                },
                &[("validity", self.validity.as_str_name())],
            )
            .await?;
        Ok(())
    }

    async fn save(self, exec: &mut Transaction<'_, Postgres>) -> anyhow::Result<bool> {
        // If the heartbeat is not valid, do not save it
        if self.validity != proto::HeartbeatValidity::Valid {
            return Ok(false);
        }
        match self.report.hb_type {
            HBType::Cell => self.save_cell_hb(exec).await,
            HBType::Wifi => self.save_wifi_hb(exec).await,
        }
    }

    async fn save_cell_hb(self, exec: &mut Transaction<'_, Postgres>) -> anyhow::Result<bool> {
        let cbsd_id = self
            .report
            .cbsd_id
            .as_ref()
            .ok_or_else(|| anyhow!("failed to save cell heartbeat, invalid cbsd_id"))?;

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
                INSERT INTO wifi_heartbeats (hotspot_key, cell_type, location_validation_timestamp,
                    latest_timestamp, truncated_timestamp)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (truncated_timestamp) DO UPDATE SET
                latest_timestamp = EXCLUDED.latest_timestamp
                RETURNING (xmax = 0) as inserted
                "#,
        )
        .bind(self.report.hotspot_key)
        .bind(self.cell_type)
        .bind(self.report.location_validation_timestamp)
        .bind(self.report.timestamp)
        .bind(truncated_timestamp)
        .fetch_one(&mut *exec)
        .await?
        .inserted)
    }
}

/// Validate a heartbeat in the given epoch.
async fn validate_heartbeat(
    heartbeat: &Heartbeat,
    gateway_client: &mut GatewayClient,
    epoch: &Range<DateTime<Utc>>,
) -> Result<(CellType, proto::HeartbeatValidity), ClientError> {
    // todo: a better way to write this using combinators ?
    // cant get it to handle the early non error return
    let cell_type = match heartbeat.hb_type {
        HBType::Cell => match heartbeat.cbsd_id.as_ref() {
            Some(cbsd_id) => match CellType::from_cbsd_id(cbsd_id) {
                Some(ty) => ty,
                _ => return Ok((CellType::CellTypeNone, proto::HeartbeatValidity::BadCbsdId)),
            },
            None => return Ok((CellType::CellTypeNone, proto::HeartbeatValidity::BadCbsdId)),
        },
        // for wifi HBs temporary assume we have an indoor wifi spot
        // this will be better/properly handled when coverage reports are live
        HBType::Wifi => CellType::NovaGenericWifiIndoor,
    };

    if !heartbeat.operation_mode {
        return Ok((cell_type, proto::HeartbeatValidity::NotOperational));
    }

    if !epoch.contains(&heartbeat.timestamp) {
        return Ok((cell_type, proto::HeartbeatValidity::HeartbeatOutsideRange));
    }

    if gateway_client
        .resolve_gateway_info(&heartbeat.hotspot_key)
        .await?
        .is_none()
    {
        return Ok((cell_type, proto::HeartbeatValidity::GatewayOwnerNotFound));
    }

    Ok((cell_type, proto::HeartbeatValidity::Valid))
}

pub async fn clear_heartbeats(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    timestamp: &DateTime<Utc>,
) -> Result<(), sqlx::Error> {
    sqlx::query("DELETE FROM heartbeats WHERE truncated_timestamp < $1;")
        .bind(timestamp)
        .execute(&mut *tx)
        .await?;

    sqlx::query("DELETE FROM wifi_heartbeats WHERE truncated_timestamp < $1;")
        .bind(timestamp)
        .execute(&mut *tx)
        .await?;

    Ok(())
}
