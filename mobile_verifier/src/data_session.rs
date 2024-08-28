use chrono::{DateTime, Utc};
use file_store::{
    file_info_poller::{FileInfoStream, LookbackBehavior},
    file_source,
    mobile_transfer::ValidDataTransferSession,
    FileStore, FileType,
};
use futures::{
    stream::{Stream, StreamExt, TryStreamExt},
    TryFutureExt,
};
use helium_crypto::PublicKeyBinary;
use helium_proto::ServiceProvider;
use rust_decimal::Decimal;
use sqlx::{PgPool, Pool, Postgres, Row, Transaction};
use std::{collections::HashMap, ops::Range, time::Instant};
use task_manager::{ManagedTask, TaskManager};
use tokio::sync::mpsc::Receiver;

use crate::Settings;

pub struct DataSessionIngestor {
    pub receiver: Receiver<FileInfoStream<ValidDataTransferSession>>,
    pub pool: PgPool,
}

#[derive(Default)]
pub struct HotspotReward {
    pub rewardable_bytes: u64,
    pub rewardable_dc: u64,
}

#[derive(Clone, Debug)]
pub struct ServiceProviderDataSession {
    pub service_provider_id: ServiceProvider,
    pub service_provider_name: String,
    pub total_dcs: Decimal,
}

pub type HotspotMap = HashMap<PublicKeyBinary, HotspotReward>;

impl DataSessionIngestor {
    pub async fn create_managed_task(
        pool: Pool<Postgres>,
        settings: &Settings,
    ) -> anyhow::Result<impl ManagedTask> {
        let data_transfer_ingest = FileStore::from_settings(&settings.data_transfer_ingest).await?;
        // data transfers
        let (data_session_ingest, data_session_ingest_server) =
            file_source::continuous_source::<ValidDataTransferSession, _>()
                .state(pool.clone())
                .store(data_transfer_ingest)
                .lookback(LookbackBehavior::StartAfter(settings.start_after))
                .prefix(FileType::ValidDataTransferSession.to_string())
                .create()
                .await?;

        let data_session_ingestor = DataSessionIngestor::new(pool.clone(), data_session_ingest);

        Ok(TaskManager::builder()
            .add_task(data_session_ingest_server)
            .add_task(data_session_ingestor)
            .build())
    }

    pub fn new(
        pool: sqlx::Pool<sqlx::Postgres>,
        receiver: Receiver<FileInfoStream<ValidDataTransferSession>>,
    ) -> Self {
        Self { pool, receiver }
    }

    pub async fn run(mut self, shutdown: triggered::Listener) -> anyhow::Result<()> {
        tracing::info!("starting DataSessionIngestor");
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    biased;
                    _ = shutdown.clone() => {
                        tracing::info!("DataSessionIngestor shutting down");
                        break;
                    }
                    Some(file) = self.receiver.recv() => {
                        let start = Instant::now();
                        self.process_file(file).await?;
                        metrics::histogram!("valid_data_transfer_session_processing_time")
                            .record(start.elapsed());
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
        file_info_stream: FileInfoStream<ValidDataTransferSession>,
    ) -> anyhow::Result<()> {
        tracing::info!(
            file = file_info_stream.file_info.key,
            "handling valid data transfer file"
        );
        let mut transaction = self.pool.begin().await?;
        let file_ts = file_info_stream.file_info.timestamp;
        file_info_stream
            .into_stream(&mut transaction)
            .await?
            .map(anyhow::Ok)
            .try_fold(transaction, |mut transaction, report| async move {
                let data_session = HotspotDataSession::from_valid_data_session(report, file_ts);
                data_session.save(&mut transaction).await?;
                metrics::counter!("oracles_mobile_verifier_ingest_hotspot_data_session")
                    .increment(1);
                Ok(transaction)
            })
            .await?
            .commit()
            .await?;
        Ok(())
    }
}

impl ManagedTask for DataSessionIngestor {
    fn start_task(
        self: Box<Self>,
        shutdown: triggered::Listener,
    ) -> futures_util::future::LocalBoxFuture<'static, anyhow::Result<()>> {
        let handle = tokio::spawn(self.run(shutdown));
        Box::pin(
            handle
                .map_err(anyhow::Error::from)
                .and_then(|result| async move { result.map_err(anyhow::Error::from) }),
        )
    }
}

#[derive(sqlx::FromRow)]
pub struct HotspotDataSession {
    pub pub_key: PublicKeyBinary,
    pub payer: PublicKeyBinary,
    pub upload_bytes: i64,
    pub download_bytes: i64,
    pub num_dcs: i64,
    pub received_timestamp: DateTime<Utc>,
}

impl HotspotDataSession {
    pub async fn save(self, db: &mut Transaction<'_, Postgres>) -> Result<(), sqlx::Error> {
        sqlx::query(
            r#"
            INSERT INTO hotspot_data_transfer_sessions (pub_key, payer, upload_bytes, download_bytes, num_dcs, received_timestamp)
            VALUES ($1, $2, $3, $4, $5, $6)
            "#,
        )
        .bind(self.pub_key)
        .bind(self.payer)
        .bind(self.upload_bytes)
        .bind(self.download_bytes)
        .bind(self.num_dcs)
        .bind(self.received_timestamp)
        .execute(&mut *db)
        .await?;
        Ok(())
    }

    fn from_valid_data_session(
        v: ValidDataTransferSession,
        received_timestamp: DateTime<Utc>,
    ) -> HotspotDataSession {
        Self {
            pub_key: v.pub_key,
            payer: v.payer,
            upload_bytes: v.upload_bytes as i64,
            download_bytes: v.download_bytes as i64,
            num_dcs: v.num_dcs as i64,
            received_timestamp,
        }
    }
}

pub async fn aggregate_hotspot_data_sessions_to_dc<'a>(
    exec: impl sqlx::PgExecutor<'a> + Copy + 'a,
    epoch: &'a Range<DateTime<Utc>>,
) -> Result<HotspotMap, sqlx::Error> {
    let stream = sqlx::query_as::<_, HotspotDataSession>(
        r#"
        SELECT *
        FROM hotspot_data_transfer_sessions
        WHERE received_timestamp >= $1 and received_timestamp < $2
        "#,
    )
    .bind(epoch.start)
    .bind(epoch.end)
    .fetch(exec);
    data_sessions_to_dc(stream).await
}

pub async fn sum_data_sessions_to_dc_by_payer<'a>(
    exec: impl sqlx::PgExecutor<'a> + Copy + 'a,
    epoch: &'a Range<DateTime<Utc>>,
) -> Result<HashMap<String, u64>, sqlx::Error> {
    Ok(sqlx::query(
        r#"
        SELECT payer as sp, sum(num_dcs)::bigint as total_dcs
        FROM hotspot_data_transfer_sessions
        WHERE received_timestamp >= $1 and received_timestamp < $2
        GROUP BY payer
        "#,
    )
    .bind(epoch.start)
    .bind(epoch.end)
    .fetch_all(exec)
    .await?
    .iter()
    .map(|row| {
        let sp = row.get::<String, &str>("sp");
        let dcs: u64 = row.get::<i64, &str>("total_dcs") as u64;
        (sp, dcs)
    })
    .collect::<HashMap<String, u64>>())
}

pub async fn data_sessions_to_dc<'a>(
    stream: impl Stream<Item = Result<HotspotDataSession, sqlx::Error>>,
) -> Result<HotspotMap, sqlx::Error> {
    tokio::pin!(stream);
    let mut map = HotspotMap::new();
    while let Some(session) = stream.try_next().await? {
        let rewards = map.entry(session.pub_key).or_default();
        rewards.rewardable_dc += session.num_dcs as u64;
        rewards.rewardable_bytes += session.upload_bytes as u64 + session.download_bytes as u64;
    }
    Ok(map)
}

pub async fn clear_hotspot_data_sessions(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    timestamp: &DateTime<Utc>,
) -> Result<(), sqlx::Error> {
    sqlx::query("delete from hotspot_data_transfer_sessions where received_timestamp < $1")
        .bind(timestamp)
        .execute(&mut *tx)
        .await?;
    Ok(())
}
