use crate::{gateway::Gateway, meta::Meta, mk_db_pool, Result};
use chrono::{Duration, Utc};
use file_store::{datetime_from_epoch, FileStore, FileType};
use futures::{stream, StreamExt};
use helium_crypto::PublicKey;
use helium_proto::{
    services::poc_mobile::{CellHeartbeatReqV1, SpeedtestReqV1},
    Message,
};
use sqlx::PgPool;
use strum::EnumCount;
use tokio::time;

const STORE_POLL_TIME: time::Duration = time::Duration::from_secs(10 * 60);
const LOADER_WORKERS: usize = 2;
const STORE_WORKERS: usize = 5;
// DB pool size if the store worker count multiplied by the number of file types
// since they're processed concurrently
const LOADER_DB_POOL_SIZE: usize = STORE_WORKERS * 2;

pub struct Loader {
    store: FileStore,
    pool: PgPool,
}

impl Loader {
    pub async fn from_env() -> Result<Self> {
        let pool = mk_db_pool(LOADER_DB_POOL_SIZE as u32).await?;
        let store = FileStore::from_env().await?;
        Ok(Self { pool, store })
    }

    pub async fn run(&self, shutdown: &triggered::Listener) -> Result {
        tracing::info!("started status loader");
        let mut store_timer = time::interval(STORE_POLL_TIME);
        store_timer.set_missed_tick_behavior(time::MissedTickBehavior::Delay);

        loop {
            if shutdown.is_triggered() {
                break;
            }
            tokio::select! {
                _ = shutdown.clone() => break,
                _ = store_timer.tick() => match self.handle_store_tick(shutdown.clone()).await {
                    Ok(()) => (),
                    Err(err) => {
                        tracing::error!("fatal store loader error: {err:?}");
                        return Err(err)
                    }
                }
            }
        }
        tracing::info!("stopping status loader");
        Ok(())
    }

    async fn handle_store_tick(&self, shutdown: triggered::Listener) -> Result {
        stream::iter(&[FileType::CellHeartbeat, FileType::CellSpeedtest])
            .map(|file_type| (file_type, shutdown.clone()))
            .for_each_concurrent(FileType::COUNT, |(file_type, shutdown)| async move {
                let _ = self.process_events(*file_type, shutdown).await;
            })
            .await;
        Ok(())
    }

    async fn process_events(&self, file_type: FileType, shutdown: triggered::Listener) -> Result {
        let recent_time = Utc::now() - Duration::hours(2);
        let last_time = Meta::last_timestamp(&self.pool, file_type)
            .await?
            .unwrap_or(recent_time)
            .max(recent_time);

        tracing::info!("fetching {file_type} info");

        let infos = self.store.list_all(file_type, last_time, None).await?;
        if infos.is_empty() {
            tracing::info!("no ingest {file_type} files to process from: {last_time}");
            return Ok(());
        }

        let last_timestamp = infos.last().map(|v| v.timestamp);
        let infos_len = infos.len();
        tracing::info!("processing {infos_len} {file_type} files");
        let handler = self
            .store
            .source_unordered(LOADER_WORKERS, stream::iter(infos).map(Ok).boxed())
            .for_each_concurrent(STORE_WORKERS, |msg| async move {
                match msg {
                    Err(err) => tracing::warn!("skipping entry in {file_type} stream: {err:?}"),
                    Ok(buf) => match self.handle_store_update(file_type, &buf).await {
                        Ok(()) => (),
                        Err(err) => {
                            tracing::warn!("failed to update store: {err:?}")
                        }
                    },
                }
            });

        tokio::select! {
            _ = handler => {
                tracing::info!("completed processing {infos_len} {file_type} files");
                Meta::update_last_timestamp(&self.pool, file_type, last_timestamp).await?;
            },
            _ = shutdown.clone() => (),
        }

        Ok(())
    }

    async fn handle_store_update(&self, file_type: FileType, buf: &[u8]) -> Result {
        match file_type {
            FileType::CellHeartbeat => {
                let event = CellHeartbeatReqV1::decode(buf)?;
                let address = PublicKey::try_from(event.pub_key)?;
                Gateway::update_last_heartbeat(
                    &self.pool,
                    &address,
                    &datetime_from_epoch(event.timestamp),
                )
                .await
            }
            FileType::CellSpeedtest => {
                let event = SpeedtestReqV1::decode(buf)?;
                let address = PublicKey::try_from(event.pub_key)?;
                Gateway::update_last_speedtest(
                    &self.pool,
                    &address,
                    &datetime_from_epoch(event.timestamp),
                )
                .await
            }
            _ => Ok(()),
        }
    }
}
