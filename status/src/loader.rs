use crate::{env_var, gateway::Gateway, meta::Meta, mk_db_pool, PublicKey, Result};
use chrono::{Duration, Utc};
use futures::StreamExt;
use helium_proto::{
    services::poc_mobile::{CellHeartbeatReqV1, SpeedtestReqV1},
    Message,
};
use poc_store::{datetime_from_epoch, file_source, FileStore, FileType};
use sqlx::PgPool;
use tokio::time;

const STORE_POLL_TIME: time::Duration = time::Duration::from_secs(10 * 60);
const LOADER_WORKERS: usize = 10;
const LOADER_DB_POOL_SIZE: usize = 2 * LOADER_WORKERS;

pub struct Loader {
    store: FileStore,
    bucket: String,
    pool: PgPool,
}

impl Loader {
    pub async fn from_env() -> Result<Self> {
        let pool = mk_db_pool(LOADER_DB_POOL_SIZE as u32).await?;
        let store = FileStore::from_env().await?;
        let bucket = env_var("INGEST_BUCKET")?.unwrap_or_else(|| "poc5g_ingest".to_string());
        Ok(Self {
            pool,
            store,
            bucket,
        })
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
        let file_type = FileType::CellHeartbeat;
        let last_time = Meta::last_timestamp(&self.pool, file_type)
            .await?
            .unwrap_or_else(|| Utc::now() - Duration::hours(4));
        let infos = self
            .store
            .list(&self.bucket, FileType::CellHeartbeat, last_time, None)
            .await?;
        if infos.is_empty() {
            tracing::info!("no ingest files to process from: {}", last_time);
            return Ok(());
        }

        let last_timestamp = infos.last().map(|v| v.timestamp);
        let infos_len = infos.len();
        tracing::info!("processing {infos_len} files");
        let stream: file_source::ByteStream =
            file_source::store_source(self.store.clone(), &self.bucket, infos);
        let handler = stream.for_each_concurrent(LOADER_WORKERS, |msg| async move {
            match msg {
                Err(err) => tracing::warn!("skipping entry in stream: {err:?}"),
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
                tracing::info!("completed processing {infos_len} files");
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
        }
    }
}
