use crate::{
    entropy::Entropy,
    meta::Meta,
    mk_db_pool,
    poc_report::{Report, ReportType},
    Result,
};
use chrono::{Duration, Utc};
use futures::{stream, StreamExt};
use helium_proto::Entropy as EntropyPB;
use helium_proto::{
    services::poc_lora::{LoraBeaconIngestReportV1, LoraWitnessIngestReportV1},
    Message,
};

use poc_store::{datetime_from_epoch, FileStore, FileType};
use sha2::{Digest, Sha256};
use sqlx::PgPool;
use tokio::time;

const REPORTS_POLL_TIME: time::Duration = time::Duration::from_secs(60);
const ENTROPY_POLL_TIME: time::Duration = time::Duration::from_secs(20);
const LOADER_WORKERS: usize = 2;
const STORE_WORKERS: usize = 5;
// DB pool size if the store worker count multiplied by the number of file types
// since they're processed concurrently
const LOADER_DB_POOL_SIZE: usize = STORE_WORKERS * 2;

pub struct Loader {
    store: FileStore,
    // bucket: String,
    pool: PgPool,
}

impl Loader {
    pub async fn from_env() -> Result<Self> {
        tracing::info!("from_env verifier loader");
        let pool = mk_db_pool(LOADER_DB_POOL_SIZE as u32).await?;
        let store = FileStore::from_env().await?;
        // let bucket = env_var("INGEST_BUCKET")?.unwrap_or_else(|| "poclora_ingest".to_string());
        Ok(Self { pool, store })
    }

    pub async fn run(&self, shutdown: &triggered::Listener) -> Result {
        tracing::info!("started verifier loader");

        let mut report_timer = time::interval(REPORTS_POLL_TIME);
        report_timer.set_missed_tick_behavior(time::MissedTickBehavior::Delay);

        let mut entropy_timer = time::interval(ENTROPY_POLL_TIME);
        entropy_timer.set_missed_tick_behavior(time::MissedTickBehavior::Delay);

        loop {
            if shutdown.is_triggered() {
                break;
            }
            tokio::select! {
                _ = shutdown.clone() => break,
                _ = report_timer.tick() => match self.handle_store_tick(shutdown.clone()).await {
                    Ok(()) => (),
                    Err(err) => {
                        tracing::error!("fatal report loader error: {err:?}");
                        return Err(err)
                    }
                },
                _ = entropy_timer.tick() => match self.handle_store_tick(shutdown.clone()).await {
                    Ok(()) => (),
                    Err(err) => {
                        tracing::error!("fatal entropy loader error: {err:?}");
                        return Err(err)
                    }
                }
            }
        }
        tracing::info!("stopping verifier loader");
        Ok(())
    }

    async fn handle_store_tick(&self, shutdown: triggered::Listener) -> Result {
        stream::iter(&[
            FileType::LoraBeaconIngestReport,
            FileType::LoraWitnessIngestReport,
            FileType::Entropy,
        ])
        .map(|file_type| (file_type, shutdown.clone()))
        .for_each_concurrent(2, |(file_type, shutdown)| async move {
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
        let test = match last_timestamp {
            Some(x) => x,
            None => recent_time,
        };
        tracing::info!("meta {last_time}, files last timestamp {test}");

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
            FileType::LoraBeaconIngestReport => {
                let beacon = LoraBeaconIngestReportV1::decode(buf)?;
                let event = beacon.report.unwrap();
                let packet_data = event.data.clone();
                let buf_as_vec = buf.to_vec();
                let mut public_key = event.pub_key.clone();
                // TODO: maybe this ID construction can be pushed out to a trait or part of the report struct ?
                let mut id: Vec<u8> = packet_data.clone();
                id.append(&mut public_key);
                let id_hash = Sha256::digest(&id).to_vec();
                Report::insert_into(
                    &self.pool,
                    id_hash,
                    packet_data,
                    buf_as_vec,
                    &datetime_from_epoch(event.timestamp),
                    ReportType::Beacon,
                )
                .await
            }
            FileType::LoraWitnessIngestReport => {
                let witness = LoraWitnessIngestReportV1::decode(buf)?;
                let event = witness.report.unwrap();
                let packet_data = event.data.clone();
                let buf_as_vec = buf.to_vec();
                // TODO: maybe this ID construction can be pushed out to a trait or part of the report struct ?
                let mut public_key = event.pub_key.clone();
                let mut id: Vec<u8> = packet_data.clone();
                id.append(&mut public_key);
                let id_hash = Sha256::digest(&id).to_vec();
                Report::insert_into(
                    &self.pool,
                    id_hash,
                    packet_data,
                    buf_as_vec,
                    &datetime_from_epoch(event.timestamp),
                    ReportType::Witness,
                )
                .await
            }
            FileType::Entropy => {
                let event = EntropyPB::decode(buf)?;
                let id = Sha256::digest(&event.data).to_vec();
                Entropy::insert_into(
                    &self.pool,
                    &id,
                    &event.data,
                    &datetime_from_epoch(event.timestamp),
                )
                .await
            }
            _ => {
                tracing::warn!("ignoring unexpected filetype: {file_type:?}");
                Ok(())
            }
        }
    }
}
