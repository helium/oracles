use crate::{entropy::Entropy, meta::Meta, Settings};
use blake3::hash;
use chrono::{Duration as ChronoDuration, Utc};
use file_store::{traits::TimestampDecode, FileStore, FileType};
use futures::{stream, StreamExt};
use helium_proto::{EntropyReportV1, Message};
use sqlx::PgPool;
use tokio::time::{self, MissedTickBehavior};

const ENTROPY_META_NAME: &str = "entropy_report";
/// cadence for how often to look for entropy from s3 buckets
const ENTROPY_POLL_TIME: i64 = 60 * 5;

const STORE_WORKERS: usize = 10;
const LOADER_DB_POOL_SIZE: usize = STORE_WORKERS * 4;

pub struct EntropyLoader {
    entropy_store: FileStore,
    pool: PgPool,
}

#[derive(thiserror::Error, Debug)]
pub enum NewLoaderError {
    #[error("file store error: {0}")]
    FileStoreError(#[from] file_store::Error),
    #[error("db_store error: {0}")]
    DbStoreError(#[from] db_store::Error),
}

impl EntropyLoader {
    pub async fn from_settings(settings: &Settings) -> Result<Self, NewLoaderError> {
        tracing::info!("from_settings verifier entropy loader");
        let pool = settings.database.connect(LOADER_DB_POOL_SIZE).await?;
        let entropy_store = FileStore::from_settings(&settings.entropy).await?;
        Ok(Self {
            pool,
            entropy_store,
        })
    }

    pub async fn run(&mut self, shutdown: &triggered::Listener) -> anyhow::Result<()> {
        tracing::info!("started verifier entropy loader");
        let mut report_timer = time::interval(time::Duration::from_secs(ENTROPY_POLL_TIME as u64));
        report_timer.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            if shutdown.is_triggered() {
                break;
            }
            tokio::select! {
                _ = shutdown.clone() => break,
                _ = report_timer.tick() => match self.handle_entropy_tick().await {
                    Ok(()) => (),
                    Err(err) => {
                        tracing::error!("entropy loader error: {err:?}");
                    }
                }
            }
        }
        tracing::info!("stopping verifier entropy_loader");
        Ok(())
    }

    async fn handle_entropy_tick(&self) -> anyhow::Result<()> {
        tracing::info!("handling entropy tick");
        let now = Utc::now();
        // the loader loads files from s3 via a sliding window
        // its start point is Now() - (ENTROPY_POLL_TIME * 3)
        // as such data being loaded is always stale by a time equal to ENTROPY_POLL_TIME

        // if there is NO last timestamp in the DB, we will start our sliding window from this point
        let window_default_lookback = now - ChronoDuration::seconds(ENTROPY_POLL_TIME * 6);
        // if there IS a last timestamp in the DB, we will use it as the starting point for our sliding window
        // but cap it at the max below.
        let window_max_lookback = now - ChronoDuration::seconds(ENTROPY_POLL_TIME * 12);
        let after = Meta::last_timestamp(&self.pool, ENTROPY_META_NAME)
            .await?
            .unwrap_or(window_default_lookback)
            .max(window_max_lookback);
        let before = now - ChronoDuration::seconds(ENTROPY_POLL_TIME);
        let window_width = (before - after).num_minutes() as u64;
        tracing::info!(
            "entropy sliding window, after: {after}, before: {before}, width: {window_width}"
        );
        // contain any errors whilst processing the window
        // any required recovery should happen within process_window
        // and after processing the window we should always updated last timestamp
        // in order to advance our sliding window
        match self
            .process_window(FileType::EntropyReport, &self.entropy_store, after, before)
            .await
        {
            Ok(()) => (),
            Err(err) => tracing::warn!(
                "error whilst processing window for {:?}, error: {err:?}",
                FileType::EntropyReport
            ),
        }
        Meta::update_last_timestamp(&self.pool, ENTROPY_META_NAME, Some(before)).await?;
        tracing::info!("completed handling entropy tick");
        Ok(())
    }

    async fn process_window(
        &self,
        file_type: FileType,
        store: &FileStore,
        after: chrono::DateTime<Utc>,
        before: chrono::DateTime<Utc>,
    ) -> anyhow::Result<()> {
        tracing::info!(
            "checking for new ingest files of type {file_type} after {after} and before {before}"
        );
        let infos = store.list_all(file_type, after, before).await?;
        if infos.is_empty() {
            tracing::info!("no available ingest files of type {file_type}");
            return Ok(());
        }

        let infos_len = infos.len();
        tracing::info!("processing {infos_len} ingest files of type {file_type}");
        store
            .source(stream::iter(infos).map(Ok).boxed())
            .for_each_concurrent(STORE_WORKERS, |msg| async move {
                match msg {
                    Err(err) => tracing::warn!("skipping report of type {file_type} due to error {err:?}"),
                    Ok(buf) => match self
                        .handle_report(file_type, &buf)
                        .await
                    {
                        Ok(()) => (),
                        Err(err) => {
                            tracing::warn!("error whilst handling incoming report of type: {file_type}, error: {err:?}")
                        }
                    },
                }
            })
            .await;
        tracing::info!("completed processing {infos_len} files of type {file_type}");
        Ok(())
    }

    async fn handle_report(&self, file_type: FileType, buf: &[u8]) -> anyhow::Result<()> {
        match file_type {
            FileType::EntropyReport => {
                let event = EntropyReportV1::decode(buf)?;
                tracing::debug!("entropy report: {:?}", event);
                let id = hash(&event.data).as_bytes().to_vec();
                Entropy::insert_into(
                    &self.pool,
                    &id,
                    &event.data,
                    &event.timestamp.to_timestamp()?,
                    event.version as i32,
                )
                .await?;
                metrics::increment_counter!("oracles_iot_verifier_loader_entropy");
                Ok(())
            }
            _ => {
                tracing::warn!("ignoring unexpected filetype: {file_type:?}");
                Ok(())
            }
        }
    }
}
