use crate::{entropy::Entropy, meta::Meta, Result, Settings};
use blake3::hash;
use chrono::{Duration as ChronoDuration, Utc};
use file_store::{traits::TimestampDecode, FileStore, FileType};
use futures::{stream, StreamExt};
use helium_proto::{EntropyReportV1, Message};
use sqlx::PgPool;
use tokio::time::{self, MissedTickBehavior};

const ENTROPY_META_NAME: &str = "entropy_report";
/// cadence for how often to look for entropy from s3 buckets
const ENTROPY_POLL_TIME: u64 = 60 * 10;

const STORE_WORKERS: usize = 10;
const LOADER_DB_POOL_SIZE: usize = STORE_WORKERS * 4;

pub struct EntropyLoader {
    entropy_store: FileStore,
    pool: PgPool,
}

impl EntropyLoader {
    pub async fn from_settings(settings: &Settings) -> Result<Self> {
        tracing::info!("from_settings verifier entropy loader");
        let pool = settings.database.connect(LOADER_DB_POOL_SIZE).await?;
        let entropy_store = FileStore::from_settings(&settings.entropy).await?;
        Ok(Self {
            pool,
            entropy_store,
        })
    }

    pub async fn run(&mut self, shutdown: &triggered::Listener) -> Result {
        tracing::info!("started verifier entropy loader");
        let mut report_timer = time::interval(time::Duration::from_secs(ENTROPY_POLL_TIME));
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
                        tracing::error!("fatal entropy loader error, entropy_tick triggered: {err:?}");
                    }
                }
            }
        }
        tracing::info!("stopping verifier entropy_loader");
        Ok(())
    }

    async fn handle_entropy_tick(&self) -> Result {
        tracing::info!("handling entropy tick");
        let now = Utc::now();
        // the loader loads files from s3 via a sliding window
        // its start point is Now() - (ENTROPY_POLL_TIME * 3)
        // as such data being loaded is always stale by a time equal to ENTROPY_POLL_TIME

        // if there is NO last timestamp in the DB, we will start our sliding window from this point
        let window_default_lookback = now - ChronoDuration::seconds(60 * 60);
        // if there IS a last timestamp in the DB, we will use it as the starting point for our sliding window
        // but cap it at the max below.
        let window_max_lookback = now - ChronoDuration::seconds(60 * 60 * 2);
        let after = Meta::last_timestamp(&self.pool, ENTROPY_META_NAME)
            .await?
            .unwrap_or(window_default_lookback)
            .max(window_max_lookback);

        let before = now;
        let window_width = (before - after).num_minutes() as u64;
        tracing::info!(
            "entropy sliding window, after: {after}, before: {before}, width: {window_width}"
        );
        match self
            .process_events(FileType::EntropyReport, &self.entropy_store, after, before)
            .await
        {
            Ok(()) => (),
            Err(err) => tracing::warn!(
                "error whilst processing {:?} from s3, error: {err:?}",
                FileType::EntropyReport
            ),
        }
        Meta::update_last_timestamp(&self.pool, ENTROPY_META_NAME, Some(before)).await?;
        tracing::info!("completed handling entropy tick");
        Ok(())
    }

    async fn process_events(
        &self,
        file_type: FileType,
        store: &FileStore,
        after: chrono::DateTime<Utc>,
        before: chrono::DateTime<Utc>,
    ) -> Result {
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

    async fn handle_report(&self, file_type: FileType, buf: &[u8]) -> Result {
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
                metrics::increment_counter!("oracles_poc_iot_verifier_loader_entropy");
                Ok(())
            }
            _ => {
                tracing::warn!("ignoring unexpected filetype: {file_type:?}");
                Ok(())
            }
        }
    }
}
