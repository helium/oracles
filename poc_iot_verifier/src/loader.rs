use crate::{
    entropy::Entropy,
    meta::Meta,
    poc_report::{Report, ReportType},
    Result, Settings,
};
use chrono::{Duration, Utc};
use file_store::{
    lora_beacon_report::LoraBeaconIngestReport,
    lora_witness_report::LoraWitnessIngestReport,
    traits::{IngestId, TimestampDecode},
    FileStore, FileType,
};
use futures::{stream, StreamExt};
use helium_crypto::PublicKey;
use helium_proto::{
    services::poc_lora::{LoraBeaconIngestReportV1, LoraWitnessIngestReportV1},
    EntropyReportV1, Message,
};
use node_follower::{follower_service::FollowerService, gateway_resp::GatewayInfoResolver};
use sha2::{Digest, Sha256};
use sqlx::PgPool;
use tokio::time;

/// cadence for how often to look for new beacon and witness reports from s3 bucket
const REPORTS_POLL_TIME: time::Duration = time::Duration::from_secs(60);
/// cadence for how often to look for new entropy reports from s3 bucket
const ENTROPY_POLL_TIME: time::Duration = time::Duration::from_secs(90);
/// max age in hours of reports loaded from S3 which will be processed
/// any report older will be ignored
const MAX_REPORT_AGE: i64 = 2;

const LOADER_WORKERS: usize = 2;
const STORE_WORKERS: usize = 5;
// DB pool size if the store worker count multiplied by the number of file types
// since they're processed concurrently
const LOADER_DB_POOL_SIZE: usize = STORE_WORKERS * 2;

pub struct Loader {
    ingest_store: FileStore,
    entropy_store: FileStore,
    pool: PgPool,
    follower_service: FollowerService,
}

impl Loader {
    pub async fn from_settings(settings: &Settings) -> Result<Self> {
        tracing::info!("from_settings verifier loader");
        let pool = settings.database.connect(LOADER_DB_POOL_SIZE).await?;
        let ingest_store = FileStore::from_settings(&settings.ingest).await?;
        let entropy_store = FileStore::from_settings(&settings.entropy).await?;
        let follower_service = FollowerService::from_settings(&settings.follower)?;
        Ok(Self {
            pool,
            ingest_store,
            entropy_store,
            follower_service,
        })
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
                _ = report_timer.tick() => match self.handle_tick(&self.ingest_store, shutdown.clone()).await {
                    Ok(()) => (),
                    Err(err) => {
                        tracing::error!("fatal report loader error: {err:?}");
                        return Err(err)
                    }
                },
                _ = entropy_timer.tick() => match self.handle_tick(&self.entropy_store, shutdown.clone()).await {
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

    async fn handle_tick(&self, store: &FileStore, shutdown: triggered::Listener) -> Result {
        stream::iter(&[
            FileType::LoraBeaconIngestReport,
            FileType::LoraWitnessIngestReport,
            FileType::EntropyReport,
        ])
        .map(|file_type| (file_type, shutdown.clone()))
        .for_each_concurrent(2, |(file_type, shutdown)| async move {
            let _ = self.process_events(*file_type, store, shutdown).await;
        })
        .await;
        Ok(())
    }

    async fn process_events(
        &self,
        file_type: FileType,
        store: &FileStore,
        shutdown: triggered::Listener,
    ) -> Result {
        // TODO: determine a sane value for oldest_event_time
        // events older than this will not be processed
        let oldest_event_time = Utc::now() - Duration::hours(MAX_REPORT_AGE);
        let last_time = Meta::last_timestamp(&self.pool, file_type)
            .await?
            .unwrap_or(oldest_event_time)
            .max(oldest_event_time);

        let infos = store.list_all(file_type, last_time, None).await?;
        if infos.is_empty() {
            tracing::info!("no ingest {file_type} files to process from: {last_time}");
            return Ok(());
        }

        let last_timestamp = infos.last().map(|v| v.timestamp);
        let infos_len = infos.len();
        tracing::info!("processing {infos_len} {file_type} files");
        let handler = store
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
                let beacon: LoraBeaconIngestReport =
                    LoraBeaconIngestReportV1::decode(buf)?.try_into()?;
                tracing::debug!("beacon report from ingestor: {:?}", &beacon);
                let packet_data = beacon.report.data.clone();
                match self.check_valid_gateway(&beacon.report.pub_key).await {
                    true => {
                        Report::insert_into(
                            &self.pool,
                            beacon.ingest_id(),
                            beacon.report.remote_entropy,
                            packet_data,
                            buf.to_vec(),
                            &beacon.received_timestamp,
                            ReportType::Beacon,
                        )
                        .await
                    }
                    false => {
                        // if a gateway doesnt exist then drop the report
                        tracing::warn!(
                            "dropping beacon report as gateway not found: {:?}",
                            &beacon
                        );
                        Ok(())
                    }
                }
            }
            FileType::LoraWitnessIngestReport => {
                let witness: LoraWitnessIngestReport =
                    LoraWitnessIngestReportV1::decode(buf)?.try_into()?;
                tracing::debug!("witness report from ingestor: {:?}", &witness);
                let packet_data = witness.report.data.clone();
                match self.check_valid_gateway(&witness.report.pub_key).await {
                    true => {
                        Report::insert_into(
                            &self.pool,
                            witness.ingest_id(),
                            Vec::<u8>::with_capacity(0),
                            packet_data,
                            buf.to_vec(),
                            &witness.received_timestamp,
                            ReportType::Witness,
                        )
                        .await
                    }
                    false => {
                        // if a gateway doesnt exist then drop the report
                        // dont even insert into DB
                        tracing::warn!(
                            "dropping witness report as gateway not found: {:?}",
                            &witness
                        );
                        Ok(())
                    }
                }
            }
            FileType::EntropyReport => {
                let event = EntropyReportV1::decode(buf)?;
                tracing::debug!("entropy report: {:?}", event);
                let id = Sha256::digest(&event.data).to_vec();
                Entropy::insert_into(
                    &self.pool,
                    &id,
                    &event.data,
                    &event.timestamp.to_timestamp()?,
                    event.version as i32,
                )
                .await
            }
            _ => {
                tracing::warn!("ignoring unexpected filetype: {file_type:?}");
                Ok(())
            }
        }
    }

    // TODO: maybe store any found GW in the DB, save having to refetch later during validation ?
    async fn check_valid_gateway(&self, pub_key: &PublicKey) -> bool {
        self.follower_service
            .clone()
            .resolve_gateway_info(pub_key)
            .await
            .is_ok()
    }
}
