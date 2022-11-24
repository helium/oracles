use crate::{
    entropy::Entropy,
    gateway_cache::GatewayCache,
    meta::Meta,
    poc_report::{Report, ReportType},
    Result, Settings,
};
use chrono::{Duration as ChronoDuration, Utc};
use denylist::DenyList;
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
use blake3::hash;
use sqlx::PgPool;
use std::time::Duration;
use tokio::time;

const REPORTS_POLL_TIME: u64 = 60 * 15;
const INITIAL_WAIT: u64 = REPORTS_POLL_TIME * 2;

/// cadence for how often to look for  reports from s3 buckets
// const REPORTS_POLL_TIME: time::Duration = time::Duration::from_secs(60 * 15);
/// max age in seconds of beacon & witness reports loaded from S3 which will be processed
/// any report older will be ignored
const REPORT_MAX_REPORT_AGE: i64 = 60 * 60; // 15 mins
/// max age in seconds of entropy reports loaded from S3 which will be processed
/// any report older will be ignored
const ENTROPY_MAX_REPORT_AGE: i64 = 60 * 30; // 30 mins

const LOADER_WORKERS: usize = 25;
const STORE_WORKERS: usize = 100;
// DB pool size if the store worker count multiplied by the number of file types
// since they're processed concurrently
const LOADER_DB_POOL_SIZE: usize = STORE_WORKERS * 2;

pub struct Loader {
    ingest_store: FileStore,
    entropy_store: FileStore,
    pool: PgPool,
    deny_list_latest_url: String,
    deny_list_trigger_interval: Duration,
    deny_list: DenyList,
}

impl Loader {
    pub async fn from_settings(settings: &Settings) -> Result<Self> {
        tracing::info!("from_settings verifier loader");
        let pool = settings.database.connect(LOADER_DB_POOL_SIZE).await?;
        let ingest_store = FileStore::from_settings(&settings.ingest).await?;
        let entropy_store = FileStore::from_settings(&settings.entropy).await?;
        let deny_list = DenyList::new()?;
        Ok(Self {
            pool,
            ingest_store,
            entropy_store,
            deny_list_latest_url: settings.denylist.denylist_url.clone(),
            deny_list_trigger_interval: settings.denylist.trigger_interval(),
            deny_list,
        })
    }

    pub async fn run(
        &mut self,
        shutdown: &triggered::Listener,
        gateway_cache: &GatewayCache,
    ) -> Result {
        tracing::info!("started verifier loader");

        let mut report_timer = time::interval(time::Duration::from_secs(REPORTS_POLL_TIME));
        let mut denylist_timer = time::interval(self.deny_list_trigger_interval);
        loop {
            if shutdown.is_triggered() {
                break;
            }
            tokio::select! {
                _ = shutdown.clone() => break,
                _ = denylist_timer.tick() =>
                    match self.handle_denylist_tick().await {
                    Ok(()) => (),
                    Err(err) => {
                        tracing::error!("fatal loader error, denylist_tick triggered: {err:?}");
                    }
                },
                _ = report_timer.tick() => match self.handle_report_tick(gateway_cache).await {
                    Ok(()) => (),
                    Err(err) => {
                        tracing::error!("fatal loader error, repor_tick triggered: {err:?}");
                        return Err(err)
                    }
                }
            }
        }
        tracing::info!("stopping verifier loader");
        Ok(())
    }

    async fn handle_denylist_tick(&mut self) -> Result {
        // sink any errors whilst updating the denylist
        // the verifier should not stop just because github
        // could not be reached for example
        match self
            .deny_list
            .update_to_latest(&self.deny_list_latest_url)
            .await
        {
            Ok(()) => (),
            Err(e) => tracing::warn!("failed to update denylist: {e}"),
        }
        Ok(())
    }

    async fn handle_report_tick(
        &self,
        gateway_cache: &GatewayCache,
    ) -> Result {

        // TODO: determine a sane value for oldest_event_time
        // events older than this will not be processed
        let oldest_event_time = Utc::now() - ChronoDuration::seconds(INITIAL_WAIT as i64 * 2);
        let entropy_after = Meta::last_timestamp(&self.pool, FileType::EntropyReport)
            .await?
            .unwrap_or(oldest_event_time)
            .max(oldest_event_time);
        let entropy_before = entropy_after + ChronoDuration::seconds(REPORTS_POLL_TIME as i64);
        stream::iter(&[FileType::EntropyReport])
            .map(|file_type| (file_type))
            .for_each_concurrent(5, | file_type | async move {
                let _ = self
                    .process_events(
                        *file_type,
                        &self.entropy_store,
                        gateway_cache,
                        entropy_after,
                        entropy_before,
                    )
                    .await;
            })
            .await;

        // process witness report before beacons
        // this gives a better opportunity for all relevant witness
        // reports to be in the DB prior to the associated beacon
        // report being selected for verification
        // if a beacon is verified before the witness reports make it
        // to the db then we end up dropping those witness reports
        let witness_after = Meta::last_timestamp(&self.pool, FileType::EntropyReport)
            .await?
            .unwrap_or(oldest_event_time)
            .max(oldest_event_time);
        let witness_before = witness_after + ChronoDuration::seconds(REPORTS_POLL_TIME as i64);
        stream::iter(&[FileType::LoraWitnessIngestReport])
            .map(|file_type| (file_type))
            .for_each_concurrent(5, | file_type | async move {
                let _ = self
                    .process_events(
                        *file_type,
                        &self.ingest_store,
                        gateway_cache,
                        witness_after,
                        witness_before,
                    )
                    .await;
            })
            .await;

        let beacon_after = Meta::last_timestamp(&self.pool, FileType::EntropyReport)
            .await?
            .unwrap_or(oldest_event_time)
            .max(oldest_event_time);
        let beacon_before = beacon_after + ChronoDuration::seconds(REPORTS_POLL_TIME as i64);
        stream::iter(&[FileType::LoraBeaconIngestReport])
            .map(|file_type| (file_type))
            .for_each_concurrent(5, | file_type | async move {
                let _ = self
                    .process_events(
                        *file_type,
                        &self.ingest_store,
                        gateway_cache,
                        beacon_after,
                        beacon_before,
                    )
                    .await;
            })
            .await;

        Ok(())
    }

    async fn process_events(
        &self,
        file_type: FileType,
        store: &FileStore,
        gateway_cache: &GatewayCache,
        after: chrono::DateTime<Utc>,
        before: chrono::DateTime<Utc>,
    ) -> Result {
        tracing::info!("checking for new ingest files of type {file_type} after {after} and before {before}");
        let infos = store.list_all(file_type, after, before).await?;
        if infos.is_empty() {
            tracing::info!("no available ingest files of type {file_type}");
            return Ok(());
        }

        let last_timestamp = infos.last().map(|v| v.timestamp);
        let infos_len = infos.len();
        tracing::info!("processing {infos_len} ingest files of type {file_type}");
        store
        .source_unordered(LOADER_WORKERS, stream::iter(infos).map(Ok).boxed())
        .for_each_concurrent(STORE_WORKERS, |msg| async move {
            match msg {
                Err(err) => tracing::warn!("skipping entry in {file_type} stream: {err:?}"),
                Ok(buf) => match self
                    .handle_store_update(file_type, &buf, gateway_cache)
                    .await
                {
                    Ok(()) => (),
                    Err(err) => {
                        tracing::warn!("failed to update store: {err:?}")
                    }
                },
            }
        }).await;
        tracing::info!("completed processing {infos_len} files of type {file_type}");
        Meta::update_last_timestamp(&self.pool, file_type, last_timestamp).await?;
        Ok(())
    }

    async fn handle_store_update(
        &self,
        file_type: FileType,
        buf: &[u8],
        gateway_cache: &GatewayCache,
    ) -> Result {
        match file_type {
            FileType::LoraBeaconIngestReport => {
                let beacon: LoraBeaconIngestReport =
                    LoraBeaconIngestReportV1::decode(buf)?.try_into()?;
                tracing::debug!("beacon report from ingestor: {:?}", &beacon);
                let packet_data = beacon.report.data.clone();
                match self
                    .check_valid_gateway(&beacon.report.pub_key, gateway_cache)
                    .await
                {
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
                    false => Ok(()),
                }
            }
            FileType::LoraWitnessIngestReport => {
                let witness: LoraWitnessIngestReport =
                    LoraWitnessIngestReportV1::decode(buf)?.try_into()?;
                tracing::debug!("witness report from ingestor: {:?}", &witness);
                let packet_data = witness.report.data.clone();
                match self
                    .check_valid_gateway(&witness.report.pub_key, gateway_cache)
                    .await
                {
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
                    false => Ok(()),
                }
            }
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
                .await
            }
            _ => {
                tracing::warn!("ignoring unexpected filetype: {file_type:?}");
                Ok(())
            }
        }
    }

    async fn check_valid_gateway(&self, pub_key: &PublicKey, gateway_cache: &GatewayCache) -> bool {
        // if self.check_gw_denied(pub_key).await {
        //     tracing::debug!("dropping denied gateway : {:?}", &pub_key);
        //     return false;
        // }
        // if self.check_unknown_gw(pub_key, gateway_cache).await {
        //     tracing::debug!("dropping unknown gateway: {:?}", &pub_key);
        //     return false;
        // }
        true
    }

    async fn check_unknown_gw(&self, pub_key: &PublicKey, gateway_cache: &GatewayCache) -> bool {
        gateway_cache.resolve_gateway_info(pub_key).await.is_err()
    }

    async fn check_gw_denied(&self, pub_key: &PublicKey) -> bool {
        self.deny_list.check_key(pub_key).await
    }
}
