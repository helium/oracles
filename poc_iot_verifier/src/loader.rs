use crate::{
    entropy::Entropy,
    gateway_cache::GatewayCache,
    meta::Meta,
    poc_report::{Report, ReportType},
    Result, Settings,
};
use blake3::hash;
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
use sqlx::PgPool;
use std::time::Duration;
use tokio::time::{self, MissedTickBehavior};

const REPORTS_META_NAME: &str = "report";
/// cadence for how often to look for  reports from s3 buckets
const REPORTS_POLL_TIME: u64 = 60 * 15;

const LOADER_WORKERS: usize = 25;
const STORE_WORKERS: usize = 60;
// DB pool size if the store worker count multiplied by the number of file types
// since they're processed concurrently
const LOADER_DB_POOL_SIZE: usize = STORE_WORKERS * 3;

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
        report_timer.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let mut denylist_timer = time::interval(self.deny_list_trigger_interval);
        denylist_timer.set_missed_tick_behavior(MissedTickBehavior::Skip);

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
                _ = report_timer.tick() => match self.handle_report_tick(gateway_cache, shutdown.clone()).await {
                    Ok(()) => (),
                    Err(err) => {
                        tracing::error!("fatal loader error, report_tick triggered: {err:?}");
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
        shutdown: triggered::Listener,
    ) -> Result {
        let oldest_event_time = Utc::now() - ChronoDuration::seconds(REPORTS_POLL_TIME as i64 * 2);
        let after = Meta::last_timestamp(&self.pool, REPORTS_META_NAME)
            .await?
            .unwrap_or(oldest_event_time)
            .max(oldest_event_time);
        let before = after + ChronoDuration::seconds(REPORTS_POLL_TIME as i64);

        // serially load each file type starting with entropy
        // beacons & witnesses dep on entropy
        // and ideally we wouldnt want to process a beacon
        // until witnesses are present in the db
        // otherwise we end up dropping those witnesses
        // serially loading each type ensures we have some order
        match self
            .process_events(
                FileType::EntropyReport,
                &self.entropy_store,
                gateway_cache,
                after,
                before,
                shutdown.clone(),
            )
            .await {
                Ok(()) => (),
                Err(err) =>
                    tracing::warn!("error whilst processing {:?} from s3, error: {err:?}", FileType::EntropyReport)
            }
        match self
            .process_events(
                FileType::LoraWitnessIngestReport,
                &self.ingest_store,
                gateway_cache,
                after,
                before,
                shutdown.clone(),
            )
            .await {
                Ok(()) => (),
                Err(err) =>
                    tracing::warn!("error whilst processing {:?} from s3, error: {err:?}", FileType::LoraWitnessIngestReport)
            }
        match self
            .process_events(
                FileType::LoraBeaconIngestReport,
                &self.ingest_store,
                gateway_cache,
                after,
                before,
                shutdown.clone(),
            )
            .await {
                Ok(()) => (),
                Err(err) =>
                    tracing::warn!("error whilst processing {:?} from s3, error: {err:?}", FileType::LoraBeaconIngestReport)
            }
        let _ = Meta::update_last_timestamp(&self.pool, REPORTS_META_NAME, Some(before)).await;
        Ok(())
    }

    async fn process_events(
        &self,
        file_type: FileType,
        store: &FileStore,
        gateway_cache: &GatewayCache,
        after: chrono::DateTime<Utc>,
        before: chrono::DateTime<Utc>,
        _shutdown: triggered::Listener,
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
            .source_unordered(LOADER_WORKERS, stream::iter(infos).map(Ok).boxed())
            .for_each_concurrent(STORE_WORKERS, |msg| async move {
                match msg {
                    Err(err) => tracing::warn!("skipping report of type {file_type} due to error {err:?}"),
                    Ok(buf) => match self
                        .handle_report(file_type, &buf, gateway_cache)
                        .await
                    {
                        Ok(()) => (),
                        Err(err) => {
                            tracing::warn!("error whilst handling incoming report of type: {file_type}, error: {err:?}")
                        }
                    },
                }
            }).await;

        // tokio::select! {
        //     _ = handler => {
        //         tracing::info!("completed processing {infos_len} files of type {file_type}");
        //     },
        //     _ = shutdown.clone() => (),
        // }

        tracing::info!("completed processing {infos_len} files of type {file_type}");
        Ok(())
    }

    async fn handle_report(
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
                        .await?;
                        metrics::increment_counter!("oracles_poc_iot_verifier_loader_beacon");
                        Ok(())
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
                        .await?;
                        metrics::increment_counter!("oracles_poc_iot_verifier_loader_witness");
                        Ok(())
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

    async fn check_valid_gateway(&self, pub_key: &PublicKey, gateway_cache: &GatewayCache) -> bool {
        if self.check_gw_denied(pub_key).await {
            tracing::debug!("dropping denied gateway : {:?}", &pub_key);
            return false;
        }
        if self.check_unknown_gw(pub_key, gateway_cache).await {
            tracing::debug!("dropping unknown gateway: {:?}", &pub_key);
            return false;
        }
        true
    }

    async fn check_unknown_gw(&self, pub_key: &PublicKey, gateway_cache: &GatewayCache) -> bool {
        gateway_cache.resolve_gateway_info(pub_key).await.is_err()
    }

    async fn check_gw_denied(&self, pub_key: &PublicKey) -> bool {
        self.deny_list.check_key(pub_key).await
    }
}
