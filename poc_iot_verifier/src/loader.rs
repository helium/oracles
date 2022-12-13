use crate::{
    gateway_cache::GatewayCache,
    meta::Meta,
    poc_report::{Report, ReportType},
    Settings,
};
use chrono::DateTime;
use chrono::{Duration as ChronoDuration, Utc};
use denylist::DenyList;
use file_store::{
    lora_beacon_report::LoraBeaconIngestReport, lora_witness_report::LoraWitnessIngestReport,
    traits::IngestId, FileStore, FileType,
};
use futures::{stream, StreamExt};
use helium_crypto::PublicKeyBinary;
use helium_proto::{
    services::poc_lora::{LoraBeaconIngestReportV1, LoraWitnessIngestReportV1},
    Message,
};
use sqlx::{PgPool, Postgres, QueryBuilder};
use std::time::Duration;
use tokio::time::{self, MissedTickBehavior};

const REPORTS_META_NAME: &str = "report";
/// cadence for how often to look for  reports from s3 buckets
const REPORTS_POLL_TIME: u64 = 60 * 30;

const STORE_WORKERS: usize = 100;
// DB pool size if the store worker count multiplied by the number of file types
// since they're processed concurrently
const LOADER_DB_POOL_SIZE: usize = STORE_WORKERS * 4;

pub struct Loader {
    ingest_store: FileStore,
    pool: PgPool,
    deny_list_latest_url: String,
    deny_list_trigger_interval: Duration,
    deny_list: DenyList,
}

#[derive(thiserror::Error, Debug)]
pub enum NewLoaderError {
    #[error("file store error: {0}")]
    FileStoreError(#[from] file_store::Error),
    #[error("db_store error: {0}")]
    DbStoreError(#[from] db_store::Error),
    #[error("denylist error: {0}")]
    DenyListError(#[from] denylist::Error),

pub struct InsertBindings {
    id: Vec<u8>,
    remote_entropy: Vec<u8>,
    packet_data: Vec<u8>,
    buf: Vec<u8>,
    received_ts: DateTime<Utc>,
    report_type: ReportType,
}

impl Loader {
    pub async fn from_settings(settings: &Settings) -> Result<Self, NewLoaderError> {
        tracing::info!("from_settings verifier loader");
        let pool = settings.database.connect(LOADER_DB_POOL_SIZE).await?;
        let ingest_store = FileStore::from_settings(&settings.ingest).await?;
        let deny_list = DenyList::new()?;
        Ok(Self {
            pool,
            ingest_store,
            deny_list_latest_url: settings.denylist.denylist_url.clone(),
            deny_list_trigger_interval: settings.denylist.trigger_interval(),
            deny_list,
        })
    }

    pub async fn run(
        &mut self,
        shutdown: &triggered::Listener,
        gateway_cache: &GatewayCache,
    ) -> anyhow::Result<()> {
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
                _ = report_timer.tick() => match self.handle_report_tick(gateway_cache).await {
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

    async fn handle_denylist_tick(&mut self) -> anyhow::Result<()> {
        tracing::info!("handling denylist tick");
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
        tracing::info!("completed handling denylist tick");
        Ok(())
    }

    async fn handle_report_tick(&self, gateway_cache: &GatewayCache) -> anyhow::Result<()> {
        tracing::info!("handling report tick");
        let now = Utc::now();
        // the loader loads files from s3 via a sliding window
        // the window defaults to a width = REPORTS_POLL_TIME
        // its start point is Now() - (REPORTS_POLL_TIME * 2)
        // as such data being loaded is always stale by a time equal to REPORTS_POLL_TIME

        // if there is NO last timestamp in the DB, we will start our sliding window from this point
        let window_default_lookback = now - ChronoDuration::seconds(REPORTS_POLL_TIME as i64 * 2);
        // NOTE: Atm we never look back more than window_default_lookback
        // The experience has been that once we start processing a window longer than default
        // we never recover the time and end up stuck on a window of the extended size
        // The option is here however to extend the window size should it be needed
        let window_max_lookback = now - ChronoDuration::seconds(REPORTS_POLL_TIME as i64 * 2);
        let after = Meta::last_timestamp(&self.pool, REPORTS_META_NAME)
            .await?
            .unwrap_or(window_default_lookback)
            .max(window_max_lookback);

        let before = now - ChronoDuration::seconds(REPORTS_POLL_TIME as i64);
        let window_width = (before - after).num_minutes() as u64;
        tracing::info!("sliding window, after: {after}, before: {before}, width: {window_width}");
        _ = self.process_window(gateway_cache, after, before).await;
        _ = Meta::update_last_timestamp(&self.pool, REPORTS_META_NAME, Some(before)).await;
        tracing::info!("completed handling poc_report tick");
        Ok(())
    }

    async fn process_window(
        &self,
        gateway_cache: &GatewayCache,
        after: DateTime<Utc>,
        before: DateTime<Utc>,
    ) -> Result {
        // serially load witnesses and beacons for this window
        // ideally we dont want to process a beacon
        // until any associated witnesses are present in the db
        match self
            .process_events(
                FileType::LoraWitnessIngestReport,
                &self.ingest_store,
                gateway_cache,
                after,
                before,
            )
            .await
        {
            Ok(()) => (),
            Err(err) => tracing::warn!(
                "error whilst processing {:?} from s3, error: {err:?}",
                FileType::LoraWitnessIngestReport
            ),
        }

        match self
            .process_events(
                FileType::LoraBeaconIngestReport,
                &self.ingest_store,
                gateway_cache,
                after,
                before,
            )
            .await
        {
            Ok(()) => (),
            Err(err) => tracing::warn!(
                "error whilst processing {:?} from s3, error: {err:?}",
                FileType::LoraBeaconIngestReport
            ),
        }
        Ok(())
    }

    async fn process_events(
        &self,
        file_type: FileType,
        store: &FileStore,
        gateway_cache: &GatewayCache,
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
            .chunks(50)
            .for_each_concurrent(STORE_WORKERS, |msgs| async move {
                let mut query_builder: QueryBuilder<Postgres> = QueryBuilder::new(
                    "insert into poc_report (
                        id,
                        remote_entropy,
                        packet_data,
                        report_data,
                        report_timestamp,
                        report_type
                    ) "
                );
                let mut inserts = Vec::new();
                for msg in msgs {
                    match msg {
                        Err(err) => tracing::warn!("skipping report of type {file_type} due to error {err:?}"),
                        Ok(buf) => {
                            match self.handle_report(file_type, &buf, gateway_cache).await
                            {
                                Ok(Some(bindings)) => {
                                    inserts.push(bindings);

                                },
                                Ok(None) => (),
                                Err(err) => {
                                    tracing::warn!("error whilst handling incoming report of type: {file_type}, error: {err:?}")
                                }
                            }
                        }
                    }
                }

                query_builder.push_values(inserts, |mut b, insert| {
                        b.push_bind(insert.id)
                        .push_bind(insert.remote_entropy)
                        .push_bind(insert.packet_data)
                        .push_bind(insert.buf)
                        .push_bind(insert.received_ts)
                        .push_bind(insert.report_type);
                });

                let sql = query_builder.sql();
                match Report::insert_bulk(&self.pool, sql).await
                {
                    Ok(()) => (),
                    Err(err) => tracing::warn!(
                        "error whilst inserting report to db,  error: {err:?}"),
                }
            })
            .await;
        tracing::info!("completed processing {infos_len} files of type {file_type}");
        Ok(())
    }

    async fn handle_report(
        &self,
        file_type: FileType,
        buf: &[u8],
        gateway_cache: &GatewayCache,
    ) -> anyhow::Result<Option<InsertBindings>> {
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
                        let res = InsertBindings {
                            id: beacon.ingest_id(),
                            remote_entropy: beacon.report.remote_entropy,
                            packet_data,
                            buf: buf.to_vec(),
                            received_ts: beacon.received_timestamp,
                            report_type: ReportType::Beacon,
                        };
                        metrics::increment_counter!("oracles_poc_iot_verifier_loader_beacon");
                        Ok(Some(res))
                    }
                    false => Ok(None),
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
                        let res = InsertBindings {
                            id: witness.ingest_id(),
                            remote_entropy: Vec::<u8>::with_capacity(0),
                            packet_data,
                            buf: buf.to_vec(),
                            received_ts: witness.received_timestamp,
                            report_type: ReportType::Witness,
                        };
                        metrics::increment_counter!("oracles_poc_iot_verifier_loader_witness");
                        Ok(Some(res))
                    }
                    false => Ok(None),
                }
            }
            _ => {
                tracing::warn!("ignoring unexpected filetype: {file_type:?}");
                Ok(None)
            }
        }
    }

    async fn check_valid_gateway(
        &self,
        pub_key: &PublicKeyBinary,
        gateway_cache: &GatewayCache,
    ) -> bool {
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

    async fn check_unknown_gw(
        &self,
        pub_key: &PublicKeyBinary,
        gateway_cache: &GatewayCache,
    ) -> bool {
        gateway_cache.resolve_gateway_info(pub_key).await.is_err()
    }

    async fn check_gw_denied(&self, pub_key: &PublicKeyBinary) -> bool {
        self.deny_list.check_key(pub_key).await
    }
}
