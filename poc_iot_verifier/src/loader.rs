use crate::{
    gateway_cache::GatewayCache,
    meta::Meta,
    poc_report::{InsertBindings, LoraStatus, Report, ReportType},
    Settings,
};
use chrono::DateTime;
use chrono::{Duration as ChronoDuration, Utc};
use denylist::DenyList;
use file_store::{
    lora_beacon_report::LoraBeaconIngestReport, lora_witness_report::LoraWitnessIngestReport,
    traits::IngestId, FileInfo, FileStore, FileType,
};
use futures::{stream, StreamExt};
use helium_crypto::PublicKeyBinary;
use helium_proto::{
    services::poc_lora::{LoraBeaconIngestReportV1, LoraWitnessIngestReportV1},
    Message,
};
use sqlx::PgPool;
use std::{hash::Hasher, ops::DerefMut, time::Duration};
use tokio::{
    sync::Mutex,
    time::{self, MissedTickBehavior},
};
use twox_hash::XxHash64;
use xorf::{Filter as XorFilter, Xor16};

const REPORTS_META_NAME: &str = "report";
/// cadence for how often to look for  reports from s3 buckets
const REPORTS_POLL_TIME: u64 = 60 * 5;

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
                        tracing::error!("loader error, report_tick triggered: {err:?}");
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
        self.process_window(gateway_cache, after, before).await?;
        Meta::update_last_timestamp(&self.pool, REPORTS_META_NAME, Some(before)).await?;
        Report::pending_beacons_to_ready(&self.pool, now).await?;
        tracing::info!("completed handling poc_report tick");
        Ok(())
    }

    async fn process_window(
        &self,
        gateway_cache: &GatewayCache,
        after: DateTime<Utc>,
        before: DateTime<Utc>,
    ) -> anyhow::Result<()> {
        // beacons are processed first
        // an xor filter is constructed based on the beacon packet data
        // later when processing witnesses, the witness packet data
        // is checked against the xor filter
        // if not found the witness is deemed invalid and dropped
        let xor_data = Mutex::new(Vec::<u64>::new());

        // the arc is requried when processing beacons
        // filter is not required for beacons
        match self
            .process_events(
                FileType::LoraBeaconIngestReport,
                &self.ingest_store,
                gateway_cache,
                after,
                before,
                Some(&xor_data),
                None,
            )
            .await
        {
            Ok(()) => (),
            Err(err) => tracing::warn!(
                "error whilst processing {:?} from s3, error: {err:?}",
                FileType::LoraBeaconIngestReport
            ),
        }
        tracing::info!("creating beacon xor filter");
        let beacon_packet_data = xor_data.into_inner();
        tracing::info!("xor filter len {:?}", beacon_packet_data.len());
        let filter = Xor16::from(beacon_packet_data);
        tracing::info!("completed creating beacon xor filter");

        // process the witnesses
        // widen the window for these over that used for the beacons
        // this is to allow for a witness being in a rolled up file
        // from just before or after the beacon files
        // for witnesses we do need the filter but not the arc
        match self
            .process_events(
                FileType::LoraWitnessIngestReport,
                &self.ingest_store,
                gateway_cache,
                after - ChronoDuration::seconds(60 * 2),
                before + ChronoDuration::seconds(60 * 2),
                None,
                Some(&filter),
            )
            .await
        {
            Ok(()) => (),
            Err(err) => tracing::warn!(
                "error whilst processing {:?} from s3, error: {err:?}",
                FileType::LoraWitnessIngestReport
            ),
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn process_events(
        &self,
        file_type: FileType,
        store: &FileStore,
        gateway_cache: &GatewayCache,
        after: chrono::DateTime<Utc>,
        before: chrono::DateTime<Utc>,
        xor_data: Option<&Mutex<Vec<u64>>>,
        xor_filter: Option<&Xor16>,
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
        stream::iter(infos)
            .for_each_concurrent(10, |file_info| async move {
                match self
                    .process_file(
                        store,
                        file_info.clone(),
                        gateway_cache,
                        xor_data,
                        xor_filter,
                    )
                    .await
                {
                    Ok(()) => tracing::debug!(
                        "completed processing file of type {}, ts: {}",
                        &file_type,
                        file_info.timestamp
                    ),
                    Err(err) => tracing::warn!(
                        "error whilst processing file of type {}, ts: {}, err: {err:?}",
                        &file_type,
                        file_info.timestamp
                    ),
                };
            })
            .await;
        tracing::info!("completed processing {infos_len} files of type {file_type}");
        Ok(())
    }

    async fn process_file(
        &self,
        store: &FileStore,
        file_info: FileInfo,
        gateway_cache: &GatewayCache,
        xor_data: Option<&Mutex<Vec<u64>>>,
        xor_filter: Option<&Xor16>,
    ) -> anyhow::Result<()> {
        let file_type = file_info.file_type;
        let tx = Mutex::new(self.pool.begin().await?);
        store
            .stream_file(file_info.clone())
            .await?
            .chunks(600)
            .for_each_concurrent(10, |msgs| async {
                let mut inserts = Vec::new();
                for msg in msgs {
                    match msg {
                        Err(err) => {
                            tracing::warn!("skipping report of type {file_type} due to error {err:?}")
                        }
                        Ok(buf) => {
                            match self.handle_report(file_type, &buf, gateway_cache, xor_data, xor_filter).await
                                {
                                    Ok(Some(bindings)) =>  inserts.push(bindings),
                                    Ok(None) => (),
                                    Err(err) => tracing::warn!(
                                        "error whilst handling incoming report of type: {file_type}, error: {err:?}")

                                }
                        }
                    }
                }
                if !inserts.is_empty() {
                    match Report::bulk_insert(tx.lock().await.deref_mut(), inserts).await {
                        Ok(_) => (),
                        Err(err) => tracing::warn!("error whilst inserting report to db,  error: {err:?}"),
                    }
                }
            }).await;

        tx.into_inner().commit().await?;
        Ok(())
    }

    async fn handle_report(
        &self,
        file_type: FileType,
        buf: &[u8],
        gateway_cache: &GatewayCache,
        xor_data: Option<&Mutex<Vec<u64>>>,
        xor_filter: Option<&Xor16>,
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
                            status: LoraStatus::Pending,
                        };
                        metrics::increment_counter!("oracles_poc_iot_verifier_loader_beacon");
                        if let Some(xor_data) = xor_data {
                            xor_data
                                .lock()
                                .await
                                .deref_mut()
                                .push(filter_key_hash(&beacon.report.data))
                        };
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
                if let Some(filter) = xor_filter {
                    match verify_witness_packet_data(&packet_data, filter) {
                        true => {
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
                                        status: LoraStatus::Ready,
                                    };
                                    metrics::increment_counter!(
                                        "oracles_poc_iot_verifier_loader_witness"
                                    );
                                    Ok(Some(res))
                                }
                                false => Ok(None),
                            }
                        }
                        false => {
                            tracing::debug!(
                                "dropping witness report as no assocaited beacon data: {:?}",
                                packet_data
                            );
                            Ok(None)
                        }
                    }
                } else {
                    Ok(None)
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

fn filter_key_hash(data: &[u8]) -> u64 {
    let mut hasher = XxHash64::default();
    hasher.write(data);
    hasher.finish()
}

fn verify_witness_packet_data(packet: &[u8], filter: &Xor16) -> bool {
    filter.contains(&filter_key_hash(packet))
}
