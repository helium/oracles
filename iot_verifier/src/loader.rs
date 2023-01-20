use crate::{
    gateway_cache::GatewayCache,
    meta::Meta,
    metrics::LoaderMetricTracker,
    poc_report::{InsertBindings, IotStatus, Report, ReportType},
    Settings,
};
use chrono::DateTime;
use chrono::{Duration as ChronoDuration, Utc};
use denylist::DenyList;
use file_store::{
    iot_beacon_report::IotBeaconIngestReport, iot_witness_report::IotWitnessIngestReport,
    traits::IngestId, FileInfo, FileStore, FileType,
};
use futures::{stream, StreamExt};
use helium_crypto::PublicKeyBinary;
use helium_proto::{
    services::poc_iot::{IotBeaconIngestReportV1, IotWitnessIngestReportV1},
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

pub enum ValidGatewayResult {
    Valid,
    Denied,
    Unknown,
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
                FileType::IotBeaconIngestReport,
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
                FileType::IotBeaconIngestReport
            ),
        }
        tracing::info!("creating beacon xor filter");
        let mut beacon_packet_data = xor_data.into_inner();
        beacon_packet_data.sort_unstable();
        beacon_packet_data.dedup();
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
                FileType::IotWitnessIngestReport,
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
                FileType::IotWitnessIngestReport
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
        let metrics = LoaderMetricTracker::new();
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
                            match self.handle_report(file_type, &buf, gateway_cache, xor_data, xor_filter, &metrics).await
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
        metrics.record_metrics();
        Ok(())
    }

    async fn handle_report(
        &self,
        file_type: FileType,
        buf: &[u8],
        gateway_cache: &GatewayCache,
        xor_data: Option<&Mutex<Vec<u64>>>,
        xor_filter: Option<&Xor16>,
        metrics: &LoaderMetricTracker,
    ) -> anyhow::Result<Option<InsertBindings>> {
        match file_type {
            FileType::IotBeaconIngestReport => {
                let beacon: IotBeaconIngestReport =
                    IotBeaconIngestReportV1::decode(buf)?.try_into()?;
                tracing::debug!("beacon report from ingestor: {:?}", &beacon);
                let packet_data = beacon.report.data.clone();
                match self
                    .check_valid_gateway(&beacon.report.pub_key, gateway_cache)
                    .await
                {
                    ValidGatewayResult::Valid => {
                        let res = InsertBindings {
                            id: beacon.ingest_id(),
                            remote_entropy: beacon.report.remote_entropy,
                            packet_data,
                            buf: buf.to_vec(),
                            received_ts: beacon.received_timestamp,
                            report_type: ReportType::Beacon,
                            status: IotStatus::Pending,
                        };
                        metrics.increment_beacons();
                        if let Some(xor_data) = xor_data {
                            let key_hash = filter_key_hash(&beacon.report.data);
                            xor_data.lock().await.deref_mut().push(key_hash)
                        };
                        Ok(Some(res))
                    }
                    ValidGatewayResult::Denied => {
                        metrics.increment_beacons_denied();
                        Ok(None)
                    }

                    ValidGatewayResult::Unknown => {
                        metrics.increment_beacons_unknown();
                        Ok(None)
                    }
                }
            }
            FileType::IotWitnessIngestReport => {
                let witness: IotWitnessIngestReport =
                    IotWitnessIngestReportV1::decode(buf)?.try_into()?;
                tracing::debug!("witness report from ingestor: {:?}", &witness);
                let packet_data = witness.report.data.clone();
                if let Some(filter) = xor_filter {
                    match verify_witness_packet_data(&packet_data, filter) {
                        true => {
                            match self
                                .check_valid_gateway(&witness.report.pub_key, gateway_cache)
                                .await
                            {
                                ValidGatewayResult::Valid => {
                                    let res = InsertBindings {
                                        id: witness.ingest_id(),
                                        remote_entropy: Vec::<u8>::with_capacity(0),
                                        packet_data,
                                        buf: buf.to_vec(),
                                        received_ts: witness.received_timestamp,
                                        report_type: ReportType::Witness,
                                        status: IotStatus::Ready,
                                    };
                                    metrics.increment_witnesses();
                                    Ok(Some(res))
                                }
                                ValidGatewayResult::Denied => {
                                    metrics.increment_witnesses_denied();
                                    Ok(None)
                                }
                                ValidGatewayResult::Unknown => {
                                    metrics.increment_witnesses_unknown();
                                    Ok(None)
                                }
                            }
                        }
                        false => {
                            tracing::debug!(
                                "dropping witness report as no associated beacon data: {:?}",
                                packet_data
                            );
                            metrics.increment_witnesses_no_beacon();
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
    ) -> ValidGatewayResult {
        if self.check_gw_denied(pub_key).await {
            tracing::debug!("dropping denied gateway : {:?}", &pub_key);
            return ValidGatewayResult::Denied;
        }
        if self.check_unknown_gw(pub_key, gateway_cache).await {
            tracing::debug!("dropping unknown gateway: {:?}", &pub_key);
            return ValidGatewayResult::Unknown;
        }
        ValidGatewayResult::Valid
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
