//
// the loader is responsible for processing incoming beacon and witness reports from S3
// its purpose is to validate the reports with basic sanity checks and insert them into the db
// from where they are subsequently picked up by the runner
//
// Some key points to note:
//
// ** protect the db **
// due to the volume of beacon & witness reports, the loader will attempt to drop invalid reports as early as possible
// this is to 'protect' the db from being overwhelmed with spurious or invalid data
// to achieve this the loader will first process beacon reports and construct an xor filter based on the beacon packet data
// the loader will then process witness reports
// the witness packet data will be checked against the xor filter
// if the witness packet data is not found in the xor filter, then it suggests a spurious or orphaned report
// and will be dropped
// NOTE: any reports dropped at this stage are done so silently, there is no paper trail to S3
// The deny list check used to be performed here, but it was removed in PR # 588 as we wanted
// a paper trail of the deny listed reports to hit s3

// ** sliding window **
// As part of the "protect the db" goal, the loader processes files from s3 via a sliding window
// it does not use the typical fileinfopoller approach as used elsewhere
// for any one tick, the loader will process beacon files from s3 that fall within the window
// the loader will then process witness files from s3 that fall within the window and attempt to match them with the beacon reports
// however for witnesses we need to accommodate the fact that witnesses and beacons may arrive
// at the ingestor out of order, ie we can have a witness report for a beacon which arrives before
// or much later than the beacon
// as such the window for witness reports needs to be wider than that used for beacon reports
// so that it looks for witnesses in the roll ups before and after the beacon window
// the extended width of the witness window needs to be at least equal to the ingestor roll up time plus a buffer

// ** Alternative approach **:
// the approach with the sliding window is there to protect the db from being overwhelmed
// by dropping out spurious/orphaned reports as early as possible and before they hit the db
// the end result is a loader that is much more complicated than our typical consumers/loaders
// a simpler alternative is to use the regular fileinfopoller approach
// drop the xor filtering and dump all reports into the db... effectively use the db as a scratch area
// the runner is already setup to only pull witness reports which have a corresponding beacon report
// and any spurious or orphaned reports will end up being purged by the purger
// this approach is simpler and more in line with our other consumers/loaders and will be easier to maintain
// an earlier version of the loader used this approach but was changed to the current setup
// as it was thought that the db would be overwhelmed with spurious reports
// however I am no longer convinced this is a serious concern, especially after we moved the deny list check
// from the loader to the runner and thus all reports from denied gateways are already hitting the db
// in addition the volume of reports is significantly less given a much lower active hotspot count
//

use crate::{
    gateway_cache::GatewayCache,
    meta::Meta,
    poc_report::{InsertBindings, IotStatus, Report, ReportType},
    telemetry::LoaderMetricTracker,
    Settings,
};
use chrono::DateTime;
use chrono::Utc;
use file_store::{
    iot_beacon_report::IotBeaconIngestReport,
    iot_witness_report::IotWitnessIngestReport,
    traits::{IngestId, MsgDecode},
    FileInfo, FileStore, FileType,
};
use futures::{future::LocalBoxFuture, stream, StreamExt};
use helium_crypto::PublicKeyBinary;
use humantime_serde::re::humantime;
use sqlx::PgPool;
use std::{hash::Hasher, ops::DerefMut, str::FromStr, time::Duration};
use task_manager::ManagedTask;
use tokio::{
    sync::Mutex,
    time::{self, MissedTickBehavior},
};
use twox_hash::XxHash64;
use xorf::{Filter as XorFilter, Xor16};

const REPORTS_META_NAME: &str = "report";

pub struct Loader {
    ingest_store: FileStore,
    pool: PgPool,
    poll_time: time::Duration,
    window_width: Duration,
    ingestor_rollup_time: Duration,
    max_lookback_age: Duration,
    gateway_cache: GatewayCache,
}

#[derive(thiserror::Error, Debug)]
pub enum NewLoaderError {
    #[error("file store error: {0}")]
    FileStoreError(#[from] file_store::Error),
    #[error("db_store error: {0}")]
    DbStoreError(#[from] db_store::Error),
}

pub enum ValidGatewayResult {
    Valid,
    Unknown,
}

impl ManagedTask for Loader {
    fn start_task(
        self: Box<Self>,
        shutdown: triggered::Listener,
    ) -> LocalBoxFuture<'static, anyhow::Result<()>> {
        Box::pin(self.run(shutdown))
    }
}

impl Loader {
    pub async fn from_settings(
        settings: &Settings,
        pool: PgPool,
        gateway_cache: GatewayCache,
    ) -> Result<Self, NewLoaderError> {
        tracing::info!("from_settings verifier loader");
        let ingest_store = FileStore::from_settings(&settings.ingest).await?;
        Ok(Self {
            pool,
            ingest_store,
            poll_time: settings.poc_loader_poll_time,
            window_width: settings.poc_loader_window_width,
            ingestor_rollup_time: settings.ingestor_rollup_time,
            max_lookback_age: settings.loader_window_max_lookback_age,
            gateway_cache,
        })
    }

    pub async fn run(self, shutdown: triggered::Listener) -> anyhow::Result<()> {
        tracing::info!("starting loader");
        let mut report_timer = time::interval(self.poll_time);
        report_timer.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                biased;
                _ = shutdown.clone() => break,
                _ = report_timer.tick() => match self.handle_report_tick().await {
                    Ok(()) => (),
                    Err(err) => {
                        tracing::error!("loader error, report_tick triggered: {err:?}");
                    }
                }
            }
        }
        tracing::info!("stopping loader");
        Ok(())
    }

    async fn handle_report_tick(&self) -> anyhow::Result<()> {
        tracing::info!("handling report tick");
        let now = Utc::now();
        // set up the sliding window
        // if there is no last timestamp in the meta db, the window start point will be
        // Now() - (window_width * 4)
        // as such data loading is always behind by a value equal to window_width * 4
        let window_default_lookback = now - (self.window_width * 4);

        // cap the starting point of the window at the max below.
        let window_max_lookback = now - self.max_lookback_age;
        tracing::info!(
            "default window: {window_default_lookback}, max window: {window_max_lookback}"
        );

        // *after* is the window start point
        let after = Meta::last_timestamp(&self.pool, REPORTS_META_NAME)
            .await?
            .unwrap_or(window_default_lookback)
            .max(window_max_lookback);

        // *before* is the window end point
        // this can never be later than now - window width * 3
        // otherwise we have no room to widen the window for witness reports
        let before_max = after + self.window_width;
        let before = (now - (self.window_width * 3)).min(before_max);

        let cur_window_width = (before - after).to_std()?;
        tracing::info!(
            "sliding window, after: {after}, before: {before}, cur width: {:?}, required width: {:?}",
            humantime::format_duration(cur_window_width),
            humantime::format_duration(self.window_width)
        );

        // if there is no room for our desired window width
        // eg. if the before time is after now - window width * 3
        // then do nothing and try again next tick
        if cur_window_width < self.window_width {
            tracing::info!("current window width insufficient. completed handling poc_report tick");
            return Ok(());
        }

        self.process_window(after, before).await?;

        // TODO - wrap the db writes in a transaction
        Meta::update_last_timestamp(&self.pool, REPORTS_META_NAME, Some(before)).await?;
        Report::pending_beacons_to_ready(&self.pool, now).await?;

        tracing::info!("completed handling poc_report tick");
        Ok(())
    }

    async fn process_window(
        &self,
        after: DateTime<Utc>,
        before: DateTime<Utc>,
    ) -> anyhow::Result<()> {
        // create the inital xor data vec
        // wrapped in a mutex to allow for concurrent access
        let xor_data = Mutex::new(Vec::<u64>::new());

        // process the beacons
        match self
            .process_events(
                FileType::IotBeaconIngestReport,
                &self.ingest_store,
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

        // create the xor filter from the vec of beacon data
        let mut beacon_packet_data = xor_data.into_inner();
        beacon_packet_data.sort_unstable();
        beacon_packet_data.dedup();
        let xor_len = beacon_packet_data.len();
        let filter = Xor16::from(beacon_packet_data);
        tracing::info!("created beacon xor filter. filter len: {:?}", filter.len());

        // if the xor filter is empty, then dont go any further
        // no point processing witnesses if no beacons to associate with
        if xor_len == 0 {
            return Ok(());
        };

        // process the witnesses
        // widen the window over that used for the beacons
        // this is to allow for a witness being in a roll up file
        // from before or after the beacon report
        // the width extension needs to be at least equal to that
        // of the ingestor roll up time plus a buffer
        // to account for the potential of the ingestor write time for
        // witness reports being out of sync with that of beacon files
        let buffer = Duration::from_secs(120);
        match self
            .process_events(
                FileType::IotWitnessIngestReport,
                &self.ingest_store,
                after - (self.ingestor_rollup_time + buffer),
                before + (self.ingestor_rollup_time + buffer),
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
        after: chrono::DateTime<Utc>,
        before: chrono::DateTime<Utc>,
        xor_data: Option<&Mutex<Vec<u64>>>,
        xor_filter: Option<&Xor16>,
    ) -> anyhow::Result<()> {
        tracing::info!(
            "checking for new ingest files of type {file_type} after {after} and before {before}"
        );
        let infos = store.list_all(file_type.to_str(), after, before).await?;
        if infos.is_empty() {
            tracing::info!("no available ingest files of type {file_type}");
            return Ok(());
        }
        let infos_len = infos.len();
        tracing::info!("processing {infos_len} ingest files of type {file_type}");
        stream::iter(infos)
            .for_each_concurrent(10, |file_info| async move {
                match self
                    .process_file(store, file_info.clone(), xor_data, xor_filter)
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
        xor_data: Option<&Mutex<Vec<u64>>>,
        xor_filter: Option<&Xor16>,
    ) -> anyhow::Result<()> {
        let file_type = file_info.prefix.clone();
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
                            match self.handle_report(&file_type, &buf, xor_data, xor_filter, &metrics).await
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
        file_type: &str,
        buf: &[u8],
        xor_data: Option<&Mutex<Vec<u64>>>,
        xor_filter: Option<&Xor16>,
        metrics: &LoaderMetricTracker,
    ) -> anyhow::Result<Option<InsertBindings>> {
        match FileType::from_str(file_type)? {
            FileType::IotBeaconIngestReport => {
                let beacon = IotBeaconIngestReport::decode(buf)?;
                tracing::debug!("beacon report from ingestor: {:?}", &beacon);
                let packet_data = beacon.report.data.clone();
                match self.check_valid_gateway(&beacon.report.pub_key).await {
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
                    ValidGatewayResult::Unknown => {
                        metrics.increment_beacons_unknown();
                        Ok(None)
                    }
                }
            }
            FileType::IotWitnessIngestReport => {
                let witness = IotWitnessIngestReport::decode(buf)?;
                tracing::debug!("witness report from ingestor: {:?}", &witness);
                let packet_data = witness.report.data.clone();
                if let Some(filter) = xor_filter {
                    match verify_witness_packet_data(&packet_data, filter) {
                        true => match self.check_valid_gateway(&witness.report.pub_key).await {
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
                            ValidGatewayResult::Unknown => {
                                metrics.increment_witnesses_unknown();
                                Ok(None)
                            }
                        },
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

    async fn check_valid_gateway(&self, pub_key: &PublicKeyBinary) -> ValidGatewayResult {
        if self.check_unknown_gw(pub_key).await {
            tracing::debug!("dropping unknown gateway: {:?}", &pub_key);
            return ValidGatewayResult::Unknown;
        }
        ValidGatewayResult::Valid
    }

    async fn check_unknown_gw(&self, pub_key: &PublicKeyBinary) -> bool {
        self.gateway_cache
            .resolve_gateway_info(pub_key)
            .await
            .is_err()
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
