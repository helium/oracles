use crate::{entropy::Entropy, metrics::Metrics, poc_report::Report, Settings};
use file_store::{
    file_sink::{self, FileSinkClient},
    file_upload,
    iot_beacon_report::IotBeaconIngestReport,
    iot_invalid_poc::IotInvalidBeaconReport,
    iot_invalid_poc::IotInvalidWitnessReport,
    iot_witness_report::IotWitnessIngestReport,
    traits::{IngestId, MsgDecode},
    FileType,
};
use helium_proto::services::poc_lora::{
    InvalidParticipantSide, InvalidReason, LoraInvalidBeaconReportV1, LoraInvalidWitnessReportV1,
};
use std::{ops::DerefMut, path::Path};

use futures::stream::{self, StreamExt};
use sqlx::{PgPool, Postgres};
use tokio::{
    sync::Mutex,
    time::{self, MissedTickBehavior},
};

const DB_POLL_TIME: time::Duration = time::Duration::from_secs(60 * 35);
const PURGER_WORKERS: usize = 50;

/// the period in seconds after when a beacon report in the DB will be deemed stale
// this period needs to be sufficiently long that we can be sure the beacon has had the
// opportunity to be verified and after this point extremely unlikely to ever be verified
// successfully
// this value will be added to the env var BASE_STALE_PERIOD to determine final setting
const BEACON_STALE_PERIOD: i64 = 60 * 45;
/// the period in seconds after when a witness report in the DB will be deemed stale
const WITNESS_STALE_PERIOD: i64 = 60 * 45;
/// the period of time in seconds after which entropy will be deemed stale
/// and purged from the DB
// this value should be > that beacon stale period to allow for any beacon
// to be verified right up to the end of its stale period
// any beacon or witness using this entropy & received after this period will fail
// due to being stale
// the report itself will never be verified but instead handled by the stale purger
// this value will be added to the env var BASE_STALE_PERIOD to determine final setting
const ENTROPY_STALE_PERIOD: i64 = BEACON_STALE_PERIOD + (15 * 60);

pub struct Purger {
    pool: PgPool,
    base_stale_period: i64,
    settings: Settings,
}

#[derive(thiserror::Error, Debug)]
#[error("error creating purger: {0}")]
pub struct NewPurgerError(#[from] db_store::Error);

impl Purger {
    pub async fn from_settings(settings: &Settings, pool: PgPool) -> Result<Self, NewPurgerError> {
        let settings = settings.clone();
        let base_stale_period = settings.base_stale_period;
        Ok(Self {
            pool,
            settings,
            base_stale_period,
        })
    }

    pub async fn run(&self, shutdown: &triggered::Listener) -> anyhow::Result<()> {
        tracing::info!("starting purger");

        let mut db_timer = time::interval(DB_POLL_TIME);
        db_timer.set_missed_tick_behavior(MissedTickBehavior::Skip);

        let store_base_path = Path::new(&self.settings.cache);
        let (file_upload_tx, file_upload_rx) = file_upload::message_channel();
        let file_upload =
            file_upload::FileUpload::from_settings(&self.settings.output, file_upload_rx).await?;

        let (invalid_beacon_sink, mut invalid_beacon_sink_server) =
            file_sink::FileSinkBuilder::new(
                FileType::IotInvalidBeaconReport,
                store_base_path,
                concat!(env!("CARGO_PKG_NAME"), "_invalid_beacon"),
            )
            .deposits(Some(file_upload_tx.clone()))
            .auto_commit(false)
            .create()
            .await?;

        let (invalid_witness_sink, mut invalid_witness_sink_server) =
            file_sink::FileSinkBuilder::new(
                FileType::IotInvalidWitnessReport,
                store_base_path,
                concat!(env!("CARGO_PKG_NAME"), "_invalid_witness_report"),
            )
            .deposits(Some(file_upload_tx.clone()))
            .auto_commit(false)
            .create()
            .await?;

        // spawn off the file sinks
        let shutdown2 = shutdown.clone();
        let shutdown3 = shutdown.clone();
        let shutdown4 = shutdown.clone();
        tokio::spawn(async move { invalid_beacon_sink_server.run(&shutdown2).await });
        tokio::spawn(async move { invalid_witness_sink_server.run(&shutdown3).await });
        tokio::spawn(async move { file_upload.run(&shutdown4).await });

        loop {
            if shutdown.is_triggered() {
                break;
            }
            tokio::select! {
                _ = shutdown.clone() => break,
                _ = db_timer.tick() =>
                    match self.handle_db_tick(&invalid_beacon_sink, &invalid_witness_sink).await {
                    Ok(()) => (),
                    Err(err) => {
                        tracing::error!("fatal purger error: {err:?}");
                    }
                }
            }
        }
        tracing::info!("stopping purger");
        Ok(())
    }

    async fn handle_db_tick(
        &self,
        invalid_beacon_sink: &FileSinkClient,
        invalid_witness_sink: &FileSinkClient,
    ) -> anyhow::Result<()> {
        // pull stale beacons and witnesses
        // for each we have to write out an invalid report to S3
        // as these wont have previously resulted in a file going to s3
        // once the report is safely on s3 we can then proceed to purge from the db
        let beacon_stale_period = self.base_stale_period + BEACON_STALE_PERIOD;
        tracing::info!(
            "starting query get_stale_pending_beacons with stale period: {beacon_stale_period}"
        );
        let stale_beacons = Report::get_stale_beacons(&self.pool, beacon_stale_period).await?;
        tracing::info!("completed query get_stale_beacons");
        tracing::info!("purging {:?} stale beacons", stale_beacons.len());

        let tx = Mutex::new(self.pool.begin().await?);
        stream::iter(stale_beacons)
            .for_each_concurrent(PURGER_WORKERS, |report| async {
                match self
                    .handle_purged_beacon(&tx, report, invalid_beacon_sink)
                    .await
                {
                    Ok(()) => (),
                    Err(err) => {
                        tracing::warn!("failed to purge beacon: {err:?}")
                    }
                }
            })
            .await;
        invalid_beacon_sink.commit().await?;
        tx.into_inner().commit().await?;

        let witness_stale_period = self.base_stale_period + WITNESS_STALE_PERIOD;
        tracing::info!(
            "starting query get_stale_pending_witnesses with stale period: {witness_stale_period}"
        );
        let stale_witnesses = Report::get_stale_witnesses(&self.pool, witness_stale_period).await?;
        tracing::info!("completed query get_stale_witnesses");
        let num_stale_witnesses = stale_witnesses.len();
        tracing::info!("purging {num_stale_witnesses} stale witnesses");

        let tx = Mutex::new(self.pool.begin().await?);
        stream::iter(stale_witnesses)
            .for_each_concurrent(PURGER_WORKERS, |report| async {
                match self
                    .handle_purged_witness(&tx, report, invalid_witness_sink)
                    .await
                {
                    Ok(()) => (),
                    Err(err) => {
                        tracing::warn!("failed to purge witness: {err:?}")
                    }
                }
            })
            .await;
        invalid_witness_sink.commit().await?;
        tx.into_inner().commit().await?;
        tracing::info!("completed purging {num_stale_witnesses} stale witnesses");

        // purge any stale entropy, no need to output anything to s3 here
        _ = Entropy::purge(&self.pool, self.base_stale_period + ENTROPY_STALE_PERIOD).await;
        Ok(())
    }

    async fn handle_purged_beacon(
        &self,
        tx: &Mutex<sqlx::Transaction<'_, Postgres>>,
        db_beacon: Report,
        invalid_beacon_sink: &FileSinkClient,
    ) -> anyhow::Result<()> {
        let beacon_buf: &[u8] = &db_beacon.report_data;
        let beacon_report = IotBeaconIngestReport::decode(beacon_buf)?;
        let beacon_id = beacon_report.ingest_id();
        let beacon = &beacon_report.report;
        let received_timestamp = beacon_report.received_timestamp;
        let invalid_beacon_proto: LoraInvalidBeaconReportV1 = IotInvalidBeaconReport {
            received_timestamp,
            reason: InvalidReason::Stale,
            report: beacon.clone(),
        }
        .into();

        invalid_beacon_sink
            .write(
                invalid_beacon_proto,
                &[("reason", InvalidReason::Stale.as_str_name())],
            )
            .await?;
        // delete the report from the DB
        Report::delete_report(tx.lock().await.deref_mut(), &beacon_id).await?;
        Metrics::decrement_num_beacons();
        Ok(())
    }

    async fn handle_purged_witness(
        &self,
        tx: &Mutex<sqlx::Transaction<'_, Postgres>>,
        db_witness: Report,
        invalid_witness_sink: &FileSinkClient,
    ) -> anyhow::Result<()> {
        let witness_buf: &[u8] = &db_witness.report_data;
        let witness_report = IotWitnessIngestReport::decode(witness_buf)?;
        let witness_id = witness_report.ingest_id();
        let received_timestamp = witness_report.received_timestamp;
        let invalid_witness_report_proto: LoraInvalidWitnessReportV1 = IotInvalidWitnessReport {
            received_timestamp,
            report: witness_report.report,
            reason: InvalidReason::Stale,
            participant_side: InvalidParticipantSide::Witness,
        }
        .into();

        invalid_witness_sink
            .write(
                invalid_witness_report_proto,
                &[("reason", InvalidReason::Stale.as_str_name())],
            )
            .await?;

        // delete the report from the DB
        Report::delete_report(tx.lock().await.deref_mut(), &witness_id).await?;
        Ok(())
    }
}
