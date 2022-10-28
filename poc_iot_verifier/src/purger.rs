use crate::{entropy::Entropy, poc_report::Report, Result, Settings};
use file_store::{
    file_sink, file_sink::MessageSender, file_sink_write, file_upload,
    lora_beacon_report::LoraBeaconIngestReport, lora_invalid_poc::LoraInvalidBeaconReport,
    lora_invalid_poc::LoraInvalidWitnessReport, lora_witness_report::LoraWitnessIngestReport,
    FileType,
};
use helium_proto::services::poc_lora::{
    InvalidParticipantSide, InvalidReason, LoraBeaconIngestReportV1, LoraInvalidBeaconReportV1,
    LoraInvalidWitnessReportV1, LoraWitnessIngestReportV1,
};
use std::path::Path;

use helium_proto::Message;
use sha2::{Digest, Sha256};
use sqlx::PgPool;
use tokio::time;

const DB_POLL_TIME: time::Duration = time::Duration::from_secs(300);
const LOADER_WORKERS: usize = 10;
const LOADER_DB_POOL_SIZE: usize = 2 * LOADER_WORKERS;
/// the period of time in seconds after which entropy will be deemed stale
/// and purged from the DB
// any beacon or witness using this entropy & received after this period will fail
// due to being stale
// the report itself will never be verified but instead handled by the stale purger
// this value will be added to the env var BASE_STALE_PERIOD to determine final setting
const ENTROPY_STALE_PERIOD: i32 = 60 * 60 * 8; // 8 hours in seconds
/// the period in seconds after when a beacon or witness report in the DB will be deemed stale
// this period needs to be sufficiently long that we can be sure the beacon has had the
// opportunity to be verified and after this point extremely unlikely to ever be verified
// successfully
// this value will be added to the env var BASE_STALE_PERIOD to determine final setting
const REPORT_STALE_PERIOD: i32 = 60 * 60 * 8; // 8 hours in seconds;

pub struct Purger {
    pool: PgPool,
    base_stale_period: i32,
    settings: Settings,
}

impl Purger {
    pub async fn from_settings(settings: &Settings) -> Result<Self> {
        let pool = settings.database.connect(LOADER_DB_POOL_SIZE).await?;
        let settings = settings.clone();
        // get the base_stale period
        // if the env var is set, this value will be added to the entropy and report
        // stale periods and is to prevent data being unnecessarily purged
        // in the event the verifier is down for an extended period of time
        let base_stale_period: i32 = std::env::var("BASE_STALE_PERIOD")
            .map_or_else(|_| 0, |v: String| v.parse::<i32>().unwrap_or(0));
        Ok(Self {
            pool,
            settings,
            base_stale_period,
        })
    }

    pub async fn run(&self, shutdown: &triggered::Listener) -> Result {
        tracing::info!("starting purger");

        let mut db_timer = time::interval(DB_POLL_TIME);
        db_timer.set_missed_tick_behavior(time::MissedTickBehavior::Delay);

        let store_base_path = Path::new(&self.settings.cache);
        let (lora_invalid_beacon_tx, lora_invalid_beacon_rx) = file_sink::message_channel(50);
        let (lora_invalid_witness_tx, lora_invalid_witness_rx) = file_sink::message_channel(50);

        let (file_upload_tx, file_upload_rx) = file_upload::message_channel();
        let file_upload =
            file_upload::FileUpload::from_settings(&self.settings.output, file_upload_rx).await?;

        let mut lora_invalid_beacon_sink = file_sink::FileSinkBuilder::new(
            FileType::LoraInvalidBeaconReport,
            store_base_path,
            lora_invalid_beacon_rx,
        )
        .deposits(Some(file_upload_tx.clone()))
        .create()
        .await?;

        let mut lora_invalid_witness_sink = file_sink::FileSinkBuilder::new(
            FileType::LoraInvalidWitnessReport,
            store_base_path,
            lora_invalid_witness_rx,
        )
        .deposits(Some(file_upload_tx.clone()))
        .create()
        .await?;

        // spawn off the file sinks
        let shutdown2 = shutdown.clone();
        let shutdown3 = shutdown.clone();
        let shutdown4 = shutdown.clone();
        tokio::spawn(async move { lora_invalid_beacon_sink.run(&shutdown2).await });
        tokio::spawn(async move { lora_invalid_witness_sink.run(&shutdown3).await });
        tokio::spawn(async move { file_upload.run(&shutdown4).await });

        loop {
            if shutdown.is_triggered() {
                break;
            }
            tokio::select! {
                _ = shutdown.clone() => break,
                _ = db_timer.tick() =>
                    match self.handle_db_tick(lora_invalid_beacon_tx.clone(),lora_invalid_witness_tx.clone()).await {
                    Ok(()) => (),
                    Err(err) => {
                        tracing::error!("fatal purger error: {err:?}");
                        return Err(err)
                    }
                }
            }
        }
        tracing::info!("stopping purger");
        Ok(())
    }

    async fn handle_db_tick(
        &self,
        lora_invalid_beacon_tx: MessageSender,
        lora_invalid_witness_tx: MessageSender,
    ) -> Result {
        // pull stale beacons and witnesses which are in a pending state
        // for each we have to write out an invalid report to S3
        // as these wont have previously resulted in a file going to s3
        // once the report is safely on s3 we can then proceed to purge from the db
        _ = Report::get_stale_pending_beacons(
            &self.pool,
            self.base_stale_period + REPORT_STALE_PERIOD,
        )
        .await?
        .iter()
        .map(|report| self.handle_purged_beacon(report, &lora_invalid_beacon_tx));

        _ = Report::get_stale_pending_witnesses(
            &self.pool,
            self.base_stale_period + REPORT_STALE_PERIOD,
        )
        .await?
        .iter()
        .map(|report| self.handle_purged_witness(report, &lora_invalid_witness_tx));

        // purge any stale entropy, no need to output anything to s3 here
        _ = Entropy::purge(&self.pool, self.base_stale_period + ENTROPY_STALE_PERIOD).await;
        Ok(())
    }

    async fn handle_purged_beacon(
        &self,
        db_beacon: &Report,
        lora_invalid_beacon_tx: &MessageSender,
    ) -> Result {
        let packet_data = &db_beacon.packet_data;
        let beacon_buf: &[u8] = &db_beacon.report_data;
        let beacon_report: LoraBeaconIngestReport =
            LoraBeaconIngestReportV1::decode(beacon_buf)?.try_into()?;
        let beacon = &beacon_report.report;
        let received_timestamp = beacon_report.received_timestamp;
        let invalid_beacon_proto: LoraInvalidBeaconReportV1 = LoraInvalidBeaconReport {
            received_timestamp,
            reason: InvalidReason::Stale,
            report: beacon.clone(),
        }
        .into();
        tracing::debug!("purging beacon with date: {received_timestamp}");
        file_sink_write!(
            "invalid_beacon",
            lora_invalid_beacon_tx,
            invalid_beacon_proto
        )
        .await?;
        // delete the report from the DB
        let public_key = beacon_report.report.pub_key.to_vec();
        self.delete_db_report(public_key, packet_data.clone()).await;
        Ok(())
    }

    async fn handle_purged_witness(
        &self,
        db_witness: &Report,
        lora_invalid_witness_tx: &MessageSender,
    ) -> Result {
        let packet_data = &db_witness.packet_data;
        let witness_buf: &[u8] = &db_witness.report_data;
        let witness_report: LoraWitnessIngestReport =
            LoraWitnessIngestReportV1::decode(witness_buf)?.try_into()?;
        let witness = &witness_report.report;
        let public_key = witness.pub_key.to_vec().clone();
        let received_timestamp = witness_report.received_timestamp;
        let invalid_witness_report_proto: LoraInvalidWitnessReportV1 = LoraInvalidWitnessReport {
            received_timestamp,
            report: witness_report.report,
            reason: InvalidReason::Stale,
            participant_side: InvalidParticipantSide::Witness,
        }
        .into();
        tracing::debug!("purging witness with date: {received_timestamp}");
        file_sink_write!(
            "invalid_witness_report",
            lora_invalid_witness_tx,
            invalid_witness_report_proto
        )
        .await?;

        // delete the report from the DB
        self.delete_db_report(public_key, packet_data.clone()).await;
        Ok(())
    }

    /// delete the report from the DB using ID
    async fn delete_db_report(&self, mut pub_key_bytes: Vec<u8>, packet_data: Vec<u8>) {
        // delete the report from the DB using ID
        let mut id: Vec<u8> = packet_data;
        id.append(&mut pub_key_bytes);
        let id_hash = Sha256::digest(&id).to_vec();
        _ = Report::delete_report(&self.pool, &id_hash).await;
    }
}
