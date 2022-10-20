use crate::{entropy::Entropy, mk_db_pool, poc_report::Report, Result};
use file_store::{
    file_sink, file_sink::MessageSender, file_upload, lora_beacon_report::LoraBeaconIngestReport,
    lora_invalid_poc::LoraInvalidBeaconReport, lora_invalid_poc::LoraInvalidWitnessReport,
    lora_witness_report::LoraWitnessIngestReport, FileType,
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
const LOADER_WORKERS: u32 = 10;
const LOADER_DB_POOL_SIZE: u32 = 2 * LOADER_WORKERS;
/// the fallback value to define the period in seconds after when a piece of entropy
/// in the DB will be deemed stale and purged from the DB
/// any beacon or witness using this entropy & received after this period will fail
/// due to being stale
/// the report itself will never be verified but instead handled by the stale purger
/// NOTE: the verifier being down needs to be considered here
const ENTROPY_STALE_PERIOD: i32 = 60 * 60 * 8; // 8 hours in seconds

pub struct Purger {
    pool: PgPool,
    store_path: String,
    entropy_stale_period: i32,
}

impl Purger {
    pub async fn from_env() -> Result<Self> {
        let pool = mk_db_pool(LOADER_DB_POOL_SIZE).await?;
        let store_path =
            std::env::var("VERIFIER_STORE").unwrap_or_else(|_| String::from("/var/data/verifier"));
        let entropy_stale_period: i32 = std::env::var("ENTROPY_STALE_PERIOD").map_or_else(
            |_| ENTROPY_STALE_PERIOD,
            |v: String| v.parse::<i32>().unwrap_or(ENTROPY_STALE_PERIOD),
        );
        Ok(Self {
            pool,
            store_path,
            entropy_stale_period,
        })
    }

    pub async fn run(&self, shutdown: &triggered::Listener) -> Result {
        tracing::info!("starting purger");

        let mut db_timer = time::interval(DB_POLL_TIME);
        db_timer.set_missed_tick_behavior(time::MissedTickBehavior::Delay);

        let store_base_path = Path::new(&self.store_path);
        let (lora_invalid_beacon_tx, lora_invalid_beacon_rx) = file_sink::message_channel(50);
        let (lora_invalid_witness_tx, lora_invalid_witness_rx) = file_sink::message_channel(50);

        let (file_upload_tx, file_upload_rx) = file_upload::message_channel();
        let file_upload =
            file_upload::FileUpload::from_env_with_prefix("VERIFIER", file_upload_rx).await?;

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
        _ = Report::get_stale_pending_beacons(&self.pool)
            .await?
            .iter()
            .map(|report| self.handle_purged_beacon(report, &lora_invalid_beacon_tx));

        _ = Report::get_stale_pending_witnesses(&self.pool)
            .await?
            .iter()
            .map(|report| self.handle_purged_witness(report, &lora_invalid_witness_tx));

        // purge any stale entropy, no need to output anything to s3 here
        _ = Entropy::purge(&self.pool, &self.entropy_stale_period).await;
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
        let invalid_beacon_proto: LoraInvalidBeaconReportV1 = LoraInvalidBeaconReport {
            received_timestamp: beacon_report.received_timestamp,
            reason: InvalidReason::Stale,
            report: beacon.clone(),
        }
        .into();
        file_sink::write(lora_invalid_beacon_tx, invalid_beacon_proto).await?;
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
        let invalid_witness_report_proto: LoraInvalidWitnessReportV1 = LoraInvalidWitnessReport {
            received_timestamp: witness_report.received_timestamp,
            report: witness_report.report,
            reason: InvalidReason::Stale,
            participant_side: InvalidParticipantSide::Witness,
        }
        .into();
        file_sink::write(lora_invalid_witness_tx, invalid_witness_report_proto).await?;

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
