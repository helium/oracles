use crate::{
    last_beacon::LastBeacon,
    poc::{Poc, VerificationStatus, VerifyWitnessesResult},
    poc_report::{LoraStatus, Report},
    Error, Result, Settings,
};
use chrono::{Duration as ChronoDuration, Utc};
use density_scaler::QuerySender;
use file_store::{
    file_sink,
    file_sink::MessageSender,
    file_sink_write, file_upload,
    lora_beacon_report::{LoraBeaconIngestReport, LoraBeaconReport},
    lora_invalid_poc::{LoraInvalidBeaconReport, LoraInvalidWitnessReport},
    lora_valid_poc::{LoraValidBeaconReport, LoraValidPoc},
    lora_witness_report::LoraWitnessIngestReport,
    traits::{IngestId, ReportId},
    FileType,
};
use futures::stream::{self, StreamExt};
use helium_proto::{
    services::poc_lora::{
        InvalidParticipantSide, InvalidReason, LoraBeaconIngestReportV1, LoraInvalidBeaconReportV1,
        LoraInvalidWitnessReportV1, LoraValidPocV1, LoraWitnessIngestReportV1,
    },
    Message,
};
use node_follower::follower_service::FollowerService;
use sqlx::PgPool;
use std::path::Path;
use tokio::time;

const BEACON_WORKERS: usize = 30;
/// the cadence in seconds at which the DB is polled for ready POCs
const DB_POLL_TIME: time::Duration = time::Duration::from_secs(8 * 60 + 10);
const LOADER_WORKERS: usize = 40;
const LOADER_DB_POOL_SIZE: usize = 2 * LOADER_WORKERS;

pub struct Runner {
    pool: PgPool,
    settings: Settings,
    follower_service: FollowerService,
    density_queries: Option<QuerySender>,
}

impl Runner {
    pub async fn from_settings(settings: &Settings) -> Result<Self> {
        let pool = settings.database.connect(LOADER_DB_POOL_SIZE).await?;
        let follower_service = FollowerService::from_settings(&settings.follower)?;
        Ok(Self {
            pool,
            settings: settings.clone(),
            follower_service,
            density_queries: None,
        })
    }

    pub async fn run(
        &mut self,
        density_queries: QuerySender,
        shutdown: &triggered::Listener,
    ) -> Result {
        tracing::info!("starting runner");

        self.density_queries = Some(density_queries);

        let mut db_timer = time::interval(DB_POLL_TIME);
        db_timer.set_missed_tick_behavior(time::MissedTickBehavior::Delay);

        let store_base_path = Path::new(&self.settings.cache);
        let (lora_invalid_beacon_tx, lora_invalid_beacon_rx) = file_sink::message_channel(50);
        let (lora_invalid_witness_tx, lora_invalid_witness_rx) = file_sink::message_channel(50);
        let (lora_valid_poc_tx, lora_valid_poc_rx) = file_sink::message_channel(50);

        let (file_upload_tx, file_upload_rx) = file_upload::message_channel();
        let file_upload =
            file_upload::FileUpload::from_settings(&self.settings.output, file_upload_rx).await?;

        let mut lora_invalid_beacon_sink = file_sink::FileSinkBuilder::new(
            FileType::LoraInvalidBeaconReport,
            store_base_path,
            lora_invalid_beacon_rx,
        )
        .deposits(Some(file_upload_tx.clone()))
        .roll_time(ChronoDuration::minutes(1))
        .create()
        .await?;

        let mut lora_invalid_witness_sink = file_sink::FileSinkBuilder::new(
            FileType::LoraInvalidWitnessReport,
            store_base_path,
            lora_invalid_witness_rx,
        )
        .deposits(Some(file_upload_tx.clone()))
        .roll_time(ChronoDuration::minutes(5))
        .create()
        .await?;

        let mut lora_valid_poc_sink = file_sink::FileSinkBuilder::new(
            FileType::LoraValidPoc,
            store_base_path,
            lora_valid_poc_rx,
        )
        .deposits(Some(file_upload_tx.clone()))
        .roll_time(ChronoDuration::minutes(15))
        .create()
        .await?;

        // spawn off the file sinks
        // TODO: how to avoid all da cloning?
        let shutdown2 = shutdown.clone();
        let shutdown3 = shutdown.clone();
        let shutdown4 = shutdown.clone();
        let shutdown5 = shutdown.clone();
        tokio::spawn(async move { lora_invalid_beacon_sink.run(&shutdown2).await });
        tokio::spawn(async move { lora_invalid_witness_sink.run(&shutdown3).await });
        tokio::spawn(async move { lora_valid_poc_sink.run(&shutdown4).await });
        tokio::spawn(async move { file_upload.run(&shutdown5).await });

        loop {
            if shutdown.is_triggered() {
                break;
            }
            tokio::select! {
                _ = shutdown.clone() => break,
                _ = db_timer.tick() =>
                    match self.handle_db_tick(  shutdown.clone(),
                                                lora_invalid_beacon_tx.clone(),
                                                lora_invalid_witness_tx.clone(),
                                                lora_valid_poc_tx.clone()).await {
                    Ok(()) => (),
                    Err(err) => {
                        tracing::error!("fatal db runner error: {err:?}");
                        return Err(err)
                    }
                }
            }
        }
        tracing::info!("stopping runner");
        Ok(())
    }

    async fn handle_db_tick(
        &self,
        _shutdown: triggered::Listener,
        lora_invalid_beacon_tx: MessageSender,
        lora_invalid_witness_tx: MessageSender,
        lora_valid_poc_tx: MessageSender,
    ) -> Result {
        let db_beacon_reports = Report::get_next_beacons(&self.pool).await?;
        if db_beacon_reports.is_empty() {
            tracing::info!("no beacons ready for verification");
            return Ok(());
        }
        // iterate over the beacons pulled from the db
        // for each get witnesses from the DB
        // if a beacon is invalid, then ev thing is invalid
        // but a beacon could be valid whilst witnesses
        // can be a mix of both valid and invalid
        let beacon_len = db_beacon_reports.len();
        tracing::info!("{beacon_len} beacons ready for verification");

        stream::iter(db_beacon_reports)
            .for_each_concurrent(BEACON_WORKERS, |db_beacon| {
                let tx1 = lora_invalid_beacon_tx.clone();
                let tx2 = lora_invalid_witness_tx.clone();
                let tx3 = lora_valid_poc_tx.clone();
                async move {
                    match self.handle_beacon_report(db_beacon, tx1, tx2, tx3).await {
                        Ok(()) => (),
                        Err(err) => {
                            tracing::warn!("failed to handle beacon: {err:?}")
                        }
                    }
                }
            })
            .await;
        metrics::gauge!("oracles_poc_iot_verifier_beacons_ready", beacon_len as f64);
        tracing::info!("completed processing {beacon_len} beacons");
        Ok(())
    }

    async fn handle_beacon_report(
        &self,
        db_beacon: Report,
        lora_invalid_beacon_tx: MessageSender,
        lora_invalid_witness_tx: MessageSender,
        lora_valid_poc_tx: MessageSender,
    ) -> Result {
        let entropy_start_time = match db_beacon.timestamp {
            Some(v) => v,
            None => return Ok(()),
        };
        let packet_data = &db_beacon.packet_data;

        let beacon_buf: &[u8] = &db_beacon.report_data;
        let beacon_report: LoraBeaconIngestReport =
            LoraBeaconIngestReportV1::decode(beacon_buf)?.try_into()?;
        let beacon = &beacon_report.report;
        let beacon_received_ts = beacon_report.received_timestamp;

        let db_witnesses = Report::get_witnesses_for_beacon(&self.pool, packet_data).await?;
        let witness_len = db_witnesses.len();
        tracing::debug!("found {witness_len} witness for beacon");
        metrics::gauge!(
            "oracles_poc_iot_verifier_witnesses_per_beacon",
            witness_len as f64
        );

        // get the beacon and witness report PBs from the db reports
        let mut witnesses: Vec<LoraWitnessIngestReport> = Vec::new();
        for db_witness in db_witnesses {
            let witness_buf: &[u8] = &db_witness.report_data;
            witnesses.push(LoraWitnessIngestReportV1::decode(witness_buf)?.try_into()?);
        }

        // create the struct defining this POC
        let mut poc = Poc::new(
            beacon_report.clone(),
            witnesses.clone(),
            entropy_start_time,
            self.follower_service.clone(),
            self.pool.clone(),
        )
        .await?;

        let density_queries = match &self.density_queries {
            Some(density_queries) => density_queries.clone(),
            None => return Err(Error::custom("missing density scaler query sender")),
        };
        // verify POC beacon
        let beacon_verify_result = poc.verify_beacon(density_queries.clone()).await?;
        match beacon_verify_result.result {
            VerificationStatus::Valid => {
                tracing::debug!(
                    "valid beacon. entropy: {:?}, addr: {:?}",
                    beacon.data,
                    beacon.pub_key.to_string()
                );
                // beacon is valid, verify the POC witnesses
                if let Some(beacon_info) = beacon_verify_result.gateway_info {
                    let verified_witnesses_result =
                        poc.verify_witnesses(&beacon_info, density_queries).await?;
                    // check if there are any failed witnesses
                    // if so update the DB attempts count
                    // and halt here, let things be reprocessed next tick
                    // if a witness continues to fail it will eventually
                    // be discarded from the list returned for the beacon
                    // thus one of more failing witnesses will not block the overall POC
                    if !verified_witnesses_result.failed_witnesses.is_empty() {
                        for failed_witness_report in verified_witnesses_result.failed_witnesses {
                            // something went wrong whilst verifying witnesses
                            // halt here and allow things to be reprocessed next tick
                            let failed_witness = failed_witness_report.report;
                            // have to construct the id manually here as dont have the ingest report handy
                            let id =
                                failed_witness.report_id(failed_witness_report.received_timestamp);
                            Report::update_attempts(&self.pool, &id, Utc::now()).await?;
                        }
                        return Ok(());
                    };

                    let valid_beacon_report = LoraValidBeaconReport {
                        received_timestamp: beacon_received_ts,
                        location: beacon_info.location,
                        hex_scale: beacon_verify_result.hex_scale.unwrap_or_default(),
                        report: beacon.clone(),
                    };
                    self.handle_valid_poc(
                        beacon,
                        valid_beacon_report,
                        verified_witnesses_result,
                        &lora_valid_poc_tx,
                        &lora_invalid_witness_tx,
                    )
                    .await?;
                }
            }
            VerificationStatus::Invalid => {
                // the beacon is invalid, which in turn renders all witnesses invalid
                let invalid_reason = beacon_verify_result
                    .invalid_reason
                    .ok_or_else(|| Error::not_found("invalid invalid_reason for beacon"))?;
                tracing::info!(
                    "invalid beacon. entropy: {:?}, addr: {:?}, reason: {:?}",
                    beacon.data,
                    beacon.pub_key.to_string(),
                    invalid_reason
                );
                self.handle_invalid_poc(
                    &beacon_report,
                    witnesses,
                    invalid_reason,
                    &lora_invalid_beacon_tx,
                    &lora_invalid_witness_tx,
                )
                .await?;
            }
            VerificationStatus::Failed => {
                // something went wrong whilst verifying the beacon report
                // halt here and allow things to be reprocessed next tick
                tracing::info!(
                    "failed beacon. entropy: {:?}, addr: {:?}",
                    beacon.data,
                    beacon.pub_key.to_string()
                );
                Report::update_attempts(&self.pool, &beacon_report.ingest_id(), Utc::now()).await?;
            }
        }
        Ok(())
    }

    async fn handle_invalid_poc(
        &self,
        beacon_report: &LoraBeaconIngestReport,
        witness_reports: Vec<LoraWitnessIngestReport>,
        invalid_reason: InvalidReason,
        lora_invalid_beacon_tx: &MessageSender,
        lora_invalid_witness_tx: &MessageSender,
    ) -> Result {
        // the beacon is invalid, which in turn renders all witnesses invalid
        let beacon = &beacon_report.report;
        let beacon_id = beacon.data.clone();
        let beacon_report_id = beacon_report.ingest_id();
        let invalid_poc: LoraInvalidBeaconReport = LoraInvalidBeaconReport {
            received_timestamp: beacon_report.received_timestamp,
            reason: invalid_reason,
            report: beacon.clone(),
        };
        let invalid_poc_proto: LoraInvalidBeaconReportV1 = invalid_poc.into();
        // save invalid poc to s3, if write fails update attempts and go no further
        // allow the poc to be reprocessed next tick
        match file_sink_write!(
            "invalid_beacon_report",
            lora_invalid_beacon_tx,
            invalid_poc_proto
        )
        .await
        {
            Ok(_) => (),
            Err(err) => {
                tracing::error!("failed to save invalid_poc to s3, {err}");
                Report::update_attempts(&self.pool, &beacon_report_id, Utc::now()).await?;
                return Ok(());
            }
        }
        // save invalid witnesses to s3, ignore any failed witness writes
        // taking the lossly approach here as if we re attempt the POC later
        // we will have to clean out any sucessful writes of other witnesses
        // and also the invalid poc
        // so if a report fails from this point on, it shall be lost for ever more
        for witness_report in witness_reports {
            let invalid_witness_report: LoraInvalidWitnessReport = LoraInvalidWitnessReport {
                received_timestamp: witness_report.received_timestamp,
                report: witness_report.report,
                reason: invalid_reason,
                participant_side: InvalidParticipantSide::Beaconer,
            };
            let invalid_witness_report_proto: LoraInvalidWitnessReportV1 =
                invalid_witness_report.into();
            match file_sink_write!(
                "invalid_witness_report",
                lora_invalid_witness_tx,
                invalid_witness_report_proto
            )
            .await
            {
                Ok(_) => (),
                Err(err) => {
                    tracing::error!("ignoring failed s3 write of invalid_witness_report: {err}");
                }
            }
        }
        // done with these poc reports, purge em from the db
        Report::delete_poc(&self.pool, &beacon_id).await?;
        Ok(())
    }

    async fn handle_valid_poc(
        &self,
        beacon: &LoraBeaconReport,
        valid_beacon_report: LoraValidBeaconReport,
        witnesses_result: VerifyWitnessesResult,
        lora_valid_poc_tx: &MessageSender,
        lora_invalid_witness_tx: &MessageSender,
    ) -> Result {
        let received_timestamp = valid_beacon_report.received_timestamp;
        let beacon_id = valid_beacon_report.report.report_id(received_timestamp);
        let packet_data = valid_beacon_report.report.data.clone();
        let beacon_report_id = valid_beacon_report.report.report_id(received_timestamp);
        let valid_poc: LoraValidPoc = LoraValidPoc {
            poc_id: beacon_id.clone(),
            beacon_report: valid_beacon_report,
            witness_reports: witnesses_result.valid_witnesses.clone(),
        };
        let valid_poc_proto: LoraValidPocV1 = valid_poc.into();
        // save the poc to s3, if write fails update attempts and go no further
        // allow the poc to be reprocessed next tick
        match file_sink_write!("valid_poc", lora_valid_poc_tx, valid_poc_proto).await {
            Ok(_) => (),
            Err(err) => {
                tracing::error!("failed to save invalid_witness_report to s3, {err}");
                Report::update_attempts(&self.pool, &beacon_report_id, Utc::now()).await?;
                return Ok(());
            }
        }
        // // valid beacons and witness reports are kept in the DB
        // // until after they have been rewarded
        // // for now we just have to update their status to valid
        // Report::update_status(&self.pool, &beacon_id, LoraStatus::Valid, Utc::now()).await?;
        // // update timestamp of last beacon for the beaconer
        // LastBeacon::update_last_timestamp(&self.pool, &beacon.pub_key.to_vec(), received_timestamp)
        //     .await?;

        // // update DB status of valid witnesses
        // for valid_witness_report in witnesses_result.valid_witnesses {
        //     let valid_witness_id = valid_witness_report
        //         .report
        //         .report_id(valid_witness_report.received_timestamp);
        //     Report::update_status(&self.pool, &valid_witness_id, LoraStatus::Valid, Utc::now())
        //         .await?
        // }

        // save invalid witnesses to s3, ignore any failed witness writes
        // taking the lossly approach here as if we re attempt the POC later
        // we will have to clean out any sucessful write of the valid poc above
        // so if a report fails from this point on, it shall be lost for ever more
        for invalid_witness_report in witnesses_result.invalid_witnesses {
            let invalid_witness_report_proto: LoraInvalidWitnessReportV1 =
                invalid_witness_report.clone().into();
            match file_sink_write!(
                "invalid_witness_report",
                lora_invalid_witness_tx,
                invalid_witness_report_proto
            )
            .await
            {
                Ok(_) => {
                    // // we dont need to reward invalid witness reports
                    // // so we can go ahead & delete from the DB
                    // let report_id = invalid_witness_report
                    //     .report
                    //     .report_id(invalid_witness_report.received_timestamp);
                    // _ = Report::delete_report(&self.pool, &report_id).await;

                }
                Err(err) => {
                    tracing::error!("failed to save invalid_witness_report to s3, {err}");
                }
            }
        }
        _ = Report::delete_poc(&self.pool, &packet_data).await;
        Ok(())
    }
}
