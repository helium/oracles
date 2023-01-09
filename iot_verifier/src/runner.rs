use crate::{
    gateway_cache::GatewayCache,
    last_beacon::LastBeacon,
    poc::{Poc, VerificationStatus, VerifyWitnessesResult},
    poc_report::Report,
    reward_share::GatewayShare,
    Settings,
};
use chrono::{Duration as ChronoDuration, Utc};
use density_scaler::{HexDensityMap, SCALING_PRECISION};
use file_store::{
    file_sink,
    file_sink::MessageSender,
    file_sink_write,
    file_upload::MessageSender as FileUploadSender,
    lora_beacon_report::LoraBeaconIngestReport,
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
use rust_decimal::{Decimal, MathematicalOps};
use rust_decimal_macros::dec;
use sqlx::PgPool;
use std::path::Path;
use tokio::time::{self, MissedTickBehavior};

/// the cadence in seconds at which the DB is polled for ready POCs
const DB_POLL_TIME: time::Duration = time::Duration::from_secs(30);
const BEACON_WORKERS: usize = 100;
const RUNNER_DB_POOL_SIZE: usize = BEACON_WORKERS * 4;

const WITNESS_REDUNDANCY: u32 = 4;
const POC_REWARD_DECAY_RATE: Decimal = dec!(0.8);
const HIP15_TX_REWARD_UNIT_CAP: Decimal = Decimal::TWO;

pub struct Runner {
    pool: PgPool,
    settings: Settings,
}

#[derive(thiserror::Error, Debug)]
#[error("error creating runner: {0}")]
pub struct NewRunnerError(#[from] db_store::Error);

#[derive(thiserror::Error, Debug)]
pub enum RunnerError {
    #[error("not found: {0}")]
    NotFound(&'static str),
}

impl Runner {
    pub async fn from_settings(settings: &Settings) -> Result<Self, NewRunnerError> {
        let pool = settings.database.connect(RUNNER_DB_POOL_SIZE).await?;
        Ok(Self {
            pool,
            settings: settings.clone(),
        })
    }

    pub async fn run(
        &mut self,
        file_upload_tx: FileUploadSender,
        gateway_cache: &GatewayCache,
        hex_density_map: impl HexDensityMap,
        shutdown: &triggered::Listener,
    ) -> anyhow::Result<()> {
        tracing::info!("starting runner");

        let mut db_timer = time::interval(DB_POLL_TIME);
        db_timer.set_missed_tick_behavior(MissedTickBehavior::Skip);

        let store_base_path = Path::new(&self.settings.cache);
        let (lora_invalid_beacon_tx, lora_invalid_beacon_rx) = file_sink::message_channel(50);
        let (lora_invalid_witness_tx, lora_invalid_witness_rx) = file_sink::message_channel(50);
        let (lora_valid_poc_tx, lora_valid_poc_rx) = file_sink::message_channel(50);

        let mut lora_invalid_beacon_sink = file_sink::FileSinkBuilder::new(
            FileType::LoraInvalidBeaconReport,
            store_base_path,
            lora_invalid_beacon_rx,
        )
        .deposits(Some(file_upload_tx.clone()))
        .roll_time(ChronoDuration::minutes(5))
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
        .roll_time(ChronoDuration::minutes(2))
        .create()
        .await?;

        // spawn off the file sinks
        // TODO: how to avoid all da cloning?
        let shutdown2 = shutdown.clone();
        let shutdown3 = shutdown.clone();
        let shutdown4 = shutdown.clone();
        tokio::spawn(async move { lora_invalid_beacon_sink.run(&shutdown2).await });
        tokio::spawn(async move { lora_invalid_witness_sink.run(&shutdown3).await });
        tokio::spawn(async move { lora_valid_poc_sink.run(&shutdown4).await });

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
                                                lora_valid_poc_tx.clone(),
                                                gateway_cache, hex_density_map.clone()).await {
                    Ok(()) => (),
                    Err(err) => {
                        tracing::error!("fatal db runner error: {err:?}");
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
        gateway_cache: &GatewayCache,
        hex_density_map: impl HexDensityMap,
    ) -> anyhow::Result<()> {
        tracing::info!("starting query get_next_beacons");
        let db_beacon_reports = Report::get_next_beacons(&self.pool).await?;
        tracing::info!("completed query get_next_beacons");
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
                let hdm = hex_density_map.clone();
                async move {
                    match self
                        .handle_beacon_report(db_beacon, tx1, tx2, tx3, gateway_cache, hdm)
                        .await
                    {
                        Ok(()) => (),
                        Err(err) => {
                            tracing::warn!("failed to handle beacon: {err:?}")
                        }
                    }
                }
            })
            .await;
        metrics::gauge!("oracles_iot_verifier_beacons_ready", beacon_len as f64);
        tracing::info!("completed processing {beacon_len} beacons");
        Ok(())
    }

    async fn handle_beacon_report(
        &self,
        db_beacon: Report,
        lora_invalid_beacon_tx: MessageSender,
        lora_invalid_witness_tx: MessageSender,
        lora_valid_poc_tx: MessageSender,
        gateway_cache: &GatewayCache,
        hex_density_map: impl HexDensityMap,
    ) -> anyhow::Result<()> {
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
            "oracles_iot_verifier_witnesses_per_beacon",
            witness_len as f64
        );

        // get the beacon and witness report PBs from the db reports
        let mut witnesses: Vec<LoraWitnessIngestReport> = Vec::new();
        for db_witness in db_witnesses {
            let witness_buf: &[u8] = &db_witness.report_data;
            witnesses.push(LoraWitnessIngestReportV1::decode(witness_buf)?.try_into()?);
        }

        // create the struct defining this POC
        let mut poc = Poc::new(beacon_report.clone(), witnesses.clone(), entropy_start_time).await;

        // verify POC beacon
        let beacon_verify_result = poc
            .verify_beacon(hex_density_map.clone(), gateway_cache, &self.pool)
            .await?;
        match beacon_verify_result.result {
            VerificationStatus::Valid => {
                tracing::debug!(
                    "valid beacon. entropy: {:?}, addr: {:?}",
                    beacon.data,
                    beacon.pub_key
                );
                // beacon is valid, verify the POC witnesses
                if let Some(beacon_info) = beacon_verify_result.gateway_info {
                    let mut verified_witnesses_result = poc
                        .verify_witnesses(&beacon_info, hex_density_map, gateway_cache)
                        .await?;
                    // check if there are any failed witnesses
                    // if so update the DB attempts count
                    // and halt here, let things be reprocessed next tick
                    // if a witness continues to fail it will eventually
                    // be discarded from the list returned for the beacon
                    // thus one or more failing witnesses will not block the overall POC
                    if !verified_witnesses_result.failed_witnesses.is_empty() {
                        for failed_witness_report in verified_witnesses_result.failed_witnesses {
                            let failed_witness = failed_witness_report.report;
                            let id =
                                failed_witness.report_id(failed_witness_report.received_timestamp);
                            Report::update_attempts(&self.pool, &id, Utc::now()).await?;
                        }
                        return Ok(());
                    };

                    let witness_reward_units = poc_challengee_reward_unit(
                        verified_witnesses_result.valid_witnesses.len() as u32,
                    )?;

                    verified_witnesses_result.update_reward_units(witness_reward_units);

                    let valid_beacon_report = LoraValidBeaconReport {
                        received_timestamp: beacon_received_ts,
                        location: beacon_info.location,
                        hex_scale: beacon_verify_result
                            .hex_scale
                            .ok_or(RunnerError::NotFound("invalid hex scaling factor"))?,
                        report: beacon.clone(),
                        reward_unit: witness_reward_units,
                    };
                    self.handle_valid_poc(
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
                let Some(invalid_reason) = beacon_verify_result
                    .invalid_reason else {
                        anyhow::bail!("invalid_reason is None");
                    };
                tracing::debug!(
                    "invalid beacon. entropy: {:?}, addr: {:?}, reason: {:?}",
                    beacon.data,
                    beacon.pub_key,
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
                    beacon.pub_key
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
    ) -> anyhow::Result<()> {
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
            invalid_poc_proto,
            vec![("reason", invalid_reason.as_str_name())]
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
                invalid_witness_report_proto,
                vec![("reason", invalid_reason.as_str_name())]
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
        valid_beacon_report: LoraValidBeaconReport,
        witnesses_result: VerifyWitnessesResult,
        lora_valid_poc_tx: &MessageSender,
        lora_invalid_witness_tx: &MessageSender,
    ) -> anyhow::Result<()> {
        let received_timestamp = valid_beacon_report.received_timestamp;
        let pub_key = valid_beacon_report.report.pub_key.clone();
        let beacon_id = valid_beacon_report.report.report_id(received_timestamp);
        let packet_data = valid_beacon_report.report.data.clone();
        let beacon_report_id = valid_beacon_report.report.report_id(received_timestamp);
        let valid_poc: LoraValidPoc = LoraValidPoc {
            poc_id: beacon_id,
            beacon_report: valid_beacon_report,
            witness_reports: witnesses_result.valid_witnesses,
        };

        let mut transaction = self.pool.begin().await?;
        for reward_share in GatewayShare::shares_from_poc(&valid_poc) {
            reward_share.save(&mut transaction).await?;
        }
        // TODO: expand this transaction to cover all of the database access below?
        transaction.commit().await?;

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

        // save invalid witnesses to s3, ignore any failed witness writes
        // taking the lossly approach here as if we re attempt the POC later
        // we will have to clean out any sucessful write of the valid poc above
        // so if a report fails from this point on, it shall be lost for ever more
        for invalid_witness_report in witnesses_result.invalid_witnesses {
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
                    tracing::error!("failed to save invalid_witness_report to s3, {err}");
                }
            }
        }
        // update timestamp of last beacon for the beaconer
        LastBeacon::update_last_timestamp(&self.pool, pub_key.as_ref(), received_timestamp).await?;
        Report::delete_poc(&self.pool, &packet_data).await?;
        Ok(())
    }
}

fn poc_challengee_reward_unit(num_witnesses: u32) -> anyhow::Result<Decimal> {
    let reward_units = if num_witnesses == 0 {
        Decimal::ZERO
    } else if num_witnesses < WITNESS_REDUNDANCY {
        Decimal::from(WITNESS_REDUNDANCY / num_witnesses)
    } else {
        let exp = num_witnesses - WITNESS_REDUNDANCY;
        if let Some(to_sub) = POC_REWARD_DECAY_RATE.checked_powu(exp as u64) {
            let unnormalized = Decimal::TWO - to_sub;
            std::cmp::min(HIP15_TX_REWARD_UNIT_CAP, unnormalized)
        } else {
            anyhow::bail!("invalid exponent: {}", exp);
        }
    };
    Ok(reward_units.round_dp(SCALING_PRECISION))
}
