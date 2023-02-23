use crate::{
    gateway_cache::GatewayCache, hex_density::HexDensityMap, last_beacon::LastBeacon,
    metrics::Metrics, poc::Poc, poc_report::Report, reward_share::GatewayShare, Settings,
};
use chrono::{Duration as ChronoDuration, Utc};
use file_store::{
    file_sink,
    file_sink::FileSinkClient,
    file_upload::MessageSender as FileUploadSender,
    iot_beacon_report::IotBeaconIngestReport,
    iot_invalid_poc::{IotInvalidBeaconReport, IotInvalidWitnessReport},
    iot_valid_poc::{IotPoc, IotValidBeaconReport, IotVerifiedWitnessReport},
    iot_witness_report::IotWitnessIngestReport,
    traits::{IngestId, MsgDecode, ReportId},
    FileType, SCALING_PRECISION,
};
use futures::stream::{self, StreamExt};
use helium_proto::services::poc_lora::{
    InvalidParticipantSide, InvalidReason, LoraInvalidBeaconReportV1, LoraInvalidWitnessReportV1,
    LoraPocV1, VerificationStatus,
};
use rand::{rngs::StdRng, seq::SliceRandom, SeedableRng};
use rust_decimal::{Decimal, MathematicalOps};
use rust_decimal_macros::dec;
use sha2::{Digest, Sha256};
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
    beacon_interval: ChronoDuration,
    beacon_interval_tolerance: ChronoDuration,
}

#[derive(thiserror::Error, Debug)]
#[error("error creating runner: {0}")]
pub struct NewRunnerError(#[from] db_store::Error);

#[derive(thiserror::Error, Debug)]
pub enum RunnerError {
    #[error("not found: {0}")]
    NotFound(&'static str),
}

pub enum FilterStatus {
    Drop,
    Exclude,
    Include,
}
impl Runner {
    pub async fn from_settings(settings: &Settings) -> Result<Self, NewRunnerError> {
        let pool = settings.database.connect(RUNNER_DB_POOL_SIZE).await?;
        let beacon_interval = settings.beacon_interval();
        let beacon_interval_tolerance = settings.beacon_interval_tolerance();
        Ok(Self {
            pool,
            settings: settings.clone(),
            beacon_interval,
            beacon_interval_tolerance,
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

        let (iot_invalid_beacon_sink, mut iot_invalid_beacon_sink_server) =
            file_sink::FileSinkBuilder::new(
                FileType::IotInvalidBeaconReport,
                store_base_path,
                concat!(env!("CARGO_PKG_NAME"), "_invalid_beacon_report"),
            )
            .deposits(Some(file_upload_tx.clone()))
            .roll_time(ChronoDuration::minutes(5))
            .create()
            .await?;

        let (iot_invalid_witness_sink, mut iot_invalid_witness_sink_server) =
            file_sink::FileSinkBuilder::new(
                FileType::IotInvalidWitnessReport,
                store_base_path,
                concat!(env!("CARGO_PKG_NAME"), "_invalid_witness_report"),
            )
            .deposits(Some(file_upload_tx.clone()))
            .roll_time(ChronoDuration::minutes(5))
            .create()
            .await?;

        let (iot_poc_sink, mut iot_poc_sink_server) = file_sink::FileSinkBuilder::new(
            FileType::IotPoc,
            store_base_path,
            concat!(env!("CARGO_PKG_NAME"), "_valid_poc"),
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
        tokio::spawn(async move { iot_invalid_beacon_sink_server.run(&shutdown2).await });
        tokio::spawn(async move { iot_invalid_witness_sink_server.run(&shutdown3).await });
        tokio::spawn(async move { iot_poc_sink_server.run(&shutdown4).await });

        loop {
            if shutdown.is_triggered() {
                break;
            }
            tokio::select! {
                _ = shutdown.clone() => break,
                _ = db_timer.tick() =>
                    match self.handle_db_tick(  shutdown.clone(),
                                                &iot_invalid_beacon_sink,
                                                &iot_invalid_witness_sink,
                                                &iot_poc_sink,
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
        iot_invalid_beacon_sink: &FileSinkClient,
        iot_invalid_witness_sink: &FileSinkClient,
        iot_poc_sink: &FileSinkClient,
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
                let hdm = hex_density_map.clone();
                async move {
                    let beacon_id = db_beacon.id.clone();
                    match self
                        .handle_beacon_report(
                            db_beacon,
                            iot_invalid_beacon_sink,
                            iot_invalid_witness_sink,
                            iot_poc_sink,
                            gateway_cache,
                            hdm,
                        )
                        .await
                    {
                        Ok(()) => (),
                        Err(err) => {
                            tracing::warn!("failed to handle beacon: {err:?}");
                            _ = Report::update_attempts(&self.pool, &beacon_id, Utc::now()).await;
                        }
                    }
                }
            })
            .await;
        tracing::info!("completed processing {beacon_len} beacons");
        Ok(())
    }

    async fn handle_beacon_report(
        &self,
        db_beacon: Report,
        iot_invalid_beacon_sink: &FileSinkClient,
        iot_invalid_witness_sink: &FileSinkClient,
        iot_poc_sink: &FileSinkClient,
        gateway_cache: &GatewayCache,
        hex_density_map: impl HexDensityMap,
    ) -> anyhow::Result<()> {
        let entropy_start_time = match db_beacon.timestamp {
            Some(v) => v,
            None => return Ok(()),
        };
        let entropy_version = match db_beacon.version {
            Some(v) => v,
            None => return Ok(()),
        };
        let packet_data = &db_beacon.packet_data;

        let beacon_buf: &[u8] = &db_beacon.report_data;
        let beacon_report = IotBeaconIngestReport::decode(beacon_buf)?;
        let beacon = &beacon_report.report;
        let beacon_received_ts = beacon_report.received_timestamp;

        let db_witnesses = Report::get_witnesses_for_beacon(&self.pool, packet_data).await?;
        let witness_len = db_witnesses.len();
        tracing::debug!("found {witness_len} witness for beacon");

        // get the beacon and witness report PBs from the db reports
        let mut witnesses: Vec<IotWitnessIngestReport> = Vec::new();
        for db_witness in db_witnesses {
            let witness_buf: &[u8] = &db_witness.report_data;
            witnesses.push(IotWitnessIngestReport::decode(witness_buf)?);
        }

        // create the struct defining this POC
        let mut poc = Poc::new(
            beacon_report.clone(),
            witnesses.clone(),
            entropy_start_time,
            entropy_version,
        )
        .await;

        // verify POC beacon
        let beacon_verify_result = poc
            .verify_beacon(
                hex_density_map.clone(),
                gateway_cache,
                &self.pool,
                self.beacon_interval,
                self.beacon_interval_tolerance,
            )
            .await?;
        match beacon_verify_result.result {
            VerificationStatus::Valid => {
                // beacon is valid, verify the POC witnesses
                if let Some(beacon_info) = beacon_verify_result.gateway_info {
                    let verified_witnesses_result = poc
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

                    let max_witnesses_per_poc = self.settings.max_witnesses_per_poc as usize;
                    let beacon_id = beacon.report_id(beacon_received_ts);

                    // filter out self witnesses
                    // & partition remaining witnesses into exclude and include sets
                    // 'drop' items are dropped to the floor, never make it to s3
                    // 'exclude' items are not permitted in last 14 but
                    // will make it into the unselected list and thus will goto s3
                    // 'include' items are from where the last 14 are selected
                    // any witness with include status which doesnt make it to the last 14
                    // will join the excluded items in the unselected list and thus will goto s3
                    let (excluded_witnesses, mut selected_witnesses) =
                        filter_witnesses(verified_witnesses_result.verified_witnesses);

                    // split our verified witness list up into selected and unselected items
                    let mut unselected_witnesses = shuffle_and_split_witnesses(
                        &beacon_id,
                        &mut selected_witnesses,
                        max_witnesses_per_poc,
                    )?;
                    // concat the unselected witnesses and the previously excluded witnesses
                    // these will then form the unseleted list on the poc
                    unselected_witnesses =
                        [&unselected_witnesses[..], &excluded_witnesses[..]].concat();

                    // get the number of valid witnesses in our selected list
                    let num_valid_selected_witnesses = selected_witnesses
                        .iter()
                        .filter(|witness| witness.status == VerificationStatus::Valid)
                        .count();

                    // get reward units based on the count of valid selected witnesses
                    let beaconer_reward_units =
                        poc_beaconer_reward_unit(num_valid_selected_witnesses as u32)?;
                    let witness_reward_units =
                        poc_per_witness_reward_unit(num_valid_selected_witnesses as u32)?;
                    // update the reward units for those valid witnesses within our selected list
                    selected_witnesses
                        .iter_mut()
                        .for_each(|witness| match witness.status {
                            VerificationStatus::Valid => witness.reward_unit = witness_reward_units,
                            VerificationStatus::Invalid => witness.reward_unit = Decimal::ZERO,
                        });

                    let valid_beacon_report = IotValidBeaconReport {
                        received_timestamp: beacon_received_ts,
                        location: beacon_info.location,
                        hex_scale: beacon_verify_result
                            .hex_scale
                            .ok_or(RunnerError::NotFound("invalid hex scaling factor"))?,
                        report: beacon.clone(),
                        reward_unit: beaconer_reward_units,
                    };
                    self.handle_valid_poc(
                        valid_beacon_report,
                        selected_witnesses,
                        unselected_witnesses,
                        iot_poc_sink,
                    )
                    .await?;
                }
            }
            VerificationStatus::Invalid => {
                // the beacon is invalid, which in turn renders all witnesses invalid
                self.handle_invalid_poc(
                    &beacon_report,
                    witnesses,
                    beacon_verify_result.invalid_reason,
                    iot_invalid_beacon_sink,
                    iot_invalid_witness_sink,
                )
                .await?;
            }
        }
        Ok(())
    }

    async fn handle_invalid_poc(
        &self,
        beacon_report: &IotBeaconIngestReport,
        witness_reports: Vec<IotWitnessIngestReport>,
        invalid_reason: InvalidReason,
        iot_invalid_beacon_sink: &FileSinkClient,
        iot_invalid_witness_sink: &FileSinkClient,
    ) -> anyhow::Result<()> {
        // the beacon is invalid, which in turn renders all witnesses invalid
        let beacon = &beacon_report.report;
        let beacon_id = beacon.data.clone();
        let beacon_report_id = beacon_report.ingest_id();
        let invalid_poc: IotInvalidBeaconReport = IotInvalidBeaconReport {
            received_timestamp: beacon_report.received_timestamp,
            reason: invalid_reason,
            report: beacon.clone(),
        };
        let invalid_poc_proto: LoraInvalidBeaconReportV1 = invalid_poc.into();
        // save invalid poc to s3, if write fails update attempts and go no further
        // allow the poc to be reprocessed next tick
        match iot_invalid_beacon_sink
            .write(
                invalid_poc_proto,
                &[("reason", invalid_reason.as_str_name())],
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
            let invalid_witness_report: IotInvalidWitnessReport = IotInvalidWitnessReport {
                received_timestamp: witness_report.received_timestamp,
                report: witness_report.report,
                reason: invalid_reason,
                participant_side: InvalidParticipantSide::Beaconer,
            };
            let invalid_witness_report_proto: LoraInvalidWitnessReportV1 =
                invalid_witness_report.into();
            match iot_invalid_witness_sink
                .write(
                    invalid_witness_report_proto,
                    &[("reason", invalid_reason.as_str_name())],
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
        Metrics::decrement_num_beacons();
        Ok(())
    }

    async fn handle_valid_poc(
        &self,
        valid_beacon_report: IotValidBeaconReport,
        selected_witnesses: Vec<IotVerifiedWitnessReport>,
        unselected_witnesses: Vec<IotVerifiedWitnessReport>,
        iot_poc_sink: &FileSinkClient,
    ) -> anyhow::Result<()> {
        let received_timestamp = valid_beacon_report.received_timestamp;
        let pub_key = valid_beacon_report.report.pub_key.clone();
        let beacon_id = valid_beacon_report.report.report_id(received_timestamp);
        let packet_data = valid_beacon_report.report.data.clone();
        let beacon_report_id = valid_beacon_report.report.report_id(received_timestamp);
        let iot_poc: IotPoc = IotPoc {
            poc_id: beacon_id,
            beacon_report: valid_beacon_report,
            selected_witnesses: selected_witnesses.clone(),
            unselected_witnesses: unselected_witnesses.clone(),
        };

        let mut transaction = self.pool.begin().await?;
        for reward_share in GatewayShare::shares_from_poc(&iot_poc) {
            reward_share.save(&mut transaction).await?;
        }
        // TODO: expand this transaction to cover all of the database access below?
        transaction.commit().await?;

        let poc_proto: LoraPocV1 = iot_poc.into();
        // save the poc to s3, if write fails update attempts and go no further
        // allow the poc to be reprocessed next tick
        match iot_poc_sink.write(poc_proto, []).await {
            Ok(_) => (),
            Err(err) => {
                tracing::error!("failed to save invalid_witness_report to s3, {err}");
                Report::update_attempts(&self.pool, &beacon_report_id, Utc::now()).await?;
                return Ok(());
            }
        }
        // write out metrics for any witness which failed verification
        // TODO: work our approach that doesnt require the prior cloning of
        // the selected and unselected witnesses vecs
        // tried to do this directly from the now discarded poc_proto
        // but could nae get it to get a way past the lack of COPY
        fire_invalid_witness_metric(&selected_witnesses);
        fire_invalid_witness_metric(&unselected_witnesses);
        // update timestamp of last beacon for the beaconer
        LastBeacon::update_last_timestamp(&self.pool, pub_key.as_ref(), received_timestamp).await?;
        Report::delete_poc(&self.pool, &packet_data).await?;
        Metrics::decrement_num_beacons();
        Ok(())
    }
}

fn poc_beaconer_reward_unit(num_witnesses: u32) -> anyhow::Result<Decimal> {
    let reward_units = if num_witnesses == 0 {
        Decimal::ZERO
    } else if num_witnesses <= WITNESS_REDUNDANCY {
        if let Some(sub_redundancy_units) =
            Decimal::from_f32_retain(num_witnesses as f32 / WITNESS_REDUNDANCY as f32)
        {
            sub_redundancy_units
        } else {
            anyhow::bail!("invalid fractional division: {num_witnesses} / {WITNESS_REDUNDANCY}");
        }
    } else {
        let exp = num_witnesses - WITNESS_REDUNDANCY;
        if let Some(to_sub) = POC_REWARD_DECAY_RATE.checked_powu(exp as u64) {
            let unnormalized = Decimal::TWO - to_sub;
            std::cmp::min(HIP15_TX_REWARD_UNIT_CAP, unnormalized)
        } else {
            anyhow::bail!("invalid exponent: {exp}");
        }
    };
    Ok(reward_units.round_dp(SCALING_PRECISION))
}

fn poc_per_witness_reward_unit(num_witnesses: u32) -> anyhow::Result<Decimal> {
    let reward_units = if num_witnesses == 0 {
        Decimal::ZERO
    } else if num_witnesses <= WITNESS_REDUNDANCY {
        Decimal::ONE
    } else {
        let exp = num_witnesses - WITNESS_REDUNDANCY;
        if let Some(to_sub) = POC_REWARD_DECAY_RATE.checked_powu(exp as u64) {
            let unnormalized = (Decimal::from(WITNESS_REDUNDANCY) - (Decimal::ONE - to_sub))
                / Decimal::from(num_witnesses);
            std::cmp::min(HIP15_TX_REWARD_UNIT_CAP, unnormalized)
        } else {
            anyhow::bail!("invalid exponent: {exp}");
        }
    };
    Ok(reward_units.round_dp(SCALING_PRECISION))
}

/// maybe shuffle & split the verified witness list
// if the number of witnesses exceeds our cap
// split the list into two
// one representing selected witnesses
// the other representing unselected witnesses
fn shuffle_and_split_witnesses(
    poc_id: &[u8],
    witnesses: &mut Vec<IotVerifiedWitnessReport>,
    max_count: usize,
) -> anyhow::Result<Vec<IotVerifiedWitnessReport>> {
    if witnesses.len() <= max_count {
        return Ok(Vec::new());
    }
    // Seed a random number from the poc_id for shuffling the witnesses
    let seed = Sha256::digest(poc_id);
    let mut rng = StdRng::from_seed(seed.into());
    witnesses.shuffle(&mut rng);
    let unselected_witnesses = witnesses.split_off(max_count);
    Ok(unselected_witnesses)
}

fn filter_witnesses(
    witnesses: Vec<IotVerifiedWitnessReport>,
) -> (Vec<IotVerifiedWitnessReport>, Vec<IotVerifiedWitnessReport>) {
    let (excluded_witnesses, included_witnesses) = witnesses
        .into_iter()
        .filter(|witness| !matches!(filter_witness(witness.invalid_reason), FilterStatus::Drop))
        .partition(|witness| {
            matches!(
                filter_witness(witness.invalid_reason),
                FilterStatus::Exclude
            )
        });
    (excluded_witnesses, included_witnesses)
}
fn filter_witness(invalid_reason: InvalidReason) -> FilterStatus {
    match invalid_reason {
        InvalidReason::SelfWitness => FilterStatus::Drop,
        InvalidReason::ReasonNone => FilterStatus::Include,
        InvalidReason::BelowMinDistance => FilterStatus::Include,
        InvalidReason::MaxDistanceExceeded => FilterStatus::Include,
        InvalidReason::BadRssi => FilterStatus::Include,
        InvalidReason::InvalidFrequency => FilterStatus::Include,
        InvalidReason::InvalidRegion => FilterStatus::Include,
        _ => FilterStatus::Exclude,
    }
}

fn fire_invalid_witness_metric(witnesses: &[IotVerifiedWitnessReport]) {
    witnesses
        .iter()
        .filter(|witness| !matches!(witness.invalid_reason, InvalidReason::ReasonNone))
        .for_each(|witness| {
            Metrics::increment_invalid_witnesses(&[(
                "reason",
                witness.invalid_reason.as_str_name(),
            )])
        });
}

#[cfg(test)]
mod tests {
    use super::*;
    use file_store::iot_witness_report::IotWitnessReport;
    use helium_crypto::PublicKeyBinary;
    use helium_proto::services::poc_lora::InvalidReason;
    use helium_proto::DataRate;
    use rust_decimal::Decimal;
    use std::str::FromStr;

    #[test]
    fn witness_filtering() {
        let key1 =
            PublicKeyBinary::from_str("112bUuQaE7j73THS9ABShHGokm46Miip9L361FSyWv7zSYn8hZWf")
                .unwrap();
        let report = IotWitnessReport {
            pub_key: key1,
            data: vec![],
            timestamp: Utc::now(),
            tmst: 1,
            signal: 100,
            snr: 10,
            frequency: 68000,
            datarate: DataRate::Sf11bw125,
            signature: vec![],
        };

        let witness1 = IotVerifiedWitnessReport {
            received_timestamp: Utc::now(),
            report: report.clone(),
            location: Some(631252734740306943),
            hex_scale: Decimal::ZERO,
            reward_unit: Decimal::ZERO,
            status: VerificationStatus::Valid,
            invalid_reason: InvalidReason::ReasonNone,
            participant_side: InvalidParticipantSide::SideNone,
        };

        let witness2 = IotVerifiedWitnessReport {
            received_timestamp: Utc::now(),
            report: report.clone(),
            location: Some(631252734740306943),
            hex_scale: Decimal::ZERO,
            reward_unit: Decimal::ZERO,
            status: VerificationStatus::Valid,
            invalid_reason: InvalidReason::SelfWitness,
            participant_side: InvalidParticipantSide::Witness,
        };

        let witness3 = IotVerifiedWitnessReport {
            received_timestamp: Utc::now(),
            report: report.clone(),
            location: Some(631252734740306943),
            hex_scale: Decimal::ZERO,
            reward_unit: Decimal::ZERO,
            status: VerificationStatus::Invalid,
            invalid_reason: InvalidReason::Stale,
            participant_side: InvalidParticipantSide::Witness,
        };

        let witness4 = IotVerifiedWitnessReport {
            received_timestamp: Utc::now(),
            report,
            location: Some(631252734740306943),
            hex_scale: Decimal::ZERO,
            reward_unit: Decimal::ZERO,
            status: VerificationStatus::Invalid,
            invalid_reason: InvalidReason::Duplicate,
            participant_side: InvalidParticipantSide::Witness,
        };

        let witnesses = vec![witness1, witness2, witness3, witness4];
        let (excluded_witnesses, included_witnesses) = filter_witnesses(witnesses);
        assert_eq!(2, excluded_witnesses.len());
        assert_eq!(1, included_witnesses.len());
        assert_eq!(
            InvalidReason::Stale,
            excluded_witnesses.get(0).unwrap().invalid_reason
        );
        assert_eq!(
            InvalidReason::Duplicate,
            excluded_witnesses.get(1).unwrap().invalid_reason
        );
        assert_eq!(
            InvalidReason::ReasonNone,
            included_witnesses.first().unwrap().invalid_reason
        );
    }

    #[test]
    fn max_witnesses_per_poc_test() {
        let key1 =
            PublicKeyBinary::from_str("112bUuQaE7j73THS9ABShHGokm46Miip9L361FSyWv7zSYn8hZWf")
                .unwrap();
        let report = IotWitnessReport {
            pub_key: key1,
            data: vec![],
            timestamp: Utc::now(),
            tmst: 1,
            signal: 100,
            snr: 10,
            frequency: 68000,
            datarate: DataRate::Sf11bw125,
            signature: vec![],
        };

        let witness = IotVerifiedWitnessReport {
            received_timestamp: Utc::now(),
            report,
            location: Some(631252734740306943),
            hex_scale: Decimal::ZERO,
            reward_unit: Decimal::ZERO,
            status: VerificationStatus::Valid,
            invalid_reason: InvalidReason::ReasonNone,
            participant_side: InvalidParticipantSide::SideNone,
        };
        let poc_id: Vec<u8> = vec![0];
        let max_witnesses_per_poc = 14;

        // list of 20 witnesses
        let mut selected_witnesses = vec![witness.clone(); 20];
        assert_eq!(20, selected_witnesses.len());
        // after shuffle and split we should have 14 selected and 6 unselected
        let unselected_witnesses =
            shuffle_and_split_witnesses(&poc_id, &mut selected_witnesses, max_witnesses_per_poc)
                .unwrap();
        assert_eq!(14, selected_witnesses.len());
        assert_eq!(6, unselected_witnesses.len());

        // list of 10 witnesses
        let mut selected_witnesses2 = vec![witness; 10];
        assert_eq!(10, selected_witnesses2.len());
        // after shuffle and split we should have 10 selected and 0 unselected
        let unselected_witnesses2 =
            shuffle_and_split_witnesses(&poc_id, &mut selected_witnesses2, max_witnesses_per_poc)
                .unwrap();
        assert_eq!(10, selected_witnesses2.len());
        assert_eq!(0, unselected_witnesses2.len());
    }

    #[test]
    // this test is based off the example calculations defined in HIP 15
    // https://github.com/helium/HIP/blob/main/0015-beaconing-rewards.md#example-reward-distribution
    // expected values have been adjusted to a 4-decimal precision rounded result rather than the
    // documented 2-decimal places defined in the HIP
    fn reward_unit_calculations() {
        let mut witness_rewards = vec![];
        let mut beacon_rewards = vec![];
        for witnesses in 1..=15 {
            let witness_reward =
                poc_per_witness_reward_unit(witnesses).expect("failed witness reward calculation");
            witness_rewards.push(witness_reward);
            let beacon_reward =
                poc_beaconer_reward_unit(witnesses).expect("failed beacon reward calculation");
            beacon_rewards.push(beacon_reward);
        }

        let expected_witness_rewards: Vec<Decimal> = vec![
            1.0000, 1.0000, 1.0000, 1.0000, 0.7600, 0.6067, 0.5017, 0.4262, 0.3697, 0.3262, 0.2918,
            0.2640, 0.2411, 0.2220, 0.2057,
        ]
        .into_iter()
        .map(|float| {
            Decimal::from_f32_retain(float)
                .expect("failed float to decimal conversion")
                .round_dp(SCALING_PRECISION)
        })
        .collect();
        let expected_beacon_rewards: Vec<Decimal> = vec![
            0.25, 0.50, 0.75, 1.0000, 1.2000, 1.3600, 1.4880, 1.5904, 1.6723, 1.7379, 1.7903,
            1.8322, 1.8658, 1.8926, 1.9141,
        ]
        .into_iter()
        .map(|float| {
            Decimal::from_f32_retain(float)
                .expect("failed float to decimal conversion")
                .round_dp(SCALING_PRECISION)
        })
        .collect();
        assert_eq!(expected_witness_rewards, witness_rewards);
        assert_eq!(expected_beacon_rewards, beacon_rewards);
    }
}
