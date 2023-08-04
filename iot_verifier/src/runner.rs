use crate::{
    gateway_cache::GatewayCache, hex_density::HexDensityMap, last_beacon::LastBeacon, poc::Poc,
    poc_report::Report, region_cache::RegionCache, reward_share::GatewayPocShare, telemetry,
    Settings,
};
use chrono::{Duration as ChronoDuration, Utc};
use denylist::DenyList;
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
use rust_decimal::{Decimal, MathematicalOps};
use rust_decimal_macros::dec;
use sqlx::PgPool;
use std::{path::Path, time::Duration};
use tokio::time::{self, MissedTickBehavior};

/// the cadence in seconds at which the DB is polled for ready POCs
const DB_POLL_TIME: time::Duration = time::Duration::from_secs(30);
const BEACON_WORKERS: usize = 100;

const WITNESS_REDUNDANCY: u32 = 4;
const POC_REWARD_DECAY_RATE: Decimal = dec!(0.8);
const HIP15_TX_REWARD_UNIT_CAP: Decimal = Decimal::TWO;

pub struct Runner {
    pool: PgPool,
    cache: String,
    beacon_interval: ChronoDuration,
    beacon_interval_tolerance: ChronoDuration,
    max_witnesses_per_poc: u64,
    beacon_max_retries: u64,
    witness_max_retries: u64,
    deny_list_latest_url: String,
    deny_list_trigger_interval: Duration,
    deny_list: DenyList,
}

#[derive(thiserror::Error, Debug)]
pub enum RunnerError {
    #[error("not found: {0}")]
    NotFound(&'static str),
}

pub enum FilterStatus {
    Drop,
    Include,
}

impl Runner {
    pub async fn new(settings: &Settings, pool: PgPool) -> anyhow::Result<Self> {
        let cache = settings.cache.clone();
        let beacon_interval = settings.beacon_interval();
        let beacon_interval_tolerance = settings.beacon_interval_tolerance();
        let max_witnesses_per_poc = settings.max_witnesses_per_poc;
        let beacon_max_retries = settings.beacon_max_retries;
        let witness_max_retries = settings.witness_max_retries;
        let deny_list = DenyList::new()?;
        Ok(Self {
            pool,
            cache,
            beacon_interval,
            beacon_interval_tolerance,
            max_witnesses_per_poc,
            beacon_max_retries,
            witness_max_retries,
            deny_list_latest_url: settings.denylist.denylist_url.clone(),
            deny_list_trigger_interval: settings.denylist.trigger_interval(),
            deny_list,
        })
    }

    pub async fn run(
        &mut self,
        file_upload_tx: FileUploadSender,
        gateway_cache: &GatewayCache,
        region_cache: &RegionCache,
        hex_density_map: impl HexDensityMap,
        shutdown: &triggered::Listener,
    ) -> anyhow::Result<()> {
        tracing::info!("starting runner");

        let mut db_timer = time::interval(DB_POLL_TIME);
        db_timer.set_missed_tick_behavior(MissedTickBehavior::Skip);

        let mut denylist_timer = time::interval(self.deny_list_trigger_interval);
        denylist_timer.set_missed_tick_behavior(MissedTickBehavior::Skip);

        let store_base_path = Path::new(&self.cache);

        let (iot_invalid_beacon_sink, mut iot_invalid_beacon_sink_server) =
            file_sink::FileSinkBuilder::new(
                FileType::IotInvalidBeaconReport,
                store_base_path,
                concat!(env!("CARGO_PKG_NAME"), "_invalid_beacon_report"),
                shutdown.clone(),
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
                shutdown.clone(),
            )
            .deposits(Some(file_upload_tx.clone()))
            .roll_time(ChronoDuration::minutes(5))
            .create()
            .await?;

        let (iot_poc_sink, mut iot_poc_sink_server) = file_sink::FileSinkBuilder::new(
            FileType::IotPoc,
            store_base_path,
            concat!(env!("CARGO_PKG_NAME"), "_valid_poc"),
            shutdown.clone(),
        )
        .deposits(Some(file_upload_tx.clone()))
        .roll_time(ChronoDuration::minutes(2))
        .create()
        .await?;

        tokio::spawn(async move { iot_invalid_beacon_sink_server.run().await });
        tokio::spawn(async move { iot_invalid_witness_sink_server.run().await });
        tokio::spawn(async move { iot_poc_sink_server.run().await });

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
                _ = db_timer.tick() =>
                    match self.handle_db_tick(  shutdown.clone(),
                                                &iot_invalid_beacon_sink,
                                                &iot_invalid_witness_sink,
                                                &iot_poc_sink,
                                                gateway_cache,
                                                region_cache,
                                                hex_density_map.clone()).await {
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

    #[allow(clippy::too_many_arguments)]
    async fn handle_db_tick(
        &self,
        _shutdown: triggered::Listener,
        iot_invalid_beacon_sink: &FileSinkClient,
        iot_invalid_witness_sink: &FileSinkClient,
        iot_poc_sink: &FileSinkClient,
        gateway_cache: &GatewayCache,
        region_cache: &RegionCache,
        hex_density_map: impl HexDensityMap,
    ) -> anyhow::Result<()> {
        tracing::info!("starting query get_next_beacons");
        let db_beacon_reports =
            Report::get_next_beacons(&self.pool, self.beacon_max_retries).await?;
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
                            region_cache,
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

    #[allow(clippy::too_many_arguments)]
    async fn handle_beacon_report(
        &self,
        db_beacon: Report,
        iot_invalid_beacon_sink: &FileSinkClient,
        iot_invalid_witness_sink: &FileSinkClient,
        iot_poc_sink: &FileSinkClient,
        gateway_cache: &GatewayCache,
        region_cache: &RegionCache,
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

        let db_witnesses =
            Report::get_witnesses_for_beacon(&self.pool, packet_data, self.witness_max_retries)
                .await?;
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
                region_cache,
                &self.pool,
                self.beacon_interval,
                self.beacon_interval_tolerance,
                &self.deny_list,
            )
            .await?;
        match beacon_verify_result.result {
            VerificationStatus::Valid => {
                // beacon is valid, verify the POC witnesses
                if let Some(beacon_info) = beacon_verify_result.gateway_info {
                    let verified_witnesses_result = poc
                        .verify_witnesses(
                            &beacon_info,
                            hex_density_map,
                            gateway_cache,
                            &self.deny_list,
                        )
                        .await?;
                    // check if there are any failed witnesses
                    // if so update the DB attempts count
                    // and halt here, let things be reprocessed next tick
                    // if a witness continues to fail it will eventually
                    // be discarded from the list returned for the beacon
                    // thus one or more failing witnesses will not block the overall POC
                    if !verified_witnesses_result.failed_witnesses.is_empty() {
                        tracing::warn!("failed to handle witness");
                        for failed_witness_report in verified_witnesses_result.failed_witnesses {
                            let failed_witness = failed_witness_report.report;
                            let id =
                                failed_witness.report_id(failed_witness_report.received_timestamp);
                            Report::update_attempts(&self.pool, &id, Utc::now()).await?;
                        }
                        return Ok(());
                    };

                    let max_witnesses_per_poc = self.max_witnesses_per_poc as usize;

                    // filter witnesses into selected and unselected lists
                    // the selected list will contain only valid witnesses
                    // up to a max count equal to `max_witnesses_per_poc`
                    // these witnesses will be rewarded
                    // the unselected list will contain potentially a mix of
                    // valid and invalid witnesses
                    // none of which will be rewarded
                    // we exclude self witnesses from the unselected lists
                    // these are dropped to the floor, never make it to s3
                    let (mut selected_witnesses, invalid_witnesses) =
                        filter_witnesses(verified_witnesses_result.verified_witnesses);

                    // keep a subset of our selected and valid witnesses
                    let mut unselected_witnesses =
                        sort_and_split_witnesses(&mut selected_witnesses, max_witnesses_per_poc)?;

                    // concat the unselected valid witnesses and the invalid witnesses
                    // these will then form the unseleted list on the poc
                    unselected_witnesses =
                        [&unselected_witnesses[..], &invalid_witnesses[..]].concat();

                    // get the number of valid witnesses in our selected list
                    let num_valid_selected_witnesses = selected_witnesses.len();

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

                    // metadata at this point will always be Some...
                    let (location, gain, elevation) = match beacon_info.metadata {
                        Some(metadata) => {
                            (Some(metadata.location), metadata.gain, metadata.elevation)
                        }
                        None => (None, 0, 0),
                    };

                    let valid_beacon_report = IotValidBeaconReport {
                        received_timestamp: beacon_received_ts,
                        location,
                        gain,
                        elevation,
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
        telemetry::decrement_num_beacons();
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
        for reward_share in GatewayPocShare::shares_from_poc(&iot_poc) {
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
        telemetry::decrement_num_beacons();
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

/// maybe sort & split the verified witness list
// if the number of witnesses exceeds our cap
// split the list into two
// one representing selected witnesses
// the other representing unselected witnesses
fn sort_and_split_witnesses(
    witnesses: &mut Vec<IotVerifiedWitnessReport>,
    max_count: usize,
) -> anyhow::Result<Vec<IotVerifiedWitnessReport>> {
    if witnesses.len() <= max_count {
        return Ok(Vec::new());
    }
    witnesses.sort_by(|a, b| a.received_timestamp.cmp(&b.received_timestamp));
    let unselected_witnesses = witnesses.split_off(max_count);
    Ok(unselected_witnesses)
}

fn filter_witnesses(
    witnesses: Vec<IotVerifiedWitnessReport>,
) -> (Vec<IotVerifiedWitnessReport>, Vec<IotVerifiedWitnessReport>) {
    let (valid_witnesses, invalid_witnesses) = witnesses
        .into_iter()
        .filter(|witness| {
            matches!(
                filter_witness(witness.invalid_reason),
                FilterStatus::Include
            )
        })
        .partition(|witness| witness.status == VerificationStatus::Valid);
    (valid_witnesses, invalid_witnesses)
}

fn filter_witness(invalid_reason: InvalidReason) -> FilterStatus {
    match invalid_reason {
        InvalidReason::SelfWitness => FilterStatus::Drop,
        _ => FilterStatus::Include,
    }
}

fn fire_invalid_witness_metric(witnesses: &[IotVerifiedWitnessReport]) {
    witnesses
        .iter()
        .filter(|witness| !matches!(witness.invalid_reason, InvalidReason::ReasonNone))
        .for_each(|witness| {
            telemetry::increment_invalid_witnesses(&[(
                "reason",
                witness.invalid_reason.clone().as_str_name(),
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
            gain: 20,
            elevation: 100,
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
            gain: 20,
            elevation: 100,
            hex_scale: Decimal::ZERO,
            reward_unit: Decimal::ZERO,
            status: VerificationStatus::Invalid,
            invalid_reason: InvalidReason::SelfWitness,
            participant_side: InvalidParticipantSide::Witness,
        };

        let witness3 = IotVerifiedWitnessReport {
            received_timestamp: Utc::now(),
            report: report.clone(),
            location: Some(631252734740306943),
            gain: 20,
            elevation: 100,
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
            gain: 20,
            elevation: 100,
            hex_scale: Decimal::ZERO,
            reward_unit: Decimal::ZERO,
            status: VerificationStatus::Invalid,
            invalid_reason: InvalidReason::Duplicate,
            participant_side: InvalidParticipantSide::Witness,
        };

        let witnesses = vec![witness1, witness2, witness3, witness4];
        let (included_witnesses, excluded_witnesses) = filter_witnesses(witnesses);
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

        let current_time = Utc::now();
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

        // list of 30 witnesses
        let mut selected_witnesses: Vec<IotVerifiedWitnessReport> = (0..30)
            .map(|i| IotVerifiedWitnessReport {
                received_timestamp: current_time
                    .checked_add_signed(ChronoDuration::milliseconds(i))
                    .unwrap(),
                report: report.clone(),
                location: Some(631252734740306943),
                gain: 20,
                elevation: 100,
                hex_scale: Decimal::ZERO,
                reward_unit: Decimal::ZERO,
                status: VerificationStatus::Valid,
                invalid_reason: InvalidReason::ReasonNone,
                participant_side: InvalidParticipantSide::SideNone,
            })
            .collect::<Vec<IotVerifiedWitnessReport>>();
        selected_witnesses.reverse();
        let max_witnesses_per_poc = 14;

        assert_eq!(30, selected_witnesses.len());
        assert_eq!(
            current_time
                .checked_add_signed(ChronoDuration::milliseconds(9))
                .unwrap(),
            selected_witnesses[20].received_timestamp
        );

        let unselected_witnesses =
            sort_and_split_witnesses(&mut selected_witnesses, max_witnesses_per_poc).unwrap();
        assert_eq!(14, selected_witnesses.len());
        assert_eq!(16, unselected_witnesses.len());

        assert_eq!(current_time, selected_witnesses[0].received_timestamp);
        assert_eq!(
            current_time
                .checked_add_signed(ChronoDuration::milliseconds(4))
                .unwrap(),
            selected_witnesses[4].received_timestamp
        );

        // list of 10 witnesses
        let mut selected_witnesses2 = vec![selected_witnesses[0].clone(); 10];
        assert_eq!(10, selected_witnesses2.len());
        // after sort and split we should have 10 selected and 0 unselected
        let unselected_witnesses2 =
            sort_and_split_witnesses(&mut selected_witnesses2, max_witnesses_per_poc).unwrap();
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
