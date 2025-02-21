use crate::{
    gateway_cache::GatewayCache,
    hex_density::HexDensityMap,
    last_beacon_reciprocity::LastBeaconReciprocity,
    poc::{Poc, VerifyBeaconResult},
    poc_report::Report,
    region_cache::RegionCache,
    reward_share::GatewayPocShare,
    telemetry,
    witness_updater::WitnessUpdater,
    Settings,
};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use denylist::DenyList;
use file_store::{
    file_sink::FileSinkClient,
    iot_beacon_report::IotBeaconIngestReport,
    iot_invalid_poc::{IotInvalidBeaconReport, IotInvalidWitnessReport},
    iot_valid_poc::{IotPoc, IotValidBeaconReport, IotVerifiedWitnessReport},
    iot_witness_report::IotWitnessIngestReport,
    traits::{IngestId, MsgDecode, ReportId},
    SCALING_PRECISION,
};
use futures::{future::LocalBoxFuture, stream, StreamExt, TryFutureExt};
use helium_proto::services::poc_lora::{
    InvalidDetails, InvalidParticipantSide, InvalidReason, LoraInvalidBeaconReportV1,
    LoraInvalidWitnessReportV1, LoraPocV1, VerificationStatus,
};
use iot_config::{client::Gateways, gateway_info::GatewayInfo};
use lazy_static::lazy_static;
use rust_decimal::{Decimal, MathematicalOps};
use rust_decimal_macros::dec;
use sqlx::PgPool;
use std::time::Duration;
use task_manager::ManagedTask;
use tokio::time::{self, MissedTickBehavior};

/// the cadence in seconds at which the DB is polled for ready POCs
const DB_POLL_TIME: Duration = Duration::from_secs(30);
const BEACON_WORKERS: usize = 100;

const WITNESS_REDUNDANCY: u32 = 4;
const POC_REWARD_DECAY_RATE: Decimal = dec!(0.8);
const HIP15_TX_REWARD_UNIT_CAP: Decimal = Decimal::TWO;

lazy_static! {
/// the duration in which a beaconer or witness must have a valid opposite report from
    static ref RECIPROCITY_WINDOW: ChronoDuration = ChronoDuration::hours(48);
}

pub struct Runner<G> {
    pub pool: PgPool,
    pub beacon_interval: Duration,
    pub max_witnesses_per_poc: u64,
    pub beacon_max_retries: u64,
    pub witness_max_retries: u64,
    pub deny_list_latest_url: String,
    pub deny_list_trigger_interval: Duration,
    pub deny_list: DenyList,
    pub gateway_cache: GatewayCache,
    pub region_cache: RegionCache<G>,
    pub invalid_beacon_sink: FileSinkClient<LoraInvalidBeaconReportV1>,
    pub invalid_witness_sink: FileSinkClient<LoraInvalidWitnessReportV1>,
    pub poc_sink: FileSinkClient<LoraPocV1>,
    pub hex_density_map: HexDensityMap,
    pub witness_updater: WitnessUpdater,
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

impl<G> ManagedTask for Runner<G>
where
    G: Gateways,
{
    fn start_task(
        self: Box<Self>,
        shutdown: triggered::Listener,
    ) -> LocalBoxFuture<'static, anyhow::Result<()>> {
        let handle = tokio::spawn(self.run(shutdown));
        Box::pin(
            handle
                .map_err(anyhow::Error::from)
                .and_then(|result| async move { result }),
        )
    }
}

impl<G> Runner<G>
where
    G: Gateways,
{
    #[allow(clippy::too_many_arguments)]
    pub async fn from_settings(
        settings: &Settings,
        gateways: G,
        pool: PgPool,
        gateway_cache: GatewayCache,
        invalid_beacon_sink: FileSinkClient<LoraInvalidBeaconReportV1>,
        invalid_witness_sink: FileSinkClient<LoraInvalidWitnessReportV1>,
        poc_sink: FileSinkClient<LoraPocV1>,
        hex_density_map: HexDensityMap,
        witness_updater: WitnessUpdater,
    ) -> anyhow::Result<Self> {
        let beacon_interval = settings.beacon_interval;
        let max_witnesses_per_poc = settings.max_witnesses_per_poc;
        let beacon_max_retries = settings.beacon_max_retries;
        let witness_max_retries = settings.witness_max_retries;
        let deny_list_latest_url = settings.denylist.denylist_url.clone();
        let mut deny_list = DenyList::new(&settings.denylist)?;
        let region_cache = RegionCache::new(settings.region_params_refresh_interval, gateways)?;
        // force update to latest in order to update the tag name
        // when first run, the denylist will load the local filter
        // but we dont save the tag name so it defaults to 0
        // updating it here forces the tag name to be refreshed
        // which will see it carry through to invalid poc reports
        // if we cant update such as github being down then ignore
        match deny_list.update_to_latest(&deny_list_latest_url).await {
            Ok(()) => (),
            Err(err) => {
                tracing::error!("error whilst updating denylist to latest: {err:?}");
            }
        }

        Ok(Self {
            pool,
            beacon_interval,
            max_witnesses_per_poc,
            gateway_cache,
            region_cache,
            beacon_max_retries,
            witness_max_retries,
            deny_list_latest_url,
            deny_list_trigger_interval: settings.denylist.trigger_interval,
            deny_list,
            invalid_beacon_sink,
            invalid_witness_sink,
            poc_sink,
            hex_density_map,
            witness_updater,
        })
    }

    pub async fn run(mut self, shutdown: triggered::Listener) -> anyhow::Result<()> {
        tracing::info!("starting runner");

        let mut db_timer = time::interval(DB_POLL_TIME);
        db_timer.set_missed_tick_behavior(MissedTickBehavior::Skip);

        let mut denylist_timer = time::interval(self.deny_list_trigger_interval);
        denylist_timer.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                biased;
                _ = shutdown.clone() => break,
                _ = denylist_timer.tick() =>
                    match self.handle_denylist_tick().await {
                    Ok(()) => (),
                    Err(err) => {
                        tracing::error!("error whilst handling denylist tick: {err:?}");
                    }
                },
                _ = db_timer.tick() =>
                    match self.handle_db_tick().await {
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

    pub async fn handle_db_tick(&self) -> anyhow::Result<()> {
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
            .for_each_concurrent(BEACON_WORKERS, |db_beacon| async move {
                let beacon_id = db_beacon.id.clone();
                match self.handle_beacon_report(db_beacon).await {
                    Ok(()) => (),
                    Err(err) => {
                        tracing::warn!("failed to handle beacon: {err:?}");
                        _ = Report::update_attempts(&self.pool, &beacon_id, Utc::now()).await;
                    }
                }
            })
            .await;
        tracing::info!("completed processing {beacon_len} beacons");
        Ok(())
    }

    async fn handle_beacon_report(&self, db_beacon: Report) -> anyhow::Result<()> {
        // get the beacon report and any associated witnesses and then generate a POC
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

        let db_witnesses =
            Report::get_witnesses_for_beacon(&self.pool, packet_data, self.witness_max_retries)
                .await?;
        let witness_len = db_witnesses.len();
        tracing::debug!("found {witness_len} witness for beacon");

        // decode the protobuf witness reports
        let witnesses = db_witnesses
            .into_iter()
            .map(|w| IotWitnessIngestReport::decode(w.report_data.as_slice()))
            .collect::<Result<Vec<IotWitnessIngestReport>, _>>()?;

        // create the struct defining this POC
        let poc = Poc::new(
            self.pool.clone(),
            self.beacon_interval,
            beacon_report,
            witnesses,
            entropy_start_time,
            entropy_version,
        )
        .await;

        self.verify_poc(poc).await
    }

    async fn verify_poc(&self, mut poc: Poc) -> anyhow::Result<()> {
        // verify POC beacon
        let beacon_verify_result = poc
            .verify_beacon(
                &self.hex_density_map,
                &self.gateway_cache,
                &self.region_cache,
                &self.deny_list,
            )
            .await?;

        match beacon_verify_result {
            VerifyBeaconResult {
                result: VerificationStatus::Valid,
                gateway_info: Some(beacon_info),
                ..
            } => {
                // beacon is valid, verify the POC witnesses
                let verified_witnesses_result = poc
                    .verify_witnesses(
                        &beacon_info,
                        &self.hex_density_map,
                        &self.gateway_cache,
                        &self.deny_list,
                        &self.witness_updater,
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
                        let id = failed_witness_report
                            .report
                            .report_id(failed_witness_report.received_timestamp);
                        Report::update_attempts(&self.pool, &id, Utc::now()).await?;
                    }
                    return Ok(());
                };

                if !self.verify_beacon_reciprocity(&poc.beacon_report).await? {
                    return self
                        .handle_invalid_poc(
                            poc,
                            InvalidReason::GatewayNoValidWitnesses,
                            None,
                            Some(beacon_info),
                        )
                        .await;
                }

                let final_verified_witnesses = self
                    .verify_witnesses_reciprocity(verified_witnesses_result.verified_witnesses)
                    .await?;

                self.handle_valid_poc(
                    poc,
                    beacon_info,
                    beacon_verify_result.hex_scale,
                    final_verified_witnesses,
                )
                .await
            }
            _ => {
                // the beacon is invalid, which in turn renders all witnesses invalid
                self.handle_invalid_poc(
                    poc,
                    beacon_verify_result.invalid_reason,
                    beacon_verify_result.invalid_details,
                    beacon_verify_result.gateway_info,
                )
                .await
            }
        }
    }

    async fn handle_valid_poc(
        &self,
        poc: Poc,
        beacon_info: GatewayInfo,
        beacon_hex_scale: Option<Decimal>,
        verified_witnesses: Vec<IotVerifiedWitnessReport>,
    ) -> anyhow::Result<()> {
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
        let (mut selected_witnesses, unselected_witnesses) =
            filter_and_split_witnesses(verified_witnesses, max_witnesses_per_poc)?;

        // get the number of valid witnesses in our selected list
        let num_valid_selected_witnesses = selected_witnesses.len();

        update_witness_reward_units(&mut selected_witnesses, num_valid_selected_witnesses)?;

        let beacon_received_ts = poc.beacon_report.received_timestamp;
        let beacon_report_id = poc.beacon_report.report.report_id(beacon_received_ts);
        let packet_data = poc.beacon_report.report.data.clone();

        // collect all the invalid reasons, we will use these later for metrics
        let invalid_reasons = collect_invalid_witness_reasons(&unselected_witnesses);

        let iot_poc = create_iot_poc(
            poc,
            beacon_hex_scale,
            selected_witnesses,
            unselected_witnesses,
            beacon_report_id.clone(),
            beacon_received_ts,
            beacon_info,
        )?;

        // save the gateway shares for each poc to db
        let mut transaction = self.pool.begin().await?;
        for reward_share in GatewayPocShare::shares_from_poc(&iot_poc) {
            reward_share.save(&mut transaction).await?;
        }
        transaction.commit().await?;

        let poc_proto: LoraPocV1 = iot_poc.into();
        // save the poc to s3, if write fails update attempts and go no further
        // allow the poc to be reprocessed next tick
        match self.poc_sink.write(poc_proto, []).await {
            Ok(_) => (),
            Err(err) => {
                tracing::error!("failed to save invalid_witness_report to s3, {err}");
                Report::update_attempts(&self.pool, &beacon_report_id, Utc::now()).await?;
                return Ok(());
            }
        }

        Report::delete_poc(&self.pool, &packet_data).await?;

        // write out metrics for any witness which failed verification
        fire_invalid_witness_metric(invalid_reasons);
        telemetry::decrement_num_beacons();
        Ok(())
    }

    async fn handle_invalid_poc(
        &self,
        poc: Poc,
        beacon_invalid_reason: InvalidReason,
        beacon_invalid_details: Option<InvalidDetails>,
        beacon_info: Option<GatewayInfo>,
    ) -> anyhow::Result<()> {
        // the beacon is invalid, which in turn renders all witnesses invalid
        let beacon = &poc.beacon_report.report;
        let beacon_id = beacon.data.clone();
        let beacon_report_id = poc.beacon_report.ingest_id();

        let (location, elevation, gain) = match beacon_info {
            Some(gateway_info) => match gateway_info.metadata {
                Some(metadata) => (Some(metadata.location), metadata.elevation, metadata.gain),
                None => (None, 0, 0),
            },
            None => (None, 0, 0),
        };

        let invalid_poc: IotInvalidBeaconReport = IotInvalidBeaconReport {
            received_timestamp: poc.beacon_report.received_timestamp,
            reason: beacon_invalid_reason,
            invalid_details: beacon_invalid_details.clone(),
            report: beacon.clone(),
            location,
            elevation,
            gain,
        };
        let invalid_poc_proto: LoraInvalidBeaconReportV1 = invalid_poc.into();
        // save invalid poc to s3, if write fails update attempts and go no further
        // allow the poc to be reprocessed next tick
        match self
            .invalid_beacon_sink
            .write(
                invalid_poc_proto,
                &[("reason", beacon_invalid_reason.as_str_name())],
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
        // we will have to clean out any successful writes of other witnesses
        // and also the invalid poc
        // so if a report fails from this point on, it shall be lost for ever more
        for witness_report in poc.witness_reports {
            let invalid_witness_report: IotInvalidWitnessReport = IotInvalidWitnessReport {
                received_timestamp: witness_report.received_timestamp,
                report: witness_report.report,
                reason: beacon_invalid_reason,
                invalid_details: beacon_invalid_details.clone(),
                participant_side: InvalidParticipantSide::Beaconer,
            };
            let invalid_witness_report_proto: LoraInvalidWitnessReportV1 =
                invalid_witness_report.into();
            match self
                .invalid_witness_sink
                .write(
                    invalid_witness_report_proto,
                    &[("reason", beacon_invalid_reason.as_str_name())],
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

    async fn verify_beacon_reciprocity(
        &self,
        beacon_report: &IotBeaconIngestReport,
    ) -> anyhow::Result<bool> {
        let last_witness = self
            .witness_updater
            .get_last_witness(&beacon_report.report.pub_key)
            .await?;
        Ok(last_witness.is_some_and(|lw| {
            beacon_report.received_timestamp - lw.timestamp < *RECIPROCITY_WINDOW
        }))
    }

    async fn verify_witnesses_reciprocity(
        &self,
        witnesses: Vec<IotVerifiedWitnessReport>,
    ) -> anyhow::Result<Vec<IotVerifiedWitnessReport>> {
        let mut verified_witnesses = Vec::new();
        for mut witness in witnesses {
            if witness.status == VerificationStatus::Valid
                && !self.verify_witness_reciprocity(&witness).await?
            {
                witness.invalid_reason = InvalidReason::GatewayNoValidBeacons;
                witness.status = VerificationStatus::Invalid;
                witness.invalid_details = None;
                witness.participant_side = InvalidParticipantSide::Witness
            }
            verified_witnesses.push(witness)
        }
        Ok(verified_witnesses)
    }

    async fn verify_witness_reciprocity(
        &self,
        report: &IotVerifiedWitnessReport,
    ) -> anyhow::Result<bool> {
        let last_beacon_recip =
            LastBeaconReciprocity::get(&self.pool, &report.report.pub_key).await?;
        Ok(last_beacon_recip
            .is_some_and(|lw| report.received_timestamp - lw.timestamp < *RECIPROCITY_WINDOW))
    }
}

fn create_iot_poc(
    poc: Poc,
    beacon_hex_scale: Option<Decimal>,
    selected_witnesses: Vec<IotVerifiedWitnessReport>,
    unselected_witnesses: Vec<IotVerifiedWitnessReport>,
    beacon_report_id: Vec<u8>,
    beacon_received_ts: DateTime<Utc>,
    beacon_info: GatewayInfo,
) -> anyhow::Result<IotPoc> {
    let (location, gain, elevation) = match beacon_info.metadata {
        Some(metadata) => (Some(metadata.location), metadata.gain, metadata.elevation),
        None => (None, 0, 0),
    };
    let num_valid_selected_witnesses = selected_witnesses.len();
    let beaconer_reward_units = poc_beaconer_reward_unit(num_valid_selected_witnesses as u32)?;
    let valid_beacon_report = IotValidBeaconReport {
        received_timestamp: beacon_received_ts,
        location,
        gain,
        elevation,
        hex_scale: beacon_hex_scale.ok_or(RunnerError::NotFound("invalid hex scaling factor"))?,
        report: poc.beacon_report.report,
        reward_unit: beaconer_reward_units,
    };
    Ok(IotPoc {
        poc_id: beacon_report_id,
        beacon_report: valid_beacon_report,
        selected_witnesses,
        unselected_witnesses,
    })
}

fn update_witness_reward_units(
    selected_witnesses: &mut [IotVerifiedWitnessReport],
    num_valid_selected_witnesses: usize,
) -> anyhow::Result<()> {
    // get reward units based on the count of valid selected witnesses
    let witness_reward_units = poc_per_witness_reward_unit(num_valid_selected_witnesses as u32)?;
    // update the reward units for those valid witnesses within our selected list
    selected_witnesses
        .iter_mut()
        .for_each(|witness| match witness.status {
            VerificationStatus::Valid => witness.reward_unit = witness_reward_units,
            VerificationStatus::Invalid => witness.reward_unit = Decimal::ZERO,
        });
    Ok(())
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

fn filter_and_split_witnesses(
    witnesses: Vec<IotVerifiedWitnessReport>,
    max_witnesses_per_poc: usize,
) -> anyhow::Result<(Vec<IotVerifiedWitnessReport>, Vec<IotVerifiedWitnessReport>)> {
    let (mut selected_witnesses, invalid_witnesses) = witnesses
        .into_iter()
        .filter(|witness| {
            matches!(
                filter_witness(witness.invalid_reason),
                FilterStatus::Include
            )
        })
        .partition(|witness| witness.status == VerificationStatus::Valid);

    // keep a subset of our selected and valid witnesses
    let mut unselected_witnesses =
        sort_and_split_witnesses(&mut selected_witnesses, max_witnesses_per_poc)?;

    // concat the unselected valid witnesses and the invalid witnesses
    // these will then form the unselected list on the poc
    unselected_witnesses.extend(invalid_witnesses);
    Ok((selected_witnesses, unselected_witnesses))
}

fn filter_witness(invalid_reason: InvalidReason) -> FilterStatus {
    match invalid_reason {
        InvalidReason::SelfWitness => FilterStatus::Drop,
        _ => FilterStatus::Include,
    }
}
fn collect_invalid_witness_reasons(witnesses: &[IotVerifiedWitnessReport]) -> Vec<InvalidReason> {
    witnesses
        .iter()
        .filter(|witness| !matches!(witness.invalid_reason, InvalidReason::ReasonNone))
        .map(|witness| witness.invalid_reason)
        .collect()
}

fn fire_invalid_witness_metric(invalid_reasons: Vec<InvalidReason>) {
    invalid_reasons.iter().for_each(|reason| {
        telemetry::increment_invalid_witnesses(&[("reason", reason.as_str_name())])
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration as ChronoDuration;
    use file_store::iot_witness_report::IotWitnessReport;
    use helium_crypto::PublicKeyBinary;
    use helium_proto::services::poc_lora::InvalidReason;
    use helium_proto::DataRate;
    use rust_decimal::Decimal;
    use std::str::FromStr;
    const NUM_WITNESSES_PER_POC: usize = 14;

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
            invalid_details: None,
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
            invalid_details: None,
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
            invalid_details: None,
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
            invalid_details: None,
            participant_side: InvalidParticipantSide::Witness,
        };

        let witnesses = vec![witness1, witness2, witness3, witness4];
        let (included_witnesses, excluded_witnesses) =
            filter_and_split_witnesses(witnesses, NUM_WITNESSES_PER_POC).unwrap();
        assert_eq!(2, excluded_witnesses.len());
        assert_eq!(1, included_witnesses.len());
        assert_eq!(
            InvalidReason::Stale,
            excluded_witnesses.first().unwrap().invalid_reason
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
                invalid_details: None,
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
