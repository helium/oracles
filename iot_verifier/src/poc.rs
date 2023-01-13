use crate::{
    entropy::ENTROPY_LIFESPAN,
    gateway_cache::GatewayCache,
    last_beacon::{LastBeacon, LastBeaconError},
};
use chrono::{DateTime, Duration, Utc};
use density_scaler::HexDensityMap;
use file_store::{
    iot_beacon_report::IotBeaconIngestReport, iot_invalid_poc::IotInvalidWitnessReport,
    iot_valid_poc::IotValidWitnessReport, iot_witness_report::IotWitnessIngestReport,
};
use geo::{point, prelude::*, vincenty_distance::FailedToConvergeError};
use h3ron::{to_geo::ToCoordinate, H3Cell, H3DirectedEdge, Index};
use helium_crypto::PublicKeyBinary;
use helium_proto::{
    services::poc_iot::{InvalidParticipantSide, InvalidReason},
    GatewayStakingMode, Region,
};
use node_follower::gateway_resp::GatewayInfo;
use rust_decimal::Decimal;
use sqlx::PgPool;
use std::f64::consts::PI;

pub type GenericVerifyResult<T = ()> = std::result::Result<T, InvalidReason>;

/// C is the speed of light in air in meters per second
pub const C: f64 = 2.998e8;
/// R is the (average) radius of the earth
pub const R: f64 = 6.371e6;

/// the cadence in seconds at which hotspots are permitted to beacon
const BEACON_INTERVAL: i64 = (10 * 60) - 10; // 10 mins ( minus 10 sec tolerance )
/// max permitted distance of a witness from a beaconer measured in KM
const POC_DISTANCE_LIMIT: i32 = 100;

#[derive(Debug)]
pub enum VerificationStatus {
    Valid,
    Invalid,
    Failed,
}

pub struct Poc {
    beacon_report: IotBeaconIngestReport,
    witness_reports: Vec<IotWitnessIngestReport>,
    entropy_start: DateTime<Utc>,
    entropy_end: DateTime<Utc>,
}

pub struct VerifyBeaconResult {
    pub result: VerificationStatus,
    pub invalid_reason: Option<InvalidReason>,
    pub gateway_info: Option<GatewayInfo>,
    pub hex_scale: Option<Decimal>,
}

pub struct VerifyWitnessResult {
    result: VerificationStatus,
    invalid_reason: Option<InvalidReason>,
    pub gateway_info: Option<GatewayInfo>,
    hex_scale: Option<Decimal>,
}

pub struct VerifyWitnessesResult {
    pub valid_witnesses: Vec<IotValidWitnessReport>,
    pub invalid_witnesses: Vec<IotInvalidWitnessReport>,
    pub failed_witnesses: Vec<IotInvalidWitnessReport>,
}

impl VerifyWitnessesResult {
    pub fn update_reward_units(&mut self, reward_units: Decimal) {
        self.valid_witnesses
            .iter_mut()
            .for_each(|witness| witness.reward_unit = reward_units)
    }
}

#[derive(thiserror::Error, Debug)]
pub enum VerificationError {
    #[error("not found: {0}")]
    NotFound(&'static str),
    #[error("last beacon error: {0}")]
    LastBeaconError(#[from] LastBeaconError),
    #[error("calc distance error: {0}")]
    CalcDistanceError(#[from] CalcDistanceError),
}

impl Poc {
    pub async fn new(
        beacon_report: IotBeaconIngestReport,
        witness_reports: Vec<IotWitnessIngestReport>,
        entropy_start: DateTime<Utc>,
    ) -> Self {
        let entropy_end = entropy_start + Duration::seconds(ENTROPY_LIFESPAN);
        Self {
            beacon_report,
            witness_reports,
            entropy_start,
            entropy_end,
        }
    }

    pub async fn verify_beacon(
        &mut self,
        hex_density_map: impl HexDensityMap,
        gateway_cache: &GatewayCache,
        pool: &PgPool,
    ) -> Result<VerifyBeaconResult, VerificationError> {
        let beacon = &self.beacon_report.report;
        let beaconer_pub_key = beacon.pub_key.clone();
        // get the beaconer info from our follower
        // it not available then declare beacon invalid
        let beaconer_info = match gateway_cache.resolve_gateway_info(&beaconer_pub_key).await {
            Ok(res) => res,
            Err(_e) => {
                return Ok(VerifyBeaconResult::gateway_not_found());
            }
        };
        // we have beaconer info, proceed to verifications
        let last_beacon = LastBeacon::get(pool, beaconer_pub_key.as_ref()).await?;
        match self
            .do_beacon_verifications(last_beacon, &beaconer_info)
            .await
        {
            Ok(()) => {
                let beaconer_location = beaconer_info
                    .location
                    .ok_or(VerificationError::NotFound("invalid beaconer_location"))?;

                if let Some(scaling_factor) = hex_density_map.get(beaconer_location).await {
                    Ok(VerifyBeaconResult::valid(beaconer_info, scaling_factor))
                } else {
                    Ok(VerifyBeaconResult::scaling_factor_not_found(beaconer_info))
                }
            }
            Err(invalid_reason) => Ok(VerifyBeaconResult::invalid(invalid_reason, beaconer_info)),
        }
    }

    pub async fn verify_witnesses(
        &mut self,
        beacon_info: &GatewayInfo,
        hex_density_map: impl HexDensityMap,
        gateway_cache: &GatewayCache,
    ) -> Result<VerifyWitnessesResult, VerificationError> {
        let mut valid_witnesses: Vec<IotValidWitnessReport> = Vec::new();
        let mut invalid_witnesses: Vec<IotInvalidWitnessReport> = Vec::new();
        let mut failed_witnesses: Vec<IotInvalidWitnessReport> = Vec::new();
        let witnesses = self.witness_reports.clone();
        for witness_report in witnesses {
            match self
                .verify_witness(
                    &witness_report,
                    beacon_info,
                    gateway_cache,
                    &hex_density_map,
                )
                .await
            {
                Ok(witness_result) => {
                    match witness_result.result {
                        VerificationStatus::Valid => {
                            if let Ok(valid_witness) = self
                                .valid_witness_report(witness_result, witness_report)
                                .await
                            {
                                valid_witnesses.push(valid_witness)
                            };
                        }
                        VerificationStatus::Invalid => {
                            if let Ok(invalid_witness) = self
                                .invalid_witness_report(witness_result, witness_report)
                                .await
                            {
                                invalid_witnesses.push(invalid_witness)
                            }
                        }
                        VerificationStatus::Failed => {
                            // if a witness check returns failed it suggests something
                            // unexpected has occurred. Fail the witness
                            let failed_reason = invalid_reason_or_default(
                                witness_result.invalid_reason,
                                InvalidReason::UnknownError,
                            );
                            if let Ok(failed_witness) = self
                                .failed_witness_report(failed_reason, witness_report)
                                .await
                            {
                                failed_witnesses.push(failed_witness)
                            }
                        }
                    }
                }
                Err(err) => {
                    tracing::warn!("Unexpected error verifying witness: {err:?}");
                    if let Ok(failed_witness) = self
                        .failed_witness_report(InvalidReason::UnknownError, witness_report)
                        .await
                    {
                        failed_witnesses.push(failed_witness)
                    }
                }
            }
        }
        let resp = VerifyWitnessesResult {
            invalid_witnesses,
            valid_witnesses,
            failed_witnesses,
        };
        Ok(resp)
    }

    async fn verify_witness(
        &mut self,
        witness_report: &IotWitnessIngestReport,
        beaconer_info: &GatewayInfo,
        gateway_cache: &GatewayCache,
        hex_density_map: &impl HexDensityMap,
    ) -> Result<VerifyWitnessResult, VerificationError> {
        let witness = &witness_report.report;
        let witness_pub_key = witness.pub_key.clone();
        // pull the witness info from our follower
        let witness_info = match gateway_cache.resolve_gateway_info(&witness_pub_key).await {
            Ok(res) => res,
            Err(_e) => {
                return Ok(VerifyWitnessResult::gateway_not_found());
            }
        };
        // run the witness verifications
        match self
            .do_witness_verifications(&witness_info, witness_report, beaconer_info)
            .await
        {
            Ok(()) => {
                // to avoid assuming witness location is set and to avoid unwrap
                // we explicity match location here again
                let Some(witness_location) = witness_info.location else {
                    return Ok(VerifyWitnessResult::not_asserted())
                };

                if let Some(hex_scale) = hex_density_map.get(witness_location).await {
                    Ok(VerifyWitnessResult::valid(witness_info, hex_scale))
                } else {
                    Ok(VerifyWitnessResult::scaling_factor_not_found(witness_info))
                }
            }
            Err(invalid_reason) => Ok(VerifyWitnessResult::invalid(invalid_reason, witness_info)),
        }
    }

    pub async fn do_beacon_verifications(
        &mut self,
        last_beacon: Option<LastBeacon>,
        beaconer_info: &GatewayInfo,
    ) -> GenericVerifyResult {
        tracing::debug!(
            "verifying beacon from beaconer: {:?}",
            PublicKeyBinary::from(beaconer_info.address.clone())
        );
        let beacon_received_ts = self.beacon_report.received_timestamp;
        verify_entropy(self.entropy_start, self.entropy_end, beacon_received_ts)?;
        verify_beacon_schedule(&last_beacon, beacon_received_ts)?;
        verify_gw_location(beaconer_info.location)?;
        verify_gw_capability(beaconer_info.staking_mode)?;
        tracing::debug!(
            "valid beacon from beaconer: {:?}",
            PublicKeyBinary::from(beaconer_info.address.clone())
        );
        Ok(())
    }

    pub async fn do_witness_verifications(
        &mut self,
        witness_info: &GatewayInfo,
        witness_report: &IotWitnessIngestReport,
        beaconer_info: &GatewayInfo,
    ) -> GenericVerifyResult {
        tracing::debug!(
            "verifying witness from gateway: {:?}",
            PublicKeyBinary::from(witness_info.address.clone())
        );
        let beacon_report = &self.beacon_report;
        verify_self_witness(
            &beacon_report.report.pub_key,
            &witness_report.report.pub_key,
        )?;
        verify_entropy(
            self.entropy_start,
            self.entropy_end,
            witness_report.received_timestamp,
        )?;
        verify_witness_data(&beacon_report.report.data, &witness_report.report.data)?;
        verify_gw_location(witness_info.location)?;
        verify_witness_freq(
            beacon_report.report.frequency,
            witness_report.report.frequency,
        )?;
        verify_witness_region(beaconer_info.region, witness_info.region)?;
        verify_witness_distance(beaconer_info.location, witness_info.location)?;
        verify_witness_rssi(
            witness_report.report.signal,
            witness_report.report.frequency,
            beacon_report.report.tx_power,
            beaconer_info.gain,
            beaconer_info.location,
            witness_info.location,
        )?;
        tracing::debug!(
            "valid witness from gateway: {:?}",
            PublicKeyBinary::from(witness_info.address.clone())
        );
        Ok(())
    }

    async fn valid_witness_report(
        &self,
        witness_result: VerifyWitnessResult,
        witness_report: IotWitnessIngestReport,
    ) -> Result<IotValidWitnessReport, VerificationError> {
        let gw_info = witness_result
            .gateway_info
            .ok_or(VerificationError::NotFound(
                "expected gateway info not found",
            ))?;
        let hex_scale = witness_result
            .hex_scale
            .ok_or(VerificationError::NotFound("expected hex scale not found"))?;
        Ok(IotValidWitnessReport {
            received_timestamp: witness_report.received_timestamp,
            location: gw_info.location,
            hex_scale,
            report: witness_report.report,
            // default reward units to zero until we've got the full count of
            // valid, non-failed witnesses for the final validated poc report
            reward_unit: Decimal::ZERO,
        })
    }

    async fn invalid_witness_report(
        &self,
        witness_result: VerifyWitnessResult,
        witness_report: IotWitnessIngestReport,
    ) -> Result<IotInvalidWitnessReport, VerificationError> {
        let invalid_reason = witness_result
            .invalid_reason
            .ok_or(VerificationError::NotFound(
                "expected invalid_reason not found",
            ))?;
        Ok(IotInvalidWitnessReport {
            received_timestamp: witness_report.received_timestamp,
            reason: invalid_reason,
            report: witness_report.report,
            participant_side: InvalidParticipantSide::Witness,
        })
    }

    async fn failed_witness_report(
        &self,
        failed_reason: InvalidReason,
        witness_report: IotWitnessIngestReport,
    ) -> Result<IotInvalidWitnessReport, VerificationError> {
        Ok(IotInvalidWitnessReport {
            received_timestamp: witness_report.received_timestamp,
            reason: failed_reason,
            report: witness_report.report,
            participant_side: InvalidParticipantSide::Witness,
        })
    }
}

/// verify beaconer is permitted to beacon at this time
fn verify_beacon_schedule(
    last_beacon: &Option<LastBeacon>,
    beacon_received_ts: DateTime<Utc>,
) -> GenericVerifyResult {
    match last_beacon {
        Some(last_beacon) => {
            let interval_since_last_beacon = beacon_received_ts - last_beacon.timestamp;
            if interval_since_last_beacon < Duration::seconds(BEACON_INTERVAL) {
                tracing::debug!(
                    "beacon verification failed, reason:
                        IrregularInterval. Seconds since last beacon {:?}",
                    interval_since_last_beacon.num_seconds()
                );
                return Err(InvalidReason::IrregularInterval);
            }
        }
        None => {
            tracing::debug!("no last beacon timestamp available for this beaconer, ignoring ");
        }
    }
    Ok(())
}

/// verify remote entropy
/// if received timestamp is outside of entopy start/end then return invalid
fn verify_entropy(
    entropy_start: DateTime<Utc>,
    entropy_end: DateTime<Utc>,
    received_ts: DateTime<Utc>,
) -> GenericVerifyResult {
    if received_ts < entropy_start || received_ts > entropy_end {
        tracing::debug!(
            "report verification failed, reason: {:?}.
                received_ts: {:?},
                entropy_start_time: {:?},
                entropy_end_time: {:?}",
            InvalidReason::EntropyExpired,
            received_ts,
            entropy_start,
            entropy_end
        );
        return Err(InvalidReason::EntropyExpired);
    }
    Ok(())
}

/// verify gateway has an asserted location
fn verify_gw_location(gateway_loc: Option<u64>) -> GenericVerifyResult {
    match gateway_loc {
        Some(location) => location,
        None => {
            tracing::debug!(
                "beacon verification failed, reason: {:?}",
                InvalidReason::NotAsserted
            );
            return Err(InvalidReason::NotAsserted);
        }
    };
    Ok(())
}

/// verify gateway is permitted to participate in POC
fn verify_gw_capability(staking_mode: GatewayStakingMode) -> GenericVerifyResult {
    match staking_mode {
        GatewayStakingMode::Dataonly => {
            tracing::debug!(
                "witness verification failed, reason: {:?}. gateway staking mode: {:?}",
                InvalidReason::InvalidCapability,
                staking_mode
            );
            return Err(InvalidReason::InvalidCapability);
        }
        GatewayStakingMode::Full => (),
        GatewayStakingMode::Light => (),
    }
    Ok(())
}

/// verify witness report is not in response to its own beacon
fn verify_self_witness(
    beacon_pub_key: &PublicKeyBinary,
    witness_pub_key: &PublicKeyBinary,
) -> GenericVerifyResult {
    if witness_pub_key == beacon_pub_key {
        tracing::debug!(
            "witness verification failed, reason: {:?}",
            InvalidReason::SelfWitness
        );
        return Err(InvalidReason::SelfWitness);
    }
    Ok(())
}

/// verify witness is utilizing same freq and that of the beaconer
/// tolerance is 100Khz
fn verify_witness_freq(beacon_freq: u64, witness_freq: u64) -> GenericVerifyResult {
    if (beacon_freq.abs_diff(witness_freq) as i32) > 1000 * 100 {
        tracing::debug!(
            "witness verification failed, reason: {:?}. beaconer freq: {beacon_freq}, witness freq: {witness_freq}",
            InvalidReason::InvalidFrequency
        );
        return Err(InvalidReason::InvalidFrequency);
    }
    Ok(())
}

/// verify the witness is located in same region as beaconer
fn verify_witness_region(beacon_region: Region, witness_region: Region) -> GenericVerifyResult {
    if beacon_region != witness_region {
        tracing::debug!(
            "witness verification failed, reason: {:?}. beaconer region: {beacon_region}, witness region: {witness_region}",
            InvalidReason::InvalidRegion
        );
        return Err(InvalidReason::InvalidRegion);
    }
    Ok(())
}

/// verify witness does not exceed max distance from beaconer
fn verify_witness_distance(
    beacon_loc: Option<u64>,
    witness_loc: Option<u64>,
) -> GenericVerifyResult {
    // other verifications handle location checks but dont assume
    // we have a valid location passed in here
    // if no location for either beaconer or witness then default
    // this verification to a fail
    let l1 = beacon_loc.ok_or(InvalidReason::MaxDistanceExceeded)?;
    let l2 = witness_loc.ok_or(InvalidReason::MaxDistanceExceeded)?;
    let witness_distance = match calc_distance(l1, l2) {
        Ok(d) => d,
        Err(_) => return Err(InvalidReason::MaxDistanceExceeded),
    };
    if witness_distance.round() as i32 / 1000 > POC_DISTANCE_LIMIT {
        tracing::debug!(
            "witness verification failed, reason: {:?}. distance {witness_distance}",
            InvalidReason::MaxDistanceExceeded
        );
        return Err(InvalidReason::MaxDistanceExceeded);
    }
    Ok(())
}

/// verify witness rssi
fn verify_witness_rssi(
    witness_signal: i32,
    witness_freq: u64,
    beacon_tx_power: i32,
    beacon_gain: i32,
    beacon_loc: Option<u64>,
    witness_loc: Option<u64>,
) -> GenericVerifyResult {
    // other verifications handle location checks but dont assume
    // we have a valid location passed in here
    // if no location for either beaconer or witness or
    // distance between the two cannot be determined
    // then default this verification to a fail
    let l1 = beacon_loc.ok_or(InvalidReason::BadRssi)?;
    let l2 = witness_loc.ok_or(InvalidReason::BadRssi)?;
    let distance = match calc_distance(l1, l2) {
        Ok(d) => d,
        Err(_) => return Err(InvalidReason::BadRssi),
    };
    let min_rcv_signal = calc_fspl(beacon_tx_power, witness_freq, distance, beacon_gain);
    // signal is submitted as DBM * 10
    // min_rcv_signal    is plain old DBM
    if witness_signal / 10 > min_rcv_signal as i32 {
        tracing::debug!(
            "witness verification failed, reason: {:?}
            beaconer tx_power: {beacon_tx_power},
            beaconer gain: {beacon_gain},
            witness signal: {witness_signal},
            witness freq: {witness_freq},
            min_rcv_signal: {min_rcv_signal}",
            InvalidReason::BadRssi
        );
        return Err(InvalidReason::BadRssi);
    }
    Ok(())
}

//TODO: Plugin Jay's crate here when ready
/// verify witness reported data matches that of the beaconer
fn verify_witness_data(beacon_data: &Vec<u8>, witness_data: &Vec<u8>) -> GenericVerifyResult {
    if witness_data != beacon_data {
        tracing::debug!(
            "witness verification failed, reason: {:?}. witness_data: {:?}",
            InvalidReason::InvalidPacket,
            witness_data
        );
        return Err(InvalidReason::InvalidPacket);
    }
    Ok(())
}

fn calc_fspl(tx_power: i32, freq: u64, distance: f64, gain: i32) -> f64 {
    let gt = 0.0;
    let gl = gain as f64 / 10.0;
    let fpsl = (20.0 * (4.0 * PI * distance * (freq as f64) / C).log10()) - gt - gl;
    (tx_power as f64) - fpsl
}

#[derive(thiserror::Error, Debug)]
pub enum CalcDistanceError {
    #[error("convergence error: {0}")]
    ConvergenceError(#[from] FailedToConvergeError),
    #[error("h3ron error: {0}")]
    H3ronError(#[from] h3ron::Error),
}

fn calc_distance(p1: u64, p2: u64) -> Result<f64, CalcDistanceError> {
    let p1_cell = H3Cell::new(p1);
    let p2_cell = H3Cell::new(p2);
    let p1_coord = H3Cell::to_coordinate(&p1_cell)?;
    let p2_coord = H3Cell::to_coordinate(&p2_cell)?;

    let (p1_x, p1_y) = p1_coord.x_y();
    let (p2_x, p2_y) = p2_coord.x_y();
    let p1_geo = point!(x: p1_x, y: p1_y);
    let p2_geo = point!(x: p2_x, y: p2_y);
    let distance = p1_geo.vincenty_distance(&p2_geo)?;
    let adj_distance = distance - hex_adjustment(&p1_cell)? - hex_adjustment(&p2_cell)?;
    Ok(adj_distance.round())
}

fn hex_adjustment(loc: &H3Cell) -> Result<f64, h3ron::Error> {
    // Distance from hex center to edge, sqrt(3)*edge_length/2.
    let res = loc.resolution();
    let edge_length = H3DirectedEdge::edge_length_avg_m(res)?;
    Ok(
        edge_length * (f64::round(f64::sqrt(3.0) * f64::powf(10.0, 3.0)) / f64::powf(10.0, 3.0))
            / 2.0,
    )
}

fn invalid_reason_or_default(
    invalid_reason: Option<InvalidReason>,
    default: InvalidReason,
) -> InvalidReason {
    match invalid_reason {
        Some(reason) => reason,
        None => default,
    }
}

impl VerifyBeaconResult {
    pub fn new(
        result: VerificationStatus,
        invalid_reason: Option<InvalidReason>,
        gateway_info: Option<GatewayInfo>,
        hex_scale: Option<Decimal>,
    ) -> Self {
        Self {
            result,
            invalid_reason,
            gateway_info,
            hex_scale,
        }
    }

    pub fn valid(gateway_info: GatewayInfo, hex_scale: Decimal) -> Self {
        Self::new(
            VerificationStatus::Valid,
            None,
            Some(gateway_info),
            Some(hex_scale),
        )
    }

    pub fn invalid(invalid_reason: InvalidReason, gateway_info: GatewayInfo) -> Self {
        Self::new(
            VerificationStatus::Invalid,
            Some(invalid_reason),
            Some(gateway_info),
            None,
        )
    }

    pub fn gateway_not_found() -> Self {
        Self::new(
            VerificationStatus::Invalid,
            Some(InvalidReason::GatewayNotFound),
            None,
            None,
        )
    }

    pub fn scaling_factor_not_found(gateway_info: GatewayInfo) -> Self {
        Self::new(
            VerificationStatus::Invalid,
            Some(InvalidReason::ScalingFactorNotFound),
            Some(gateway_info),
            None,
        )
    }
}

impl VerifyWitnessResult {
    pub fn new(
        result: VerificationStatus,
        invalid_reason: Option<InvalidReason>,
        gateway_info: Option<GatewayInfo>,
        hex_scale: Option<Decimal>,
    ) -> Self {
        VerifyWitnessResult {
            result,
            invalid_reason,
            gateway_info,
            hex_scale,
        }
    }

    pub fn valid(gateway_info: GatewayInfo, hex_scale: Decimal) -> Self {
        Self::new(
            VerificationStatus::Valid,
            None,
            Some(gateway_info),
            Some(hex_scale),
        )
    }

    pub fn invalid(invalid_reason: InvalidReason, gateway_info: GatewayInfo) -> Self {
        Self::new(
            VerificationStatus::Invalid,
            Some(invalid_reason),
            Some(gateway_info),
            None,
        )
    }

    pub fn scaling_factor_not_found(gateway_info: GatewayInfo) -> Self {
        Self::new(
            VerificationStatus::Invalid,
            Some(InvalidReason::ScalingFactorNotFound),
            Some(gateway_info),
            None,
        )
    }

    pub fn gateway_not_found() -> Self {
        Self::new(
            VerificationStatus::Invalid,
            Some(InvalidReason::GatewayNotFound),
            None,
            None,
        )
    }

    pub fn not_asserted() -> Self {
        Self::new(
            VerificationStatus::Invalid,
            Some(InvalidReason::NotAsserted),
            None,
            None,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::last_beacon::LastBeacon;
    use chrono::Duration;
    use helium_proto::{services::poc_iot::InvalidReason, GatewayStakingMode, Region};
    use std::str::FromStr;

    #[test]
    fn test_calc_distance() {
        // location 1 is 51.51231394840223, -0.2919014665284206 ( ealing, london)
        // location 2 is 51.5649315958581, -0.10295780727096393 ( finsbury pk, london)
        // google maps distance is ~14.32km
        // converted co-ords to h3 index at resolution 15
        let loc1 = 644459695463521437;
        let loc2 = 644460986971331488;
        // get distance in meters between loc1 and loc2
        let gmaps_dist: i32 = 14320;
        let dist: i32 = calc_distance(loc1, loc2).unwrap() as i32;
        // verify the calculated distance is within 100m of the google maps distance
        assert!(100 > (gmaps_dist.abs_diff(dist) as i32));
    }

    #[test]
    fn test_calc_fspl() {
        //TODO: values here were taken from real work success and fail scenarios
        //      get someone in the know to verify
        let beacon1_tx_power = 27;
        let beacon1_gain = 80;
        let witness1_distance = 2011.0; //metres
        let witness1_freq = 904700032;
        let min_recv_signal = calc_fspl(
            beacon1_tx_power,
            witness1_freq,
            witness1_distance,
            beacon1_gain,
        );
        assert_eq!(-63.0, min_recv_signal.round());
    }

    #[test]
    fn test_verify_beacon_schedule() {
        let now = Utc::now();
        let id: &str = "test_id";
        let last_beacon_a = Some(LastBeacon {
            id: id.as_bytes().to_vec(),
            timestamp: now - Duration::seconds(60 * 12),
        });
        // last beacon was 12 mins in the past, expectation pass
        assert_eq!(Ok(()), verify_beacon_schedule(&last_beacon_a, now));
        //we dont have any last beacon data, expectation pass
        assert_eq!(Ok(()), verify_beacon_schedule(&None, now));
        // too soon after our last beacon, expectation fail
        assert_eq!(
            Err(InvalidReason::IrregularInterval),
            verify_beacon_schedule(&last_beacon_a, now - Duration::seconds(60 * 5))
        );
    }

    #[test]
    fn test_verify_entropy() {
        let now = Utc::now();
        let entropy_start = now - Duration::seconds(60);
        let entropy_end = now - Duration::seconds(10);
        assert_eq!(
            Ok(()),
            verify_entropy(entropy_start, entropy_end, now - Duration::seconds(30))
        );
        assert_eq!(
            Err(InvalidReason::EntropyExpired),
            verify_entropy(entropy_start, entropy_end, now - Duration::seconds(1))
        );
        assert_eq!(
            Err(InvalidReason::EntropyExpired),
            verify_entropy(entropy_start, entropy_end, now - Duration::seconds(65))
        );
    }

    #[test]
    fn test_verify_location() {
        let location = 631252734740306943;
        assert_eq!(Ok(()), verify_gw_location(Some(location)));
        assert_eq!(Err(InvalidReason::NotAsserted), verify_gw_location(None));
    }

    #[test]
    fn test_verify_capability() {
        assert_eq!(Ok(()), verify_gw_capability(GatewayStakingMode::Full));
        assert_eq!(Ok(()), verify_gw_capability(GatewayStakingMode::Light));
        assert_eq!(
            Err(InvalidReason::InvalidCapability),
            verify_gw_capability(GatewayStakingMode::Dataonly)
        );
    }

    #[test]
    fn test_verify_self_witness() {
        let key1 =
            PublicKeyBinary::from_str("112bUuQaE7j73THS9ABShHGokm46Miip9L361FSyWv7zSYn8hZWf")
                .unwrap();
        let key2 = PublicKeyBinary::from_str("11z69eJ3czc92k6snrfR9ek7g2uRWXosFbnG9v4bXgwhfUCivUo")
            .unwrap();
        assert_eq!(Ok(()), verify_self_witness(&key1, &key2));
        assert_eq!(
            Err(InvalidReason::SelfWitness),
            verify_self_witness(&key1, &key1)
        );
    }

    #[test]
    fn test_verify_witness_frequency() {
        let beacon_freq = 904499968;
        // test witness freqs with varying tolerances from beacon freq
        // under the tolerance level
        let witness1_freq = beacon_freq + (1000 * 90);
        // at the tolerance level
        let witness2_freq = beacon_freq + (1000 * 100);
        // over the tolerance level
        let witness3_freq = beacon_freq + (1000 * 110);
        assert_eq!(Ok(()), verify_witness_freq(beacon_freq, witness1_freq));
        assert_eq!(Ok(()), verify_witness_freq(beacon_freq, witness2_freq));
        assert_eq!(
            Err(InvalidReason::InvalidFrequency),
            verify_witness_freq(beacon_freq, witness3_freq)
        );
    }

    #[test]
    fn test_verify_witness_region() {
        let beacon_region = Region::Us915;
        let witness1_region = Region::Us915;
        let witness2_region = Region::Eu868;
        assert_eq!(
            Ok(()),
            verify_witness_region(beacon_region, witness1_region)
        );
        assert_eq!(
            Err(InvalidReason::InvalidRegion),
            verify_witness_region(beacon_region, witness2_region)
        );
    }

    #[test]
    fn test_verify_witness_distance() {
        let beacon_loc = 631615575095659519; // malta
        let witness1_loc = 631615575095699519; // malta and a lil out from the beaconer
        let witness2_loc = 631278052025960447; // armenia
        assert_eq!(
            Ok(()),
            verify_witness_distance(Some(beacon_loc), Some(witness1_loc))
        );
        assert_eq!(
            Err(InvalidReason::MaxDistanceExceeded),
            verify_witness_distance(Some(beacon_loc), Some(witness2_loc))
        );
    }

    #[test]
    fn test_verify_witness_rssi() {
        //TODO: values here were taken from real work success and fail scenarios
        //      get someone in the know to verify
        let beacon_loc = 631615575095659519; // malta
        let witness1_loc = 631615575095699519; // malta and a lil out from the beaconer
        let witness2_loc = 631278052025960447; // armenia

        let beacon1_tx_power = 27;
        let beacon1_gain = 80;
        let witness1_signal = -1060;
        let witness1_freq = 904700032;
        assert_eq!(
            Ok(()),
            verify_witness_rssi(
                witness1_signal,
                witness1_freq,
                beacon1_tx_power,
                beacon1_gain,
                Some(beacon_loc),
                Some(witness1_loc),
            )
        );

        let beacon2_tx_power = 27;
        let beacon2_gain = 12;
        let witness2_signal = -19;
        let witness2_freq = 904499968;
        assert_eq!(
            Err(InvalidReason::BadRssi),
            verify_witness_rssi(
                witness2_signal,
                witness2_freq,
                beacon2_tx_power,
                beacon2_gain,
                Some(beacon_loc),
                Some(witness2_loc),
            )
        );
    }

    #[test]
    fn test_verify_witness_data() {
        let beacon_data = "data1".as_bytes().to_vec();
        let witness1_data = "data1".as_bytes().to_vec();
        let witness2_data = "data2".as_bytes().to_vec();
        assert_eq!(Ok(()), verify_witness_data(&beacon_data, &witness1_data));
        assert_eq!(
            Err(InvalidReason::InvalidPacket),
            verify_witness_data(&beacon_data, &witness2_data)
        );
    }
}
