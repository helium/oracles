use crate::{
    entropy::ENTROPY_LIFESPAN,
    gateway_cache::GatewayCache,
    last_beacon::{LastBeacon, LastBeaconError},
};
use beacon;
use chrono::{DateTime, Duration, Utc};
use density_scaler::HexDensityMap;
use file_store::{
    iot_beacon_report::{IotBeaconIngestReport, IotBeaconReport},
    iot_invalid_poc::IotInvalidWitnessReport,
    iot_valid_poc::IotVerifiedWitnessReport,
    iot_witness_report::IotWitnessIngestReport,
};
use geo::{point, prelude::*, vincenty_distance::FailedToConvergeError};
use h3ron::{to_geo::ToCoordinate, H3Cell, H3DirectedEdge, Index};
use helium_crypto::PublicKeyBinary;
use helium_proto::{
    services::poc_lora::{InvalidParticipantSide, InvalidReason, VerificationStatus},
    BlockchainRegionParamV1, GatewayStakingMode, Region as ProtoRegion,
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

/// max permitted distance of a witness from a beaconer measured in KM
const POC_DISTANCE_LIMIT: u32 = 100;
/// the minimum distance in cells between a beaconer and witness
const POC_CELL_DISTANCE_MINIMUM: u32 = 8;
/// the resolution at which parent cell distance is derived
const POC_CELL_PARENT_RES: u8 = 11;

pub struct Poc {
    beacon_report: IotBeaconIngestReport,
    witness_reports: Vec<IotWitnessIngestReport>,
    entropy_start: DateTime<Utc>,
    entropy_end: DateTime<Utc>,
    entropy_version: i32,
}

pub struct VerifyBeaconResult {
    pub result: VerificationStatus,
    pub invalid_reason: InvalidReason,
    pub gateway_info: Option<GatewayInfo>,
    pub hex_scale: Option<Decimal>,
}

pub struct VerifyWitnessResult {
    result: VerificationStatus,
    invalid_reason: InvalidReason,
    pub gateway_info: Option<GatewayInfo>,
    hex_scale: Option<Decimal>,
    participant_side: InvalidParticipantSide,
}

pub struct VerifyWitnessesResult {
    pub verified_witnesses: Vec<IotVerifiedWitnessReport>,
    pub failed_witnesses: Vec<IotInvalidWitnessReport>,
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
        entropy_version: i32,
    ) -> Self {
        let entropy_end = entropy_start + Duration::seconds(ENTROPY_LIFESPAN);
        Self {
            beacon_report,
            witness_reports,
            entropy_start,
            entropy_end,
            entropy_version,
        }
    }

    pub async fn verify_beacon(
        &mut self,
        hex_density_map: impl HexDensityMap,
        gateway_cache: &GatewayCache,
        pool: &PgPool,
        beacon_interval: Duration,
        beacon_interval_tolerance: Duration,
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
            .do_beacon_verifications(
                last_beacon,
                &beaconer_info,
                beacon_interval,
                beacon_interval_tolerance,
            )
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
        let mut verified_witnesses: Vec<IotVerifiedWitnessReport> = Vec::new();
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
                Ok(witness_result) => match witness_result.result {
                    VerificationStatus::Valid => {
                        if let Ok(valid_witness) = self
                            .valid_witness_report(witness_result, witness_report)
                            .await
                        {
                            verified_witnesses.push(valid_witness)
                        };
                    }
                    VerificationStatus::Invalid => {
                        if let Ok(invalid_witness) = self
                            .invalid_witness_report(witness_result, witness_report)
                            .await
                        {
                            verified_witnesses.push(invalid_witness)
                        }
                    }
                },
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
            verified_witnesses,
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
                // to avoid assuming beaconer location is set and to avoid unwrap
                // we explicity match location here again
                let Some(beaconer_location) = beaconer_info.location else {
                    return Ok(VerifyWitnessResult::not_asserted(InvalidParticipantSide::Beaconer))
                };

                if let Some(hex_scale) = hex_density_map.get(beaconer_location).await {
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
        beacon_interval: Duration,
        beacon_interval_tolerance: Duration,
    ) -> GenericVerifyResult {
        tracing::debug!(
            "verifying beacon from beaconer: {:?}",
            PublicKeyBinary::from(beaconer_info.address.clone())
        );
        let beacon_received_ts = self.beacon_report.received_timestamp;
        verify_entropy(self.entropy_start, self.entropy_end, beacon_received_ts)?;
        verify_beacon_payload(
            &self.beacon_report.report,
            beaconer_info.region,
            &beaconer_info.region_params,
            beaconer_info.gain,
            self.entropy_start,
            self.entropy_version as u32,
        )?;
        verify_beacon_schedule(
            &last_beacon,
            beacon_received_ts,
            beacon_interval,
            beacon_interval_tolerance,
        )?;
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
        verify_witness_cell_distance(beaconer_info.location, witness_info.location)?;
        verify_witness_distance(beaconer_info.location, witness_info.location)?;
        verify_witness_rssi(
            witness_report.report.signal,
            witness_report.report.frequency,
            beacon_report.report.tx_power,
            beaconer_info.gain,
            witness_info.gain,
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
    ) -> Result<IotVerifiedWitnessReport, VerificationError> {
        let gw_info = witness_result
            .gateway_info
            .ok_or(VerificationError::NotFound(
                "expected gateway info not found",
            ))?;
        let hex_scale = witness_result
            .hex_scale
            .ok_or(VerificationError::NotFound("expected hex scale not found"))?;
        Ok(IotVerifiedWitnessReport {
            received_timestamp: witness_report.received_timestamp,
            status: witness_result.result,
            report: witness_report.report,
            location: gw_info.location,
            hex_scale,
            // default reward units to zero until we've got the full count of
            // valid, non-failed witnesses for the final validated poc report
            reward_unit: Decimal::ZERO,
            invalid_reason: InvalidReason::ReasonNone,
            participant_side: InvalidParticipantSide::SideNone,
        })
    }

    async fn invalid_witness_report(
        &self,
        witness_result: VerifyWitnessResult,
        witness_report: IotWitnessIngestReport,
    ) -> Result<IotVerifiedWitnessReport, VerificationError> {
        Ok(IotVerifiedWitnessReport {
            received_timestamp: witness_report.received_timestamp,
            status: witness_result.result,
            report: witness_report.report,
            location: None,
            hex_scale: Decimal::ZERO,
            // default reward units to zero until we've got the full count of
            // valid, non-failed witnesses for the final validated poc report
            reward_unit: Decimal::ZERO,
            invalid_reason: witness_result.invalid_reason,
            participant_side: witness_result.participant_side,
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
    beacon_interval: Duration,
    beacon_interval_tolerance: Duration,
) -> GenericVerifyResult {
    match last_beacon {
        Some(last_beacon) => {
            let interval_since_last_beacon = beacon_received_ts - last_beacon.timestamp;
            if interval_since_last_beacon < (beacon_interval - beacon_interval_tolerance) {
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

/// verify beacon construction
fn verify_beacon_payload(
    beacon_report: &IotBeaconReport,
    region: ProtoRegion,
    region_params: &[BlockchainRegionParamV1],
    gain: i32,
    entropy_start: DateTime<Utc>,
    entropy_version: u32,
) -> GenericVerifyResult {
    // cast some proto based structs to beacon structs
    let beacon_region: beacon::Region = region.into();
    // TODO: support RegionParams::new in gateway rs
    // avoid need to specify gain scale outside of region.rs
    let beacon_region_params = beacon::RegionParams {
        region: beacon_region,
        params: region_params.to_owned(),
        gain: Decimal::new(gain as i64, 1),
    };
    // generate a gateway rs beacon from the generated entropy and the beaconers region data
    let generated_beacon = generate_beacon(
        &beacon_region_params,
        entropy_start.timestamp_millis(),
        entropy_version,
        &beacon_report.local_entropy,
        &beacon_report.remote_entropy,
    )
    .map_err(|e| {
        tracing::warn!("failed to cast report to beacon, reason: {:?}", e);
        InvalidReason::InvalidPacket
    })?;

    // cast the received beaconers report into a beacon
    let reported_beacon: beacon::Beacon =
        match beacon_report.to_beacon(entropy_start, entropy_version) {
            Ok(res) => res,
            Err(e) => {
                tracing::warn!("failed to cast report to beacon, reason: {:?}", e);
                return Err(InvalidReason::InvalidPacket);
            }
        };

    // compare reports
    if reported_beacon != generated_beacon {
        tracing::debug!(
            "beacon verification failed, reason: {:?}",
            InvalidReason::InvalidPacket,
        );
        return Err(InvalidReason::InvalidPacket);
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
fn verify_witness_region(
    beacon_region: ProtoRegion,
    witness_region: ProtoRegion,
) -> GenericVerifyResult {
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
    if witness_distance / 1000 > POC_DISTANCE_LIMIT {
        tracing::debug!(
            "witness verification failed, reason: {:?}. distance {witness_distance}",
            InvalidReason::MaxDistanceExceeded
        );
        return Err(InvalidReason::MaxDistanceExceeded);
    }
    Ok(())
}

/// verify min hex distance between beaconer and witness
fn verify_witness_cell_distance(
    beacon_loc: Option<u64>,
    witness_loc: Option<u64>,
) -> GenericVerifyResult {
    // other verifications handle location checks but dont assume
    // we have a valid location passed in here
    // if no location for either beaconer or witness then default
    // this verification to a fail
    let l1 = beacon_loc.ok_or(InvalidReason::BelowMinDistance)?;
    let l2 = witness_loc.ok_or(InvalidReason::BelowMinDistance)?;
    let cell_distance = match calc_cell_distance(l1, l2) {
        Ok(d) => d,
        Err(_) => return Err(InvalidReason::BelowMinDistance),
    };
    if cell_distance < POC_CELL_DISTANCE_MINIMUM {
        tracing::debug!(
            "witness verification failed, reason: {:?}. cell distance {cell_distance}",
            InvalidReason::BelowMinDistance
        );
        return Err(InvalidReason::BelowMinDistance);
    }
    Ok(())
}

/// verify witness rssi
fn verify_witness_rssi(
    witness_signal: i32,
    witness_freq: u64,
    beacon_tx_power: i32,
    beacon_gain: i32,
    witness_gain: i32,
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
    let min_rcv_signal = calc_expected_rssi(
        beacon_tx_power,
        witness_freq,
        distance,
        beacon_gain,
        witness_gain,
    );
    // signal is submitted as DBM * 10
    // min_rcv_signal is plain old DBM
    if witness_signal as f64 / 10.0 > min_rcv_signal {
        tracing::debug!(
            "witness verification failed, reason: {:?}
            beaconer tx_power: {beacon_tx_power},
            beaconer gain: {beacon_gain},
            witness gain: {witness_gain},
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

fn calc_expected_rssi(
    conducted_tx_power_dbm: i32,
    freq: u64,
    distance_mtrs: u32,
    beaconer_gain_ddb: i32,
    witness_gain_ddb: i32,
) -> f64 {
    let beaconer_gain_db = beaconer_gain_ddb / 10;
    let witness_gain_db = witness_gain_ddb / 10;
    let fpsl = calc_fpsl(freq, distance_mtrs);
    conducted_tx_power_dbm as f64 + beaconer_gain_db as f64 - fpsl + witness_gain_db as f64
}

fn calc_fpsl(freq: u64, distance_mtrs: u32) -> f64 {
    20.0 * (4.0 * PI * distance_mtrs as f64 * (freq as f64) / C).log10()
}

#[derive(thiserror::Error, Debug)]
pub enum CalcDistanceError {
    #[error("convergence error: {0}")]
    ConvergenceError(#[from] FailedToConvergeError),
    #[error("h3ron error: {0}")]
    H3ronError(#[from] h3ron::Error),
}

fn calc_cell_distance(p1: u64, p2: u64) -> Result<u32, CalcDistanceError> {
    let p1_cell = H3Cell::new(p1);
    let p2_cell = H3Cell::new(p2);
    let source_parent = H3Cell::get_parent(&p1_cell, POC_CELL_PARENT_RES)?;
    let dest_parent = H3Cell::get_parent(&p2_cell, POC_CELL_PARENT_RES)?;
    let cell_distance = H3Cell::grid_distance_to(&source_parent, dest_parent)? as u32;
    Ok(cell_distance)
}

fn calc_distance(p1: u64, p2: u64) -> Result<u32, CalcDistanceError> {
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
    Ok(adj_distance.round() as u32)
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

fn generate_beacon(
    region_params: &beacon::RegionParams,
    entropy_start: i64,
    entropy_version: u32,
    local_entropy_data: &[u8],
    remote_entropy_data: &[u8],
) -> Result<beacon::Beacon, beacon::Error> {
    // generate a gateway rs beacon
    let remote_entropy = beacon::Entropy {
        timestamp: entropy_start,
        data: remote_entropy_data.to_vec(),
        version: entropy_version,
    };
    let local_entropy = beacon::Entropy {
        timestamp: 0, // local entroy timestamp is default to 0 on gateway rs side
        data: local_entropy_data.to_vec(),
        version: entropy_version,
    };
    let beacon = beacon::Beacon::new(remote_entropy, local_entropy, region_params)?;
    Ok(beacon)
}

impl VerifyBeaconResult {
    pub fn new(
        result: VerificationStatus,
        invalid_reason: InvalidReason,
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
            InvalidReason::ReasonNone,
            Some(gateway_info),
            Some(hex_scale),
        )
    }

    pub fn invalid(invalid_reason: InvalidReason, gateway_info: GatewayInfo) -> Self {
        Self::new(
            VerificationStatus::Invalid,
            invalid_reason,
            Some(gateway_info),
            None,
        )
    }

    pub fn gateway_not_found() -> Self {
        Self::new(
            VerificationStatus::Invalid,
            InvalidReason::GatewayNotFound,
            None,
            None,
        )
    }

    pub fn scaling_factor_not_found(gateway_info: GatewayInfo) -> Self {
        Self::new(
            VerificationStatus::Invalid,
            InvalidReason::ScalingFactorNotFound,
            Some(gateway_info),
            None,
        )
    }
}

impl VerifyWitnessResult {
    pub fn new(
        result: VerificationStatus,
        invalid_reason: InvalidReason,
        gateway_info: Option<GatewayInfo>,
        hex_scale: Option<Decimal>,
        participant_side: InvalidParticipantSide,
    ) -> Self {
        VerifyWitnessResult {
            result,
            invalid_reason,
            gateway_info,
            hex_scale,
            participant_side,
        }
    }

    pub fn valid(gateway_info: GatewayInfo, hex_scale: Decimal) -> Self {
        Self::new(
            VerificationStatus::Valid,
            InvalidReason::ReasonNone,
            Some(gateway_info),
            Some(hex_scale),
            InvalidParticipantSide::Witness,
        )
    }

    pub fn invalid(invalid_reason: InvalidReason, gateway_info: GatewayInfo) -> Self {
        Self::new(
            VerificationStatus::Invalid,
            invalid_reason,
            Some(gateway_info),
            None,
            InvalidParticipantSide::Witness,
        )
    }

    pub fn scaling_factor_not_found(gateway_info: GatewayInfo) -> Self {
        Self::new(
            VerificationStatus::Invalid,
            InvalidReason::ScalingFactorNotFound,
            Some(gateway_info),
            None,
            InvalidParticipantSide::Witness,
        )
    }

    pub fn gateway_not_found() -> Self {
        Self::new(
            VerificationStatus::Invalid,
            InvalidReason::GatewayNotFound,
            None,
            None,
            InvalidParticipantSide::Witness,
        )
    }

    pub fn not_asserted(participant_side: InvalidParticipantSide) -> Self {
        Self::new(
            VerificationStatus::Invalid,
            InvalidReason::NotAsserted,
            None,
            None,
            participant_side,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::last_beacon::LastBeacon;
    use chrono::Duration;
    use helium_proto::services::poc_lora;
    use std::str::FromStr;

    const EU868_PARAMS: &[u8] = &[
        10, 35, 8, 224, 202, 187, 157, 3, 16, 200, 208, 7, 24, 161, 1, 34, 20, 10, 4, 8, 6, 16, 65,
        10, 5, 8, 3, 16, 129, 1, 10, 5, 8, 2, 16, 238, 1, 10, 35, 8, 160, 229, 199, 157, 3, 16,
        200, 208, 7, 24, 161, 1, 34, 20, 10, 4, 8, 6, 16, 65, 10, 5, 8, 3, 16, 129, 1, 10, 5, 8, 2,
        16, 238, 1, 10, 35, 8, 224, 255, 211, 157, 3, 16, 200, 208, 7, 24, 161, 1, 34, 20, 10, 4,
        8, 6, 16, 65, 10, 5, 8, 3, 16, 129, 1, 10, 5, 8, 2, 16, 238, 1, 10, 35, 8, 160, 154, 224,
        157, 3, 16, 200, 208, 7, 24, 161, 1, 34, 20, 10, 4, 8, 6, 16, 65, 10, 5, 8, 3, 16, 129, 1,
        10, 5, 8, 2, 16, 238, 1, 10, 35, 8, 224, 180, 236, 157, 3, 16, 200, 208, 7, 24, 161, 1, 34,
        20, 10, 4, 8, 6, 16, 65, 10, 5, 8, 3, 16, 129, 1, 10, 5, 8, 2, 16, 238, 1, 10, 35, 8, 160,
        207, 248, 157, 3, 16, 200, 208, 7, 24, 161, 1, 34, 20, 10, 4, 8, 6, 16, 65, 10, 5, 8, 3,
        16, 129, 1, 10, 5, 8, 2, 16, 238, 1, 10, 35, 8, 224, 233, 132, 158, 3, 16, 200, 208, 7, 24,
        161, 1, 34, 20, 10, 4, 8, 6, 16, 65, 10, 5, 8, 3, 16, 129, 1, 10, 5, 8, 2, 16, 238, 1, 10,
        35, 8, 160, 132, 145, 158, 3, 16, 200, 208, 7, 24, 161, 1, 34, 20, 10, 4, 8, 6, 16, 65, 10,
        5, 8, 3, 16, 129, 1, 10, 5, 8, 2, 16, 238, 1,
    ];

    #[test]
    fn test_calc_distance() {
        // location 1 is 51.51231394840223, -0.2919014665284206 ( ealing, london)
        // location 2 is 51.5649315958581, -0.10295780727096393 ( finsbury pk, london)
        // google maps distance is ~14.32km
        // converted co-ords to h3 index at resolution 15
        let loc1 = 644459695463521437;
        let loc2 = 644460986971331488;
        // get distance in meters between loc1 and loc2
        let gmaps_dist: u32 = 14320;
        let dist = calc_distance(loc1, loc2).unwrap();
        // verify the calculated distance is within 100m of the google maps distance
        assert!(100 > (gmaps_dist.abs_diff(dist)));
    }

    #[test]
    fn test_calc_cell_distance() {
        // location 1 is 51.51231394840223, -0.2919014665284206 ( ealing, london)
        // location 2 is 51.5649315958581, -0.10295780727096393 ( finsbury pk, london)
        // google maps distance is ~14.32km
        // converted co-ords to h3 index at resolution 15
        let loc1 = 644459695463521437;
        let loc2 = 644460986971331488;
        let dist = calc_cell_distance(loc1, loc2).unwrap();
        // verify the calculated cell distance is more than min cell distance
        // the correct cell distance between the two locations at resolution 11 = 360
        assert_eq!(360, dist);
    }

    #[test]
    fn test_calc_expected_rssi() {
        //TODO: values here were taken from real work success and fail scenarios
        //      get someone in the know to verify
        let beacon1_tx_power = 12;
        let beacon1_gain = 81;
        let witness1_gain = 83;
        let witness1_distance = 508; //metres
        let witness1_freq = 867900024;
        let min_recv_signal = calc_expected_rssi(
            beacon1_tx_power,
            witness1_freq,
            witness1_distance,
            beacon1_gain,
            witness1_gain,
        );
        assert_eq!(-57.334232963418515, min_recv_signal);
    }

    #[test]
    fn test_verify_beacon_payload() {
        let pub_key =
            PublicKeyBinary::from_str("112bUuQaE7j73THS9ABShHGokm46Miip9L361FSyWv7zSYn8hZWf")
                .unwrap();
        let local_entropy_bytes: Vec<u8> = (0..50).map(|_| rand::random::<u8>()).collect();
        let remote_entropy_bytes: Vec<u8> = (0..50).map(|_| rand::random::<u8>()).collect();
        let entropy_start = Utc::now();
        let entropy_version = 1;
        let gain: i32 = 12;
        let region: ProtoRegion = ProtoRegion::Eu868;

        let region_params =
            beacon::RegionParams::from_bytes(region.into(), gain as u64, EU868_PARAMS)
                .expect("region params");

        let generated_beacon = generate_beacon(
            &region_params,
            entropy_start.timestamp_millis(),
            entropy_version,
            &local_entropy_bytes,
            &remote_entropy_bytes,
        )
        .unwrap();

        // cast the beacon in to a beacon report
        let mut lora_beacon_report =
            poc_lora::LoraBeaconReportReqV1::try_from(generated_beacon).unwrap();
        lora_beacon_report.pub_key = pub_key.try_into().unwrap();
        let iot_beacon_ingest_report: IotBeaconIngestReport =
            lora_beacon_report.try_into().unwrap();

        // assert the beacon created from the iot report is sane
        // the region params here should really come from gateway info
        // which is where they will be derived in the real world
        // but the test has no access to real gateway info
        // and so we just use the same region params we generated above
        // bit of a circular test but at least it verifies
        // the beacon -> report -> beacon flows
        assert!(verify_beacon_payload(
            &iot_beacon_ingest_report.report,
            region,
            &region_params.params,
            gain,
            entropy_start,
            entropy_version
        )
        .is_ok())
    }

    #[test]
    fn test_verify_beacon_schedule() {
        let now = Utc::now();
        let id: &str = "test_id";
        let beacon_interval = Duration::hours(6);
        let beacon_interval_tolerance = Duration::minutes(10);
        let last_beacon = Some(LastBeacon {
            id: id.as_bytes().to_vec(),
            timestamp: now - beacon_interval,
        });
        // last beacon was BEACON_INTERVAL in the past, expectation pass
        assert!(verify_beacon_schedule(
            &last_beacon,
            now,
            beacon_interval,
            beacon_interval_tolerance
        )
        .is_ok());
        // last beacon was BEACON_INTERVAL + 1hr in the past, expectation pass
        assert!(verify_beacon_schedule(
            &last_beacon,
            now + Duration::minutes(60),
            beacon_interval,
            beacon_interval_tolerance
        )
        .is_ok());
        // last beacon was BEACON_INTERVAL - 1 hr, too soon after our last beacon,
        // expectation fail
        assert_eq!(
            Err(InvalidReason::IrregularInterval),
            verify_beacon_schedule(
                &last_beacon,
                now - Duration::minutes(60),
                beacon_interval,
                beacon_interval_tolerance
            )
        );
        // last beacon was just outside of our tolerance period by 2 mins
        // therefore beacon too soon, expectation fail
        assert_eq!(
            Err(InvalidReason::IrregularInterval),
            verify_beacon_schedule(
                &last_beacon,
                now - (beacon_interval_tolerance + Duration::minutes(2)),
                beacon_interval,
                beacon_interval_tolerance
            )
        );
        // last beacon was just inside of our tolerance period by 2 mins
        // expectation pass
        assert!(verify_beacon_schedule(
            &last_beacon,
            now - (beacon_interval_tolerance - Duration::minutes(2)),
            beacon_interval,
            beacon_interval_tolerance
        )
        .is_ok());
        //we dont have any last beacon data, expectation pass
        assert!(
            verify_beacon_schedule(&None, now, beacon_interval, beacon_interval_tolerance).is_ok()
        );
    }

    #[test]
    fn test_verify_entropy() {
        let now = Utc::now();
        let entropy_start = now - Duration::seconds(60);
        let entropy_end = now - Duration::seconds(10);
        assert!(verify_entropy(entropy_start, entropy_end, now - Duration::seconds(30)).is_ok());
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
        assert!(verify_gw_location(Some(location)).is_ok());
        assert_eq!(Err(InvalidReason::NotAsserted), verify_gw_location(None));
    }

    #[test]
    fn test_verify_capability() {
        assert!(verify_gw_capability(GatewayStakingMode::Full).is_ok());
        assert!(verify_gw_capability(GatewayStakingMode::Light).is_ok());
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
        assert!(verify_self_witness(&key1, &key2).is_ok());
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

        assert!(verify_witness_freq(beacon_freq, witness1_freq).is_ok());
        assert!(verify_witness_freq(beacon_freq, witness2_freq).is_ok());
        assert_eq!(
            Err(InvalidReason::InvalidFrequency),
            verify_witness_freq(beacon_freq, witness3_freq)
        );
    }

    #[test]
    fn test_verify_witness_region() {
        let beacon_region = ProtoRegion::Us915;
        let witness1_region = ProtoRegion::Us915;
        let witness2_region = ProtoRegion::Eu868;
        assert!(verify_witness_region(beacon_region, witness1_region).is_ok());
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
        assert!(verify_witness_distance(Some(beacon_loc), Some(witness1_loc)).is_ok());
        assert_eq!(
            Err(InvalidReason::MaxDistanceExceeded),
            verify_witness_distance(Some(beacon_loc), Some(witness2_loc))
        );
    }

    #[test]
    fn test_verify_witness_cell_distance() {
        let beacon_loc = 631615575095659519; // malta
        let witness1_loc = 627111975468974079; // 7 cells away from beaconer
        let witness2_loc = 627111975465463807; // 28 cells away from beaconer

        // witness 1 location is 7 cells from the beaconer and thus invalid
        assert_eq!(
            Err(InvalidReason::BelowMinDistance),
            verify_witness_cell_distance(Some(beacon_loc), Some(witness1_loc))
        );
        // witness 2's location is 28 cells from the beaconer and thus valid
        assert!(verify_witness_cell_distance(Some(beacon_loc), Some(witness2_loc)).is_ok());
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
        let witness1_gain = 12;
        let witness1_signal = -1060;
        let witness1_freq = 904700032;
        assert!(verify_witness_rssi(
            witness1_signal,
            witness1_freq,
            beacon1_tx_power,
            beacon1_gain,
            witness1_gain,
            Some(beacon_loc),
            Some(witness1_loc),
        )
        .is_ok());
        let beacon2_tx_power = 27;
        let beacon2_gain = 12;
        let witness2_gain = 12;
        let witness2_signal = -19;
        let witness2_freq = 904499968;
        assert_eq!(
            Err(InvalidReason::BadRssi),
            verify_witness_rssi(
                witness2_signal,
                witness2_freq,
                beacon2_tx_power,
                beacon2_gain,
                witness2_gain,
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
        assert!(verify_witness_data(&beacon_data, &witness1_data).is_ok());
        assert_eq!(
            Err(InvalidReason::InvalidPacket),
            verify_witness_data(&beacon_data, &witness2_data)
        );
    }
}
