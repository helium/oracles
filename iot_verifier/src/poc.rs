use crate::{
    entropy::ENTROPY_LIFESPAN,
    gateway_cache::GatewayCache,
    gateway_cache::GatewayCacheError,
    hex_density::HexDensityMap,
    last_beacon::{LastBeacon, LastBeaconError},
    region_cache::{RegionCache, RegionCacheError},
};
use beacon;
use chrono::{DateTime, Duration, Utc};
use denylist::denylist::DenyList;
use file_store::{
    iot_beacon_report::{IotBeaconIngestReport, IotBeaconReport},
    iot_valid_poc::IotVerifiedWitnessReport,
    iot_witness_report::IotWitnessIngestReport,
};
use h3o::{CellIndex, LatLng, Resolution};
use helium_crypto::PublicKeyBinary;
use helium_proto::{
    services::poc_lora::{InvalidParticipantSide, InvalidReason, VerificationStatus},
    BlockchainRegionParamV1, Region as ProtoRegion,
};
use iot_config::gateway_info::{GatewayInfo, GatewayMetadata};
use lazy_static::lazy_static;
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
const POC_CELL_PARENT_RES: Resolution = Resolution::Eleven;

lazy_static! {
    /// Scaling factor when inactive gateway is not found in the tx scaling map (20%).
    /// A default tx scale is required to allow for inactive hotspots to become active
    /// again when an inactive hotspot's h3 index would otherwise be garbage-collected
    /// from density scaling calculations and not finding a value on subsequent lookups
    /// would disqualify the hotspot from validating further beacons
    static ref DEFAULT_TX_SCALE: Decimal = Decimal::new(2000, 4);
}

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

pub struct VerifyWitnessesResult {
    pub verified_witnesses: Vec<IotVerifiedWitnessReport>,
    pub failed_witnesses: Vec<IotWitnessIngestReport>,
}

#[derive(thiserror::Error, Debug)]
pub enum VerificationError {
    #[error("not found: {0}")]
    NotFound(&'static str),
    #[error("last beacon error: {0}")]
    LastBeaconError(#[from] LastBeaconError),
    #[error("calc distance error: {0}")]
    CalcDistanceError(#[from] CalcDistanceError),
    #[error("error querying gateway info from iot config service")]
    GatewayCache(#[from] GatewayCacheError),
    #[error("error querying region info from iot config service")]
    RegionCache(#[from] RegionCacheError),
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

    #[allow(clippy::too_many_arguments)]
    pub async fn verify_beacon(
        &mut self,
        hex_density_map: impl HexDensityMap,
        gateway_cache: &GatewayCache,
        region_cache: &RegionCache,
        pool: &PgPool,
        beacon_interval: Duration,
        beacon_interval_tolerance: Duration,
        deny_list: &DenyList,
    ) -> Result<VerifyBeaconResult, VerificationError> {
        let beacon = &self.beacon_report.report;
        let beaconer_pub_key = beacon.pub_key.clone();
        // get the beaconer info from our follower
        // it not available then declare beacon invalid
        let beaconer_info = match gateway_cache.resolve_gateway_info(&beaconer_pub_key).await {
            Ok(res) => res,
            Err(GatewayCacheError::GatewayNotFound(_)) => {
                return Ok(VerifyBeaconResult::gateway_not_found())
            }
        };
        let beaconer_metadata = match beaconer_info.metadata {
            Some(ref metadata) => metadata,
            None => {
                return Ok(VerifyBeaconResult::invalid(
                    InvalidReason::NotAsserted,
                    beaconer_info,
                ))
            }
        };
        let beaconer_region_info = match region_cache
            .resolve_region_info(beaconer_metadata.region)
            .await
        {
            Ok(res) => res,
            Err(err) => return Err(VerificationError::RegionCache(err)),
        };
        // we have beaconer info, proceed to verifications
        let last_beacon = LastBeacon::get(pool, beaconer_pub_key.as_ref()).await?;
        match do_beacon_verifications(
            deny_list,
            self.entropy_start,
            self.entropy_end,
            self.entropy_version,
            last_beacon,
            &self.beacon_report,
            &beaconer_info,
            &beaconer_region_info.region_params,
            beacon_interval,
            beacon_interval_tolerance,
        ) {
            Ok(()) => {
                let tx_scale = hex_density_map
                    .get(beaconer_metadata.location)
                    .await
                    .unwrap_or(*DEFAULT_TX_SCALE);
                Ok(VerifyBeaconResult::valid(beaconer_info, tx_scale))
            }
            Err(invalid_reason) => Ok(VerifyBeaconResult::invalid(invalid_reason, beaconer_info)),
        }
    }

    pub async fn verify_witnesses(
        &mut self,
        beacon_info: &GatewayInfo,
        hex_density_map: impl HexDensityMap,
        gateway_cache: &GatewayCache,
        deny_list: &DenyList,
    ) -> Result<VerifyWitnessesResult, VerificationError> {
        let mut verified_witnesses: Vec<IotVerifiedWitnessReport> = Vec::new();
        let mut failed_witnesses: Vec<IotWitnessIngestReport> = Vec::new();
        let mut existing_gateways: Vec<PublicKeyBinary> = Vec::new();
        let witnesses = self.witness_reports.clone();
        for witness_report in witnesses {
            // have we already processed a witness report from this gateway ?
            // if not, run verifications
            // if so, skip verifications and declare the report a dup
            if !existing_gateways.contains(&witness_report.report.pub_key) {
                // not a dup, run the verifications
                match self
                    .verify_witness(
                        deny_list,
                        &witness_report,
                        beacon_info,
                        gateway_cache,
                        &hex_density_map,
                    )
                    .await
                {
                    Ok(verified_witness) => {
                        // track which gateways we have saw a witness report from
                        existing_gateways.push(verified_witness.report.pub_key.clone());
                        verified_witnesses.push(verified_witness)
                    }
                    Err(_) => failed_witnesses.push(witness_report),
                }
            } else {
                // the report is a dup
                let dup_witness = IotVerifiedWitnessReport::invalid(
                    InvalidReason::Duplicate,
                    &witness_report.report,
                    witness_report.received_timestamp,
                    None,
                    // if location is None, default gain and elevation to zero
                    0,
                    0,
                    InvalidParticipantSide::Witness,
                );
                verified_witnesses.push(dup_witness)
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
        deny_list: &DenyList,
        witness_report: &IotWitnessIngestReport,
        beaconer_info: &GatewayInfo,
        gateway_cache: &GatewayCache,
        hex_density_map: &impl HexDensityMap,
    ) -> Result<IotVerifiedWitnessReport, VerificationError> {
        let witness = &witness_report.report;
        let witness_pub_key = witness.pub_key.clone();
        // pull the witness info from our follower
        let witness_info = match gateway_cache.resolve_gateway_info(&witness_pub_key).await {
            Ok(res) => res,
            Err(GatewayCacheError::GatewayNotFound(_)) => {
                return Ok(IotVerifiedWitnessReport::invalid(
                    InvalidReason::GatewayNotFound,
                    &witness_report.report,
                    witness_report.received_timestamp,
                    None,
                    // if location is None, default gain and elevation to zero
                    0,
                    0,
                    InvalidParticipantSide::Witness,
                ));
            }
        };
        let witness_metadata = match witness_info.metadata {
            Some(ref metadata) => metadata,
            None => {
                return Ok(IotVerifiedWitnessReport::invalid(
                    InvalidReason::NotAsserted,
                    &witness_report.report,
                    witness_report.received_timestamp,
                    None,
                    // if location is None, default gain and elevation to zero
                    0,
                    0,
                    InvalidParticipantSide::Witness,
                ));
            }
        };
        // to avoid assuming beaconer location is set and to avoid unwrap
        // we explicity match location here again
        let Some(ref beaconer_metadata) = beaconer_info.metadata else {
            return Ok(IotVerifiedWitnessReport::invalid(
                InvalidReason::NotAsserted,
                &witness_report.report,
                witness_report.received_timestamp,
                None,
                // if location is None, default gain and elevation to zero
                0,
                0,
                InvalidParticipantSide::Beaconer,
            ))
        };
        // run the witness verifications
        match do_witness_verifications(
            deny_list,
            self.entropy_start,
            self.entropy_end,
            witness_report,
            &witness_info,
            &self.beacon_report,
            beaconer_metadata,
        ) {
            Ok(()) => {
                let tx_scale = hex_density_map
                    .get(beaconer_metadata.location)
                    .await
                    .unwrap_or(*DEFAULT_TX_SCALE);
                Ok(IotVerifiedWitnessReport::valid(
                    &witness_report.report,
                    witness_report.received_timestamp,
                    Some(witness_metadata.location),
                    witness_metadata.gain,
                    witness_metadata.elevation,
                    tx_scale,
                ))
            }
            Err(invalid_reason) => Ok(IotVerifiedWitnessReport::invalid(
                invalid_reason,
                &witness_report.report,
                witness_report.received_timestamp,
                Some(beaconer_metadata.location),
                beaconer_metadata.gain,
                beaconer_metadata.elevation,
                InvalidParticipantSide::Witness,
            )),
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub fn do_beacon_verifications(
    deny_list: &DenyList,
    entropy_start: DateTime<Utc>,
    entropy_end: DateTime<Utc>,
    entropy_version: i32,
    last_beacon: Option<LastBeacon>,
    beacon_report: &IotBeaconIngestReport,
    beaconer_info: &GatewayInfo,
    beaconer_region_params: &[BlockchainRegionParamV1],
    beacon_interval: Duration,
    beacon_interval_tolerance: Duration,
) -> GenericVerifyResult {
    tracing::debug!(
        "verifying beacon from beaconer: {:?}",
        beaconer_info.address.clone()
    );
    let beacon_received_ts = beacon_report.received_timestamp;
    let beaconer_metadata = match beaconer_info.metadata {
        Some(ref metadata) => metadata,
        None => return Err(InvalidReason::NotAsserted),
    };
    verify_denylist(&beacon_report.report.pub_key, deny_list)?;
    verify_entropy(entropy_start, entropy_end, beacon_received_ts)?;
    verify_gw_capability(beaconer_info.is_full_hotspot)?;
    verify_beacon_schedule(
        &last_beacon,
        beacon_received_ts,
        beacon_interval,
        beacon_interval_tolerance,
    )?;
    verify_beacon_payload(
        &beacon_report.report,
        beaconer_metadata.region,
        beaconer_region_params,
        beaconer_metadata.gain,
        entropy_start,
        entropy_version as u32,
    )?;
    tracing::debug!(
        "valid beacon from beaconer: {:?}",
        beaconer_info.address.clone()
    );
    Ok(())
}

pub fn do_witness_verifications(
    deny_list: &DenyList,
    entropy_start: DateTime<Utc>,
    entropy_end: DateTime<Utc>,
    witness_report: &IotWitnessIngestReport,
    witness_info: &GatewayInfo,
    beacon_report: &IotBeaconIngestReport,
    beaconer_metadata: &GatewayMetadata,
) -> GenericVerifyResult {
    tracing::debug!(
        "verifying witness from gateway: {:?}",
        witness_info.address.clone()
    );
    let beacon_report = &beacon_report;
    let witness_metadata = match witness_info.metadata {
        Some(ref metadata) => metadata,
        None => return Err(InvalidReason::NotAsserted),
    };
    verify_denylist(&witness_report.report.pub_key, deny_list)?;
    verify_self_witness(
        &beacon_report.report.pub_key,
        &witness_report.report.pub_key,
    )?;
    verify_entropy(
        entropy_start,
        entropy_end,
        witness_report.received_timestamp,
    )?;
    verify_witness_data(&beacon_report.report.data, &witness_report.report.data)?;
    verify_gw_capability(witness_info.is_full_hotspot)?;
    verify_witness_freq(
        beacon_report.report.frequency,
        witness_report.report.frequency,
    )?;
    verify_witness_region(beaconer_metadata.region, witness_metadata.region)?;
    verify_witness_cell_distance(beaconer_metadata.location, witness_metadata.location)?;
    verify_witness_distance(beaconer_metadata.location, witness_metadata.location)?;
    verify_witness_rssi(
        witness_report.report.signal,
        witness_report.report.frequency,
        beacon_report.report.tx_power,
        beaconer_metadata.gain,
        witness_metadata.gain,
        beaconer_metadata.location,
        witness_metadata.location,
    )?;
    tracing::debug!(
        "valid witness from gateway: {:?}",
        witness_info.address.clone()
    );
    Ok(())
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

/// verify if gateway is on the deny list
fn verify_denylist(pub_key: &PublicKeyBinary, deny_list: &DenyList) -> GenericVerifyResult {
    if deny_list.check_key(pub_key) {
        tracing::debug!(
            "report verification failed, reason: {:?}.
            pubkey: {}",
            InvalidReason::Denied,
            pub_key,
        );
        return Err(InvalidReason::Denied);
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
    if received_ts.timestamp() < entropy_start.timestamp()
        || received_ts.timestamp() > entropy_end.timestamp()
    {
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
        entropy_start.timestamp(),
        entropy_version,
        &beacon_report.local_entropy,
        &beacon_report.remote_entropy,
    )
    .map_err(|e| {
        tracing::warn!(
            "failed to cast report to beacon, reason: {:?}, pub_key: {:?}",
            e,
            beacon_report.pub_key
        );
        InvalidReason::InvalidPacket
    })?;
    tracing::debug!("generated beacon {:?}", generated_beacon);

    // cast the received beaconers report into a beacon
    let reported_beacon: beacon::Beacon =
        match beacon_report.to_beacon(entropy_start, entropy_version) {
            Ok(res) => res,
            Err(e) => {
                tracing::warn!(
                    "failed to cast report to beacon, reason: {:?}, pub_key: {:?}",
                    e,
                    beacon_report.pub_key
                );
                return Err(InvalidReason::InvalidPacket);
            }
        };
    tracing::debug!("reported beacon {:?}", reported_beacon);

    // compare reports
    if !generated_beacon.verify(&reported_beacon) {
        tracing::debug!(
            "beacon construction verification failed, pubkey {:?}",
            beacon_report.pub_key,
        );
        return Err(InvalidReason::InvalidPacket);
    }
    Ok(())
}

/// verify gateway is permitted to participate in POC
fn verify_gw_capability(is_full_hotspot: bool) -> GenericVerifyResult {
    if !is_full_hotspot {
        tracing::debug!(
            "witness verification failed, reason: {:?}. is_full_hotspot: {:?}",
            InvalidReason::InvalidCapability,
            is_full_hotspot
        );
        return Err(InvalidReason::InvalidCapability);
    };
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
fn verify_witness_distance(beacon_loc: u64, witness_loc: u64) -> GenericVerifyResult {
    let witness_distance = match calc_distance(beacon_loc, witness_loc) {
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
fn verify_witness_cell_distance(beacon_loc: u64, witness_loc: u64) -> GenericVerifyResult {
    let cell_distance = match calc_cell_distance(beacon_loc, witness_loc) {
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
    beacon_loc: u64,
    witness_loc: u64,
) -> GenericVerifyResult {
    let distance = match calc_distance(beacon_loc, witness_loc) {
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
    #[error("h3 invalid cell: {0}")]
    H3CellError(#[from] h3o::error::InvalidCellIndex),
    #[error("h3 invalid parent")]
    H3ParentError,
    #[error("uncomputable hex grid distance")]
    H3DistanceError(#[from] h3o::error::LocalIjError),
}

fn calc_cell_distance(p1: u64, p2: u64) -> Result<u32, CalcDistanceError> {
    let p1_cell = CellIndex::try_from(p1)?;
    let p2_cell = CellIndex::try_from(p2)?;
    let source_parent = p1_cell
        .parent(POC_CELL_PARENT_RES)
        .ok_or(CalcDistanceError::H3ParentError)?;
    let dest_parent = p2_cell
        .parent(POC_CELL_PARENT_RES)
        .ok_or(CalcDistanceError::H3ParentError)?;
    let cell_distance = source_parent.grid_distance(dest_parent)? as u32;
    Ok(cell_distance)
}

fn calc_distance(p1: u64, p2: u64) -> Result<u32, CalcDistanceError> {
    let p1_cell = CellIndex::try_from(p1)?;
    let p2_cell = CellIndex::try_from(p2)?;
    let p1_latlng: LatLng = p1_cell.into();
    let p2_latlng: LatLng = p2_cell.into();
    Ok(p1_latlng.distance_m(p2_latlng).round() as u32)
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::last_beacon::LastBeacon;
    use chrono::{Duration, TimeZone};
    use denylist::DenyList;
    use file_store::iot_beacon_report::IotBeaconReport;
    use file_store::iot_witness_report::IotWitnessReport;
    use helium_proto::DataRate;
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

    const BEACONER_GAIN: u64 = 20;
    const LOC0: u64 = 631615575095659519; // malta
    const LOC1: u64 = 631615576056478207; // malta but a lil out from LOC0
    const LOC2: u64 = 631278052025960447; // armenia
    const LOC3: u64 = 627111975468974079; // 7 cells away from beaconer
    const LOC4: u64 = 627111975465463807; // 28 cells away from beaconer

    const PUBKEY1: &str = "112bUuQaE7j73THS9ABShHGokm46Miip9L361FSyWv7zSYn8hZWf";
    const PUBKEY2: &str = "11z69eJ3czc92k6snrfR9ek7g2uRWXosFbnG9v4bXgwhfUCivUo";
    const DENIED_PUBKEY1: &str = "112bUGwooPd1dCDd3h3yZwskjxCzBsQNKeaJTuUF4hSgYedcsFa9";

    // hardcode beacon & entropy data taken from a beacon generated on a hotspot
    const LOCAL_ENTROPY: [u8; 4] = [233, 70, 25, 176];
    const REMOTE_ENTROPY: [u8; 32] = [
        182, 170, 63, 128, 217, 53, 95, 19, 157, 153, 134, 38, 184, 209, 255, 23, 118, 205, 163,
        106, 225, 26, 16, 0, 106, 141, 81, 101, 70, 39, 107, 9,
    ];
    const POC_DATA: [u8; 51] = [
        28, 153, 18, 65, 96, 232, 59, 146, 134, 125, 99, 12, 175, 76, 158, 210, 28, 253, 146, 59,
        187, 203, 122, 146, 49, 241, 156, 148, 74, 246, 68, 17, 8, 212, 48, 6, 152, 58, 221, 158,
        186, 101, 37, 59, 135, 126, 18, 72, 244, 65, 174,
    ];
    const ENTROPY_VERSION: i32 = 0;
    const ENTROPY_TIMESTAMP: i64 = 1677163710000;

    #[test]
    fn test_calc_distance() {
        // location 1 is 51.51231394840223, -0.2919014665284206 ( ealing, london)
        // location 2 is 51.5649315958581, -0.10295780727096393 ( finsbury pk, london)
        // google maps distance is ~14.32km
        // converted co-ords to h3 index at resolution 15
        let loc1 = 644459695463521437;
        let loc2 = 644460986971331488;
        // get distance in meters between loc1 and loc2
        // for reference the google maps distance is ~14320m
        let dist = calc_distance(loc1, loc2).unwrap();
        assert_eq!(14318, dist);
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
    #[ignore]
    fn test_verify_beacon_payload() {
        // entropy comparisons are performed in secs but create a datetime with millisecs precision
        // confirm the millisecs precision is ignored
        let entropy_start = Utc.timestamp_millis_opt(ENTROPY_TIMESTAMP).unwrap();
        let received_ts = entropy_start + Duration::minutes(1);
        let gain: i32 = 60;
        let region: ProtoRegion = ProtoRegion::Eu868;

        let region_params =
            beacon::RegionParams::from_bytes(region.into(), gain as u64, EU868_PARAMS)
                .expect("region params");

        let generated_beacon = generate_beacon(
            &region_params,
            entropy_start.timestamp(),
            ENTROPY_VERSION as u32,
            &LOCAL_ENTROPY,
            &REMOTE_ENTROPY,
        )
        .unwrap();

        // confirm our generated beacon returns the expected data payload
        assert_eq!(POC_DATA.to_vec(), generated_beacon.data);

        // get a valid a beacon report in the form of an ingested beacon report
        let mut ingest_beacon_report = valid_beacon_report(PUBKEY1, received_ts);

        // assert the generated beacon report from the ingest report
        // matches our received report
        assert!(verify_beacon_payload(
            &ingest_beacon_report.report,
            region,
            &region_params.params,
            gain,
            entropy_start,
            ENTROPY_VERSION as u32
        )
        .is_ok());

        // modify the generated beacon to have a tx power > that that defined in
        // region params
        // this will be rendered invalid
        ingest_beacon_report.report.tx_power = 20;
        assert_eq!(
            Err(InvalidReason::InvalidPacket),
            verify_beacon_payload(
                &ingest_beacon_report.report,
                region,
                &region_params.params,
                gain,
                entropy_start,
                ENTROPY_VERSION as u32
            )
        );
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
    fn test_verify_denylist() {
        let deny_list: DenyList = vec![PublicKeyBinary::from_str(DENIED_PUBKEY1).unwrap()]
            .try_into()
            .unwrap();
        assert!(verify_denylist(&PublicKeyBinary::from_str(PUBKEY1).unwrap(), &deny_list).is_ok());
        assert_eq!(
            Err(InvalidReason::Denied),
            verify_denylist(
                &PublicKeyBinary::from_str(DENIED_PUBKEY1).unwrap(),
                &deny_list
            )
        );
    }

    #[test]
    fn test_verify_capability() {
        assert!(verify_gw_capability(true).is_ok());
        assert_eq!(
            Err(InvalidReason::InvalidCapability),
            verify_gw_capability(false)
        );
    }

    #[test]
    fn test_verify_self_witness() {
        let key1 = PublicKeyBinary::from_str(PUBKEY1).unwrap();
        let key2 = PublicKeyBinary::from_str(PUBKEY2).unwrap();
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
        let beacon_loc = LOC0;
        let witness1_loc = LOC1;
        let witness2_loc = LOC2;
        assert!(verify_witness_distance(beacon_loc, witness1_loc).is_ok());
        assert_eq!(
            Err(InvalidReason::MaxDistanceExceeded),
            verify_witness_distance(beacon_loc, witness2_loc)
        );
    }

    #[test]
    fn test_verify_witness_cell_distance() {
        let beacon_loc = LOC0;
        let witness1_loc = LOC3;
        let witness2_loc = LOC4;

        // witness 1 location is 7 cells from the beaconer and thus invalid
        assert_eq!(
            Err(InvalidReason::BelowMinDistance),
            verify_witness_cell_distance(beacon_loc, witness1_loc)
        );
        // witness 2's location is 28 cells from the beaconer and thus valid
        assert!(verify_witness_cell_distance(beacon_loc, witness2_loc).is_ok());
    }

    #[test]
    fn test_verify_witness_rssi() {
        let beacon_loc = LOC0;
        let witness1_loc = LOC1;
        let witness2_loc = LOC2;

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
            beacon_loc,
            witness1_loc,
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
                beacon_loc,
                witness2_loc,
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

    #[test]
    fn test_beacon_verification_list() {
        // this test sets up a bunch of invalid beacons and for each
        // calls do_beacon_verifications
        // in order to assert the presence of each expected verification
        // by confirming the beacon report is rendered as invalid
        // asserting the presence of each will guard against
        // one of more verifications being accidently removed
        // from `do_beacon_verifications`

        // create default data structs
        let beaconer_info = beaconer_gateway_info(Some(LOC0), ProtoRegion::Eu868, true);
        let entropy_start = Utc.timestamp_millis_opt(ENTROPY_TIMESTAMP).unwrap();
        let entropy_end = entropy_start + Duration::minutes(3);
        let beacon_interval = Duration::minutes(5);
        let beacon_interval_tolerance = Duration::seconds(60);
        let deny_list: DenyList = vec![PublicKeyBinary::from_str(DENIED_PUBKEY1).unwrap()]
            .try_into()
            .unwrap();

        // test deny list verification is active in the beacon validation list
        let beacon_report1 =
            valid_beacon_report(DENIED_PUBKEY1, entropy_start + Duration::minutes(4));
        let resp1 = do_beacon_verifications(
            &deny_list,
            entropy_start,
            entropy_end,
            ENTROPY_VERSION,
            None,
            &beacon_report1,
            &beaconer_info,
            &default_region_params(),
            beacon_interval,
            beacon_interval_tolerance,
        );
        assert_eq!(Err(InvalidReason::Denied), resp1);

        // test entropy lifepsan verification is active in the beacon validation list
        let beacon_report1 = valid_beacon_report(PUBKEY1, entropy_start + Duration::minutes(4));
        let resp1 = do_beacon_verifications(
            &deny_list,
            entropy_start,
            entropy_end,
            ENTROPY_VERSION,
            None,
            &beacon_report1,
            &beaconer_info,
            &default_region_params(),
            beacon_interval,
            beacon_interval_tolerance,
        );
        assert_eq!(Err(InvalidReason::EntropyExpired), resp1);

        // test location verification is active in the beacon validation list
        let beacon_report2 = valid_beacon_report(PUBKEY1, entropy_start + Duration::minutes(2));
        let beacon_info2 = beaconer_gateway_info(None, ProtoRegion::Eu868, true);
        let resp2 = do_beacon_verifications(
            &deny_list,
            entropy_start,
            entropy_end,
            ENTROPY_VERSION,
            None,
            &beacon_report2,
            &beacon_info2,
            &default_region_params(),
            beacon_interval,
            beacon_interval_tolerance,
        );
        assert_eq!(Err(InvalidReason::NotAsserted), resp2);

        // test schedule verification is active in the beacon validation list
        let beacon_report3 = valid_beacon_report(PUBKEY1, entropy_start + Duration::minutes(2));
        let last_beacon3 = LastBeacon {
            id: vec![],
            timestamp: Utc::now() - Duration::minutes(5),
        };
        let resp3 = do_beacon_verifications(
            &deny_list,
            entropy_start,
            entropy_end,
            ENTROPY_VERSION,
            Some(last_beacon3),
            &beacon_report3,
            &beaconer_info,
            &default_region_params(),
            beacon_interval,
            beacon_interval_tolerance,
        );
        assert_eq!(Err(InvalidReason::IrregularInterval), resp3);

        // test capability verification is active in the beacon validation list
        let beacon_report4 = valid_beacon_report(PUBKEY1, entropy_start + Duration::minutes(2));
        let beacon_info4 = beaconer_gateway_info(Some(LOC0), ProtoRegion::Eu868, false);
        let resp4 = do_beacon_verifications(
            &deny_list,
            entropy_start,
            entropy_end,
            ENTROPY_VERSION,
            None,
            &beacon_report4,
            &beacon_info4,
            &default_region_params(),
            beacon_interval,
            beacon_interval_tolerance,
        );
        assert_eq!(Err(InvalidReason::InvalidCapability), resp4);

        // test beacon construction verification is active in the beacon validation list
        let beacon_report5 = invalid_beacon_bad_payload(entropy_start + Duration::minutes(2));
        let resp5 = do_beacon_verifications(
            &deny_list,
            entropy_start,
            entropy_end,
            ENTROPY_VERSION,
            None,
            &beacon_report5,
            &beaconer_info,
            &default_region_params(),
            beacon_interval,
            beacon_interval_tolerance,
        );
        assert_eq!(Err(InvalidReason::InvalidPacket), resp5);

        // for completeness, confirm our valid beacon report is sane
        let beacon_report6 = valid_beacon_report(PUBKEY1, entropy_start + Duration::minutes(2));
        let resp6 = do_beacon_verifications(
            &deny_list,
            entropy_start,
            entropy_end,
            ENTROPY_VERSION,
            None,
            &beacon_report6,
            &beaconer_info,
            &default_region_params(),
            beacon_interval,
            beacon_interval_tolerance,
        );
        assert_eq!(Ok(()), resp6);
    }

    #[test]
    fn test_witness_verification_list() {
        // this test sets up a bunch of invalid witnesses and for each
        // calls do_witness_verifications
        // in order to assert the presence of each expected verification
        // by confirming the witness report is rendered as invalid
        // asserting the presence of each will guard against
        // one of more verifications being accidently removed
        // from `do_witness_verifications`

        // create default data structs
        let beacon_report = valid_beacon_report(PUBKEY1, Utc::now() - Duration::minutes(2));
        let beaconer_info = beaconer_gateway_info(Some(LOC0), ProtoRegion::Eu868, true);
        let beaconer_metadata = beaconer_info
            .metadata
            .expect("beaconer should have metadata");
        let witness_info = witness_gateway_info(Some(LOC4), ProtoRegion::Eu868, true);
        let entropy_start = Utc.timestamp_millis_opt(1676381847900).unwrap();
        let entropy_end = entropy_start + Duration::minutes(3);
        let deny_list: DenyList = vec![PublicKeyBinary::from_str(DENIED_PUBKEY1).unwrap()]
            .try_into()
            .unwrap();

        // test self witness verification is active in the witness validation list
        let witness_report1 = invalid_witness_self_witness(entropy_start + Duration::minutes(2));
        let resp1 = do_witness_verifications(
            &deny_list,
            entropy_start,
            entropy_end,
            &witness_report1,
            &witness_info,
            &beacon_report,
            &beaconer_metadata,
        );
        assert_eq!(Err(InvalidReason::SelfWitness), resp1);

        // test entropy lifepsan verification is active in the witness validation list
        let witness_report2 = valid_witness_report(PUBKEY2, entropy_start + Duration::minutes(5));
        let resp2 = do_witness_verifications(
            &deny_list,
            entropy_start,
            entropy_end,
            &witness_report2,
            &witness_info,
            &beacon_report,
            &beaconer_metadata,
        );
        assert_eq!(Err(InvalidReason::EntropyExpired), resp2);

        // test witness packet data verification is active in the witness validation list
        let witness_report3 = invalid_witness_bad_data(entropy_start + Duration::minutes(2));
        let resp3 = do_witness_verifications(
            &deny_list,
            entropy_start,
            entropy_end,
            &witness_report3,
            &witness_info,
            &beacon_report,
            &beaconer_metadata,
        );
        assert_eq!(Err(InvalidReason::InvalidPacket), resp3);

        // test location verification is active in the witness validation list
        let witness_report4 = valid_witness_report(PUBKEY2, entropy_start + Duration::minutes(2));
        let witness_info4 = witness_gateway_info(None, ProtoRegion::Eu868, true);
        let resp4 = do_witness_verifications(
            &deny_list,
            entropy_start,
            entropy_end,
            &witness_report4,
            &witness_info4,
            &beacon_report,
            &beaconer_metadata,
        );
        assert_eq!(Err(InvalidReason::NotAsserted), resp4);

        // test witness frequency verification is active in the witness validation list
        let witness_report5 = invalid_witness_bad_freq(entropy_start + Duration::minutes(2));
        let resp5 = do_witness_verifications(
            &deny_list,
            entropy_start,
            entropy_end,
            &witness_report5,
            &witness_info,
            &beacon_report,
            &beaconer_metadata,
        );
        assert_eq!(Err(InvalidReason::InvalidFrequency), resp5);

        // test witness region verification is active in the witness validation list
        let witness_report6 = valid_witness_report(PUBKEY2, entropy_start + Duration::minutes(2));
        let witness_info6 = witness_gateway_info(Some(LOC1), ProtoRegion::Us915, true);
        let resp6 = do_witness_verifications(
            &deny_list,
            entropy_start,
            entropy_end,
            &witness_report6,
            &witness_info6,
            &beacon_report,
            &beaconer_metadata,
        );
        assert_eq!(Err(InvalidReason::InvalidRegion), resp6);

        // test witness min cell distance verification is active in the witness validation list
        let witness_report7 = valid_witness_report(PUBKEY2, entropy_start + Duration::minutes(2));
        let witness_info7 = witness_gateway_info(Some(LOC3), ProtoRegion::Eu868, true);
        let resp7 = do_witness_verifications(
            &deny_list,
            entropy_start,
            entropy_end,
            &witness_report7,
            &witness_info7,
            &beacon_report,
            &beaconer_metadata,
        );
        assert_eq!(Err(InvalidReason::BelowMinDistance), resp7);

        // test witness max distance verification is active in the witness validation list
        let witness_report8 = valid_witness_report(PUBKEY2, entropy_start + Duration::minutes(2));
        let witness_info8 = witness_gateway_info(Some(LOC2), ProtoRegion::Eu868, true);
        let resp8 = do_witness_verifications(
            &deny_list,
            entropy_start,
            entropy_end,
            &witness_report8,
            &witness_info8,
            &beacon_report,
            &beaconer_metadata,
        );
        assert_eq!(Err(InvalidReason::MaxDistanceExceeded), resp8);

        // test witness rssi verification is active in the witness validation list
        let witness_report9 = invalid_witness_bad_rssi(entropy_start + Duration::minutes(2));
        let resp9 = do_witness_verifications(
            &deny_list,
            entropy_start,
            entropy_end,
            &witness_report9,
            &witness_info,
            &beacon_report,
            &beaconer_metadata,
        );
        assert_eq!(Err(InvalidReason::BadRssi), resp9);

        // test witness capability verification is active in the witness validation list
        let witness_report10 = valid_witness_report(PUBKEY2, entropy_start + Duration::minutes(2));
        let witness_info10 = witness_gateway_info(Some(LOC4), ProtoRegion::Eu868, false);
        let resp10 = do_witness_verifications(
            &deny_list,
            entropy_start,
            entropy_end,
            &witness_report10,
            &witness_info10,
            &beacon_report,
            &beaconer_metadata,
        );
        assert_eq!(Err(InvalidReason::InvalidCapability), resp10);

        // for completeness, confirm our valid witness report is sane
        let witness_report11 = valid_witness_report(PUBKEY2, entropy_start + Duration::minutes(2));
        let witness_info11 = witness_gateway_info(Some(LOC4), ProtoRegion::Eu868, true);
        let resp11 = do_witness_verifications(
            &deny_list,
            entropy_start,
            entropy_end,
            &witness_report11,
            &witness_info11,
            &beacon_report,
            &beaconer_metadata,
        );
        assert_eq!(Ok(()), resp11);
    }

    fn beaconer_gateway_info(
        location: Option<u64>,
        region: ProtoRegion,
        is_full_hotspot: bool,
    ) -> GatewayInfo {
        let metadata = location.map(|location| GatewayMetadata {
            location,
            gain: 12,
            elevation: 100,
            region,
        });
        GatewayInfo {
            address: PublicKeyBinary::from_str(PUBKEY1).unwrap(),
            is_full_hotspot,
            metadata,
        }
    }

    fn witness_gateway_info(
        location: Option<u64>,
        region: ProtoRegion,
        is_full_hotspot: bool,
    ) -> GatewayInfo {
        let metadata = location.map(|location| GatewayMetadata {
            location,
            gain: 20,
            elevation: 100,
            region,
        });
        GatewayInfo {
            address: PublicKeyBinary::from_str(PUBKEY2).unwrap(),
            is_full_hotspot,
            metadata,
        }
    }

    fn default_region_params() -> Vec<BlockchainRegionParamV1> {
        let region_params = beacon::RegionParams::from_bytes(
            ProtoRegion::Eu868.into(),
            BEACONER_GAIN,
            EU868_PARAMS,
        )
        .unwrap();
        region_params.params
    }

    fn valid_beacon_report(
        pubkey: &str,
        received_timestamp: DateTime<Utc>,
    ) -> IotBeaconIngestReport {
        beacon_report_to_ingest_report(
            IotBeaconReport {
                pub_key: PublicKeyBinary::from_str(pubkey).unwrap(),
                local_entropy: LOCAL_ENTROPY.to_vec(),
                remote_entropy: REMOTE_ENTROPY.to_vec(),
                data: POC_DATA.to_vec(),
                frequency: 867900000,
                channel: 0,
                datarate: DataRate::Sf12bw125,
                tx_power: 8,
                timestamp: Utc::now(),
                signature: vec![],
                tmst: 0,
            },
            received_timestamp,
        )
    }

    fn invalid_beacon_bad_payload(received_timestamp: DateTime<Utc>) -> IotBeaconIngestReport {
        let mut report = valid_beacon_report(PUBKEY1, received_timestamp);
        report.report.data = [
            138, 139, 152, 130, 179, 215, 179, 238, 75, 167, 66, 182, 209, 87, 168, 137, 192,
        ]
        .to_vec();
        report
    }

    fn valid_witness_report(
        pubkey: &str,
        received_timestamp: DateTime<Utc>,
    ) -> IotWitnessIngestReport {
        witness_report_to_ingest_report(
            IotWitnessReport {
                pub_key: PublicKeyBinary::from_str(pubkey).unwrap(),
                data: POC_DATA.to_vec(),
                timestamp: Utc::now(),
                tmst: 0,
                signal: -1080,
                snr: 35,
                frequency: 867900032,
                datarate: DataRate::Sf12bw125,
                signature: vec![],
            },
            received_timestamp,
        )
    }

    // each invalid below overrides a field of a valid report which
    // will render it invalid in the desired way
    fn invalid_witness_self_witness(received_timestamp: DateTime<Utc>) -> IotWitnessIngestReport {
        let mut report = valid_witness_report(PUBKEY2, received_timestamp);
        report.report.pub_key = PublicKeyBinary::from_str(PUBKEY1).unwrap();
        report
    }
    fn invalid_witness_bad_data(received_timestamp: DateTime<Utc>) -> IotWitnessIngestReport {
        let mut report = valid_witness_report(PUBKEY2, received_timestamp);
        report.report.data = [
            138, 139, 152, 130, 179, 215, 179, 238, 75, 167, 66, 182, 209, 87, 168, 137, 192,
        ]
        .to_vec();
        report
    }
    fn invalid_witness_bad_freq(received_timestamp: DateTime<Utc>) -> IotWitnessIngestReport {
        let mut report = valid_witness_report(PUBKEY2, received_timestamp);
        report.report.frequency = 867100000;
        report
    }
    fn invalid_witness_bad_rssi(received_timestamp: DateTime<Utc>) -> IotWitnessIngestReport {
        let mut report = valid_witness_report(PUBKEY2, received_timestamp);
        report.report.signal = 300;
        report
    }

    fn beacon_report_to_ingest_report(
        report: IotBeaconReport,
        received_timestamp: DateTime<Utc>,
    ) -> IotBeaconIngestReport {
        IotBeaconIngestReport {
            received_timestamp,
            report,
        }
    }
    fn witness_report_to_ingest_report(
        report: IotWitnessReport,
        received_timestamp: DateTime<Utc>,
    ) -> IotWitnessIngestReport {
        IotWitnessIngestReport {
            received_timestamp,
            report,
        }
    }
}
