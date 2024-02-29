use crate::{
    entropy::ENTROPY_LIFESPAN,
    gateway_cache::{GatewayCache, GatewayCacheError},
    hex_density::HexDensityMap,
    last_beacon::LastBeacon,
    last_witness::LastWitness,
    region_cache::RegionCache,
    witness_updater::WitnessUpdater,
};
use beacon;
use chrono::{DateTime, Duration, DurationRound, Utc};
use denylist::denylist::DenyList;
use file_store::{
    iot_beacon_report::{IotBeaconIngestReport, IotBeaconReport},
    iot_valid_poc::IotVerifiedWitnessReport,
    iot_witness_report::IotWitnessIngestReport,
};
use h3o::{CellIndex, LatLng, Resolution};
use helium_crypto::PublicKeyBinary;
use helium_proto::{
    services::poc_lora::{
        invalid_details, InvalidDetails, InvalidParticipantSide, InvalidReason, VerificationStatus,
    },
    BlockchainRegionParamV1, Region as ProtoRegion,
};
use iot_config::{
    client::Gateways,
    gateway_info::{GatewayInfo, GatewayMetadata},
};
use lazy_static::lazy_static;
use rust_decimal::Decimal;
use sqlx::PgPool;
use std::f64::consts::PI;

pub type GenericVerifyResult<T = ()> = Result<T, InvalidResponse>;

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
    /// max permitted lag between the first witness and all subsequent witnesses
    static ref MAX_WITNESS_LAG: Duration = Duration::milliseconds(1500);
    /// max permitted lag between the beaconer and a witness
    static ref MAX_BEACON_TO_WITNESS_LAG: Duration = Duration::milliseconds(4000);
    /// the duration in which a beaconer or witness must have a valid opposite report from
    static ref RECIPROCITY_WINDOW: Duration = Duration::hours(48);

}
#[derive(Debug, PartialEq)]
pub struct InvalidResponse {
    reason: InvalidReason,
    details: Option<InvalidDetails>,
}

pub struct Poc {
    pool: PgPool,
    beacon_interval: Duration,
    beacon_report: IotBeaconIngestReport,
    witness_reports: Vec<IotWitnessIngestReport>,
    entropy_start: DateTime<Utc>,
    entropy_end: DateTime<Utc>,
    entropy_version: i32,
}
#[derive(Clone, Debug)]
pub struct VerifyBeaconResult {
    pub result: VerificationStatus,
    pub invalid_reason: InvalidReason,
    pub invalid_details: Option<InvalidDetails>,
    pub gateway_info: Option<GatewayInfo>,
    pub hex_scale: Option<Decimal>,
}

#[derive(Clone, Debug)]
pub struct VerifyWitnessesResult {
    pub verified_witnesses: Vec<IotVerifiedWitnessReport>,
    pub failed_witnesses: Vec<IotWitnessIngestReport>,
}

impl Poc {
    pub async fn new(
        pool: PgPool,
        beacon_interval: Duration,
        beacon_report: IotBeaconIngestReport,
        witness_reports: Vec<IotWitnessIngestReport>,
        entropy_start: DateTime<Utc>,
        entropy_version: i32,
    ) -> Self {
        let entropy_end = entropy_start + Duration::seconds(ENTROPY_LIFESPAN);
        Self {
            pool,
            beacon_interval,
            beacon_report,
            witness_reports,
            entropy_start,
            entropy_end,
            entropy_version,
        }
    }

    pub async fn verify_beacon<G>(
        &mut self,
        hex_density_map: &HexDensityMap,
        gateway_cache: &GatewayCache,
        region_cache: &RegionCache<G>,
        deny_list: &DenyList,
        witness_updater: &WitnessUpdater,
    ) -> anyhow::Result<VerifyBeaconResult>
    where
        G: Gateways,
    {
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
                    None,
                    beaconer_info,
                ))
            }
        };
        let beaconer_region_info = match region_cache
            .resolve_region_info(beaconer_metadata.region)
            .await
        {
            Ok(res) => res,
            Err(err) => return Err(anyhow::Error::from(err)),
        };
        // we have beaconer info, proceed to verifications
        let last_beacon = LastBeacon::get(&self.pool, &beaconer_pub_key).await?;
        match do_beacon_verifications(
            deny_list,
            self.entropy_start,
            self.entropy_end,
            self.entropy_version,
            last_beacon,
            &self.beacon_report,
            &beaconer_info,
            &beaconer_region_info.region_params,
            self.beacon_interval,
        ) {
            Ok(()) => {
                let tx_scale = hex_density_map
                    .get(beaconer_metadata.location)
                    .await
                    .unwrap_or(*DEFAULT_TX_SCALE);
                // update 'last beacon' timestamp if the beacon has passed regular validations
                // but only if there has been at least one witness report
                if !self.witness_reports.is_empty() {
                    LastBeacon::update_last_timestamp(
                        &self.pool,
                        &beaconer_pub_key,
                        self.beacon_report.received_timestamp,
                    )
                    .await?;
                }

                // post regular validations, check for beacon reciprocity
                // if this check fails we will invalidate the beacon
                // even tho it has passed all regular validations
                if !self.verify_beacon_reciprocity(witness_updater).await? {
                    Ok(VerifyBeaconResult::invalid(
                        InvalidReason::GatewayNoValidWitnesses,
                        None,
                        beaconer_info,
                    ))
                } else {
                    Ok(VerifyBeaconResult::valid(beaconer_info, tx_scale))
                }
            }
            Err(invalid_response) => Ok(VerifyBeaconResult::invalid(
                invalid_response.reason,
                invalid_response.details,
                beaconer_info,
            )),
        }
    }

    pub async fn verify_witnesses(
        &mut self,
        beacon_info: &GatewayInfo,
        hex_density_map: &HexDensityMap,
        gateway_cache: &GatewayCache,
        deny_list: &DenyList,
        witness_updater: &WitnessUpdater,
    ) -> anyhow::Result<VerifyWitnessesResult> {
        let mut witnesses_to_update: Vec<LastWitness> = Vec::new();
        let mut verified_witnesses: Vec<IotVerifiedWitnessReport> = Vec::new();
        let mut failed_witnesses: Vec<IotWitnessIngestReport> = Vec::new();
        let mut existing_gateways: Vec<PublicKeyBinary> = Vec::new();
        let witnesses = self.witness_reports.clone();

        if !witnesses.is_empty() {
            let witness_earliest_received_ts = witnesses[0].received_timestamp;
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
                            hex_density_map,
                            witness_earliest_received_ts,
                        )
                        .await
                    {
                        Ok(mut verified_witness) => {
                            // track which gateways we have saw a witness report from
                            existing_gateways.push(verified_witness.report.pub_key.clone());
                            if verified_witness.status == VerificationStatus::Valid {
                                // add to list of witness to update last timestamp off
                                witnesses_to_update.push(LastWitness {
                                    id: witness_report.report.pub_key.clone(),
                                    timestamp: verified_witness.received_timestamp,
                                });

                                // post regular validations, check for witness reciprocity
                                // if this check fails we will invalidate the witness
                                // even tho it has passed all regular validations
                                if !self.verify_witness_reciprocity(&witness_report).await? {
                                    verified_witness.status = VerificationStatus::Invalid;
                                    verified_witness.invalid_reason =
                                        InvalidReason::GatewayNoValidBeacons;
                                    verified_witness.participant_side =
                                        InvalidParticipantSide::Witness;
                                }
                            };
                            verified_witnesses.push(verified_witness);
                        }
                        Err(_) => failed_witnesses.push(witness_report),
                    }
                } else {
                    // the report is a dup
                    let dup_witness = IotVerifiedWitnessReport::invalid(
                        InvalidReason::Duplicate,
                        None,
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
        }

        // save a list of gateways which require their last witness timestamp to be updated
        witness_updater.update(witnesses_to_update).await?;

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
        hex_density_map: &HexDensityMap,
        witness_first_ts: DateTime<Utc>,
    ) -> anyhow::Result<IotVerifiedWitnessReport> {
        let witness = &witness_report.report;
        let witness_pub_key = witness.pub_key.clone();
        // pull the witness info from our follower
        let witness_info = match gateway_cache.resolve_gateway_info(&witness_pub_key).await {
            Ok(res) => res,
            Err(GatewayCacheError::GatewayNotFound(_)) => {
                return Ok(IotVerifiedWitnessReport::invalid(
                    InvalidReason::GatewayNotFound,
                    None,
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
                    None,
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
        // we explicitly match location here again
        let Some(ref beaconer_metadata) = beaconer_info.metadata else {
            return Ok(IotVerifiedWitnessReport::invalid(
                InvalidReason::NotAsserted,
                None,
                &witness_report.report,
                witness_report.received_timestamp,
                None,
                // if location is None, default gain and elevation to zero
                0,
                0,
                InvalidParticipantSide::Beaconer,
            ));
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
            witness_first_ts,
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
            Err(invalid_response) => Ok(IotVerifiedWitnessReport::invalid(
                invalid_response.reason,
                invalid_response.details,
                &witness_report.report,
                witness_report.received_timestamp,
                Some(witness_metadata.location),
                witness_metadata.gain,
                witness_metadata.elevation,
                InvalidParticipantSide::Witness,
            )),
        }
    }

    async fn verify_beacon_reciprocity(
        &self,
        witness_updater: &WitnessUpdater,
    ) -> anyhow::Result<bool> {
        if !self.witness_reports.is_empty() {
            let last_witness = witness_updater
                .get_last_witness(&self.beacon_report.report.pub_key)
                .await?;
            return Ok(last_witness.map_or(false, |lw| {
                self.beacon_report.received_timestamp - lw.timestamp < *RECIPROCITY_WINDOW
            }));
        }
        Ok(false)
    }

    async fn verify_witness_reciprocity(
        &self,
        report: &IotWitnessIngestReport,
    ) -> anyhow::Result<bool> {
        let last_beacon = LastBeacon::get(&self.pool, &report.report.pub_key).await?;
        Ok(last_beacon.map_or(false, |lw| {
            report.received_timestamp - lw.timestamp < *RECIPROCITY_WINDOW
        }))
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
) -> GenericVerifyResult {
    tracing::debug!(
        "verifying beacon from beaconer: {:?}",
        beaconer_info.address.clone()
    );
    let beacon_received_ts = beacon_report.received_timestamp;
    let beaconer_metadata = match beaconer_info.metadata {
        Some(ref metadata) => metadata,
        None => {
            return Err(InvalidResponse {
                reason: InvalidReason::NotAsserted,
                details: None,
            })
        }
    };
    verify_denylist(&beacon_report.report.pub_key, deny_list)?;
    verify_entropy(entropy_start, entropy_end, beacon_received_ts)?;
    verify_gw_capability(beaconer_info.is_full_hotspot)?;
    verify_beacon_schedule(&last_beacon, beacon_received_ts, beacon_interval)?;
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

#[allow(clippy::too_many_arguments)]
pub fn do_witness_verifications(
    deny_list: &DenyList,
    entropy_start: DateTime<Utc>,
    entropy_end: DateTime<Utc>,
    witness_report: &IotWitnessIngestReport,
    witness_info: &GatewayInfo,
    beacon_report: &IotBeaconIngestReport,
    beaconer_metadata: &GatewayMetadata,
    witness_first_ts: DateTime<Utc>,
) -> GenericVerifyResult {
    tracing::debug!(
        "verifying witness from gateway: {:?}",
        witness_info.address.clone()
    );
    let beacon_report = &beacon_report;
    let witness_metadata = match witness_info.metadata {
        Some(ref metadata) => metadata,
        None => {
            return Err(InvalidResponse {
                reason: InvalidReason::NotAsserted,
                details: None,
            })
        }
    };
    verify_denylist(&witness_report.report.pub_key, deny_list)?;
    verify_edge_denylist(
        &beacon_report.report.pub_key,
        &witness_report.report.pub_key,
        deny_list,
    )?;
    verify_self_witness(
        &beacon_report.report.pub_key,
        &witness_report.report.pub_key,
    )?;
    verify_entropy(
        entropy_start,
        entropy_end,
        witness_report.received_timestamp,
    )?;
    verify_witness_lag(
        beacon_report.received_timestamp,
        witness_first_ts,
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
) -> GenericVerifyResult {
    match last_beacon {
        Some(last_beacon) => {
            let last_bucket = last_beacon
                .timestamp
                .duration_trunc(beacon_interval)
                .map_err(|e| {
                    // if we fail to cast to a valid bucket beacon interval is likely incorrectly set
                    // rather than default all beacons to success or fail, bail out
                    panic!(
                        "failed to parse last bucket: {}, beacon_interval: {}, error: {}:",
                        last_beacon.timestamp, beacon_interval, e
                    )
                })?;
            let cur_bucket = beacon_received_ts
                .duration_trunc(beacon_interval)
                .map_err(|e| {
                    // if we fail to cast to a valid bucket beacon interval is likely incorrectly set
                    // rather than default all beacons to success or fail, bail out
                    panic!(
                        "failed to parse cur bucket: {}, beacon_interval: {}, error: {}:",
                        beacon_received_ts, beacon_interval, e
                    )
                })?;
            if cur_bucket <= last_bucket {
                tracing::debug!(
                    "beacon verification failed, reason:
                        IrregularInterval. last_beacon_ts: {}, beacon_received_ts:{}",
                    last_beacon.timestamp,
                    beacon_received_ts
                );
                return Err(InvalidResponse {
                    reason: InvalidReason::IrregularInterval,
                    details: None,
                });
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
    if deny_list.contains_key(pub_key) {
        tracing::debug!(
            "report verification failed, reason: {:?}.
            pubkey: {}, tagname: {}",
            InvalidReason::Denied,
            pub_key,
            deny_list.tag_name
        );
        return Err(InvalidResponse {
            reason: InvalidReason::Denied,
            details: Some(InvalidDetails {
                data: Some(invalid_details::Data::DenylistTag(
                    deny_list.tag_name.to_string(),
                )),
            }),
        });
    }
    //
    Ok(())
}

/// verify if gateway-gateway edge is on the deny list
/// note that the order of the gateway keys is unimportant as edges are not considered directional
fn verify_edge_denylist(
    beaconer: &PublicKeyBinary,
    witness: &PublicKeyBinary,
    deny_list: &DenyList,
) -> GenericVerifyResult {
    if deny_list.contains_edge(beaconer, witness) {
        tracing::debug!(
            "report verification failed, reason: {:?}.
            beacon: {}, witness {}, tagname: {}",
            InvalidReason::DeniedEdge,
            beaconer,
            witness,
            deny_list.tag_name
        );
        return Err(InvalidResponse {
            reason: InvalidReason::DeniedEdge,
            details: Some(InvalidDetails {
                data: Some(invalid_details::Data::DenylistTag(
                    deny_list.tag_name.to_string(),
                )),
            }),
        });
    }
    //
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
        return Err(InvalidResponse {
            reason: InvalidReason::EntropyExpired,
            details: None,
        });
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
        timestamp: 0,
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
        InvalidResponse {
            reason: InvalidReason::InvalidPacket,
            details: None,
        }
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
                return Err(InvalidResponse {
                    reason: InvalidReason::InvalidPacket,
                    details: None,
                });
            }
        };
    tracing::debug!("reported beacon {:?}", reported_beacon);

    // compare reports
    if !generated_beacon.verify(&reported_beacon) {
        tracing::debug!(
            "beacon construction verification failed, pubkey {:?}",
            beacon_report.pub_key,
        );
        return Err(InvalidResponse {
            reason: InvalidReason::InvalidPacket,
            details: None,
        });
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
        return Err(InvalidResponse {
            reason: InvalidReason::InvalidCapability,
            details: None,
        });
    };
    Ok(())
}

/// verify witness lag
/// if the first received event is the beacon then,
/// all witnesses must be received within MAX_BEACON_TO_WITNESS_LAG of the beacon
/// if the first received event is a witness then,
/// all subsequent witnesses must be received within MAX_WITNESS_LAG of that first witness
fn verify_witness_lag(
    beacon_received_ts: DateTime<Utc>,
    first_witness_ts: DateTime<Utc>,
    received_ts: DateTime<Utc>,
) -> GenericVerifyResult {
    let (first_event_ts, max_permitted_lag) = if beacon_received_ts <= first_witness_ts {
        (beacon_received_ts, *MAX_BEACON_TO_WITNESS_LAG)
    } else {
        (first_witness_ts, *MAX_WITNESS_LAG)
    };
    let this_witness_lag = received_ts - first_event_ts;
    if this_witness_lag > max_permitted_lag {
        tracing::debug!(
            reason = ?InvalidReason::TooLate,
            %received_ts,
            %beacon_received_ts,
            %first_witness_ts,
            "witness verification failed"
        );
        return Err(InvalidResponse {
            reason: InvalidReason::TooLate,
            details: None,
        });
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
        return Err(InvalidResponse {
            reason: InvalidReason::SelfWitness,
            details: None,
        });
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
        return Err(InvalidResponse {
            reason: InvalidReason::InvalidFrequency,
            details: None,
        });
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
        return Err(InvalidResponse {
            reason: InvalidReason::InvalidRegion,
            details: None,
        });
    }
    Ok(())
}

/// verify witness does not exceed max distance from beaconer
fn verify_witness_distance(beacon_loc: u64, witness_loc: u64) -> GenericVerifyResult {
    let witness_distance = match calc_distance(beacon_loc, witness_loc) {
        Ok(d) => d,
        Err(_) => {
            return Err(InvalidResponse {
                reason: InvalidReason::MaxDistanceExceeded,
                details: None,
            })
        }
    };
    if witness_distance / 1000 > POC_DISTANCE_LIMIT {
        tracing::debug!(
            "witness verification failed, reason: {:?}. distance {witness_distance}",
            InvalidReason::MaxDistanceExceeded
        );
        return Err(InvalidResponse {
            reason: InvalidReason::MaxDistanceExceeded,
            details: None,
        });
    }
    Ok(())
}

/// verify min hex distance between beaconer and witness
fn verify_witness_cell_distance(beacon_loc: u64, witness_loc: u64) -> GenericVerifyResult {
    let cell_distance = match calc_cell_distance(beacon_loc, witness_loc) {
        Ok(d) => d,
        Err(_) => {
            return Err(InvalidResponse {
                reason: InvalidReason::BelowMinDistance,
                details: None,
            })
        }
    };
    if cell_distance < POC_CELL_DISTANCE_MINIMUM {
        tracing::debug!(
            "witness verification failed, reason: {:?}. cell distance {cell_distance}",
            InvalidReason::BelowMinDistance
        );
        return Err(InvalidResponse {
            reason: InvalidReason::BelowMinDistance,
            details: None,
        });
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
        Err(_) => {
            return Err(InvalidResponse {
                reason: InvalidReason::BadRssi,
                details: None,
            })
        }
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
        return Err(InvalidResponse {
            reason: InvalidReason::BadRssi,
            details: None,
        });
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
        return Err(InvalidResponse {
            reason: InvalidReason::InvalidPacket,
            details: None,
        });
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
        invalid_details: Option<InvalidDetails>,
        gateway_info: Option<GatewayInfo>,
        hex_scale: Option<Decimal>,
    ) -> Self {
        Self {
            result,
            invalid_reason,
            invalid_details,
            gateway_info,
            hex_scale,
        }
    }

    pub fn valid(gateway_info: GatewayInfo, hex_scale: Decimal) -> Self {
        Self::new(
            VerificationStatus::Valid,
            InvalidReason::ReasonNone,
            None,
            Some(gateway_info),
            Some(hex_scale),
        )
    }

    pub fn invalid(
        invalid_reason: InvalidReason,
        invalid_details: Option<InvalidDetails>,
        gateway_info: GatewayInfo,
    ) -> Self {
        Self::new(
            VerificationStatus::Invalid,
            invalid_reason,
            invalid_details,
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
    const DENIED_PUBKEY2: &str = "13ABbtvMrRK8jgYrT3h6Y9Zu44nS6829kzsamiQn9Eefeu3VAZs";

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
            beacon::RegionParams::from_bytes(region.into(), gain as u64, EU868_PARAMS, 0)
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
        /*
        assert_eq!(
            Err(InvalidResponse {
                reason: InvalidReason::IrregularInterval,
                details: None
            }),
            verify_beacon_payload(
                &ingest_beacon_report.report,
                region,
                &region_params.params,
                gain,
                entropy_start,
                ENTROPY_VERSION as u32
            )
        );
        */
    }

    #[test]
    fn test_verify_beacon_schedule() {
        let beacon_interval = Duration::seconds(21600); // 6 hours
        let last_beacon_ts: DateTime<Utc> =
            DateTime::parse_from_str("2023 Jan 02 00:00:01 +0000", "%Y %b %d %H:%M:%S %z")
                .unwrap()
                .into();
        let last_beacon = Some(LastBeacon {
            id: PublicKeyBinary::from_str(PUBKEY1).unwrap(),
            timestamp: last_beacon_ts,
        });
        // beacon is in a later bucket ( by one hour) after last beacon, expectation pass
        assert!(verify_beacon_schedule(
            &last_beacon,
            last_beacon_ts + beacon_interval + Duration::hours(1),
            beacon_interval,
        )
        .is_ok());

        // beacon is in the same bucket as last beacon, expectation fail
        assert_eq!(
            Err(InvalidResponse {
                reason: InvalidReason::IrregularInterval,
                details: None
            }),
            verify_beacon_schedule(
                &last_beacon,
                last_beacon_ts + (beacon_interval / 2),
                beacon_interval,
            )
        );

        // beacon is in an earlier bucket than last beacon, expectation fail
        assert_eq!(
            Err(InvalidResponse {
                reason: InvalidReason::IrregularInterval,
                details: None
            }),
            verify_beacon_schedule(
                &last_beacon,
                last_beacon_ts - (beacon_interval + Duration::hours(1)),
                beacon_interval,
            )
        );
    }

    #[test]
    fn test_verify_entropy() {
        let now = Utc::now();
        let entropy_start = now - Duration::seconds(60);
        let entropy_end = now - Duration::seconds(10);
        assert!(verify_entropy(entropy_start, entropy_end, now - Duration::seconds(30)).is_ok());
        assert_eq!(
            Err(InvalidResponse {
                reason: InvalidReason::EntropyExpired,
                details: None
            }),
            verify_entropy(entropy_start, entropy_end, now - Duration::seconds(1))
        );
        assert_eq!(
            Err(InvalidResponse {
                reason: InvalidReason::EntropyExpired,
                details: None
            }),
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
            Err(InvalidResponse {
                reason: InvalidReason::Denied,
                details: Some(InvalidDetails {
                    data: Some(invalid_details::Data::DenylistTag("0".to_string()))
                }),
            }),
            verify_denylist(
                &PublicKeyBinary::from_str(DENIED_PUBKEY1).unwrap(),
                &deny_list
            )
        );
    }

    #[test]
    fn test_verify_edge_denylist() {
        let deny_list: DenyList = vec![(
            PublicKeyBinary::from_str(DENIED_PUBKEY1).unwrap(),
            PublicKeyBinary::from_str(DENIED_PUBKEY2).unwrap(),
        )]
        .try_into()
        .unwrap();
        assert!(verify_edge_denylist(
            &PublicKeyBinary::from_str(PUBKEY1).unwrap(),
            &PublicKeyBinary::from_str(PUBKEY2).unwrap(),
            &deny_list
        )
        .is_ok());
        assert!(verify_edge_denylist(
            &PublicKeyBinary::from_str(DENIED_PUBKEY1).unwrap(),
            &PublicKeyBinary::from_str(PUBKEY2).unwrap(),
            &deny_list
        )
        .is_ok());
        assert!(verify_edge_denylist(
            &PublicKeyBinary::from_str(PUBKEY1).unwrap(),
            &PublicKeyBinary::from_str(DENIED_PUBKEY2).unwrap(),
            &deny_list
        )
        .is_ok());
        assert_eq!(
            Err(InvalidResponse {
                reason: InvalidReason::DeniedEdge,
                details: Some(InvalidDetails {
                    data: Some(invalid_details::Data::DenylistTag("0".to_string()))
                }),
            }),
            verify_edge_denylist(
                &PublicKeyBinary::from_str(DENIED_PUBKEY1).unwrap(),
                &PublicKeyBinary::from_str(DENIED_PUBKEY2).unwrap(),
                &deny_list
            )
        );
        // edges are not directional
        assert_eq!(
            Err(InvalidResponse {
                reason: InvalidReason::DeniedEdge,
                details: Some(InvalidDetails {
                    data: Some(invalid_details::Data::DenylistTag("0".to_string()))
                }),
            }),
            verify_edge_denylist(
                &PublicKeyBinary::from_str(DENIED_PUBKEY2).unwrap(),
                &PublicKeyBinary::from_str(DENIED_PUBKEY1).unwrap(),
                &deny_list
            )
        );
    }

    #[test]
    fn test_verify_capability() {
        assert!(verify_gw_capability(true).is_ok());
        assert_eq!(
            Err(InvalidResponse {
                reason: InvalidReason::InvalidCapability,
                details: None
            }),
            verify_gw_capability(false)
        );
    }

    #[test]
    fn test_verify_witness_lag() {
        let now = Utc::now();
        // a beacon is received first and our test witness is within the acceptable lag from that beacon
        assert!(verify_witness_lag(
            now - Duration::seconds(60),
            now - Duration::seconds(59),
            now - Duration::seconds(58)
        )
        .is_ok());
        // a witness is received first and our test witness is within the acceptable lag from that first witness
        assert!(verify_witness_lag(
            now - Duration::seconds(60),
            now - Duration::seconds(64),
            now - Duration::seconds(63)
        )
        .is_ok());
        // a beacon is received first and our test witness is over the acceptable lag from that beacon
        assert_eq!(
            Err(InvalidResponse {
                reason: InvalidReason::TooLate,
                details: None
            }),
            verify_witness_lag(
                now - Duration::seconds(60),
                now - Duration::seconds(59),
                now - Duration::seconds(55)
            )
        );

        // a witness is received first and our test witness is over the acceptable lag from that first witness
        assert_eq!(
            Err(InvalidResponse {
                reason: InvalidReason::TooLate,
                details: None
            }),
            verify_witness_lag(
                now - Duration::seconds(55),
                now - Duration::seconds(60),
                now - Duration::seconds(58)
            )
        );
        // a witness is received first and our test witness is that same first witness
        assert!(verify_witness_lag(
            now - Duration::seconds(55),
            now - Duration::seconds(60),
            now - Duration::seconds(60)
        )
        .is_ok());
    }

    #[test]
    fn test_verify_self_witness() {
        let key1 = PublicKeyBinary::from_str(PUBKEY1).unwrap();
        let key2 = PublicKeyBinary::from_str(PUBKEY2).unwrap();
        assert!(verify_self_witness(&key1, &key2).is_ok());
        assert_eq!(
            Err(InvalidResponse {
                reason: InvalidReason::SelfWitness,
                details: None
            }),
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
            Err(InvalidResponse {
                reason: InvalidReason::InvalidFrequency,
                details: None
            }),
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
            Err(InvalidResponse {
                reason: InvalidReason::InvalidRegion,
                details: None
            }),
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
            Err(InvalidResponse {
                reason: InvalidReason::MaxDistanceExceeded,
                details: None
            }),
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
            Err(InvalidResponse {
                reason: InvalidReason::BelowMinDistance,
                details: None
            }),
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
            Err(InvalidResponse {
                reason: InvalidReason::BadRssi,
                details: None
            }),
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
            Err(InvalidResponse {
                reason: InvalidReason::InvalidPacket,
                details: None
            }),
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
        // one of more verifications being accidentally removed
        // from `do_beacon_verifications`

        // create default data structs
        let beaconer_info = beaconer_gateway_info(Some(LOC0), ProtoRegion::Eu868, true);
        let entropy_start = Utc.timestamp_millis_opt(ENTROPY_TIMESTAMP).unwrap();
        let entropy_end = entropy_start + Duration::minutes(3);
        let beacon_interval = Duration::seconds(21600); // 6 hours in secs
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
        );
        assert_eq!(
            Err(InvalidResponse {
                reason: InvalidReason::Denied,
                details: Some(InvalidDetails {
                    data: Some(invalid_details::Data::DenylistTag("0".to_string()))
                }),
            }),
            resp1
        );

        // test entropy lifespan verification is active in the beacon validation list
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
        );
        assert_eq!(
            Err(InvalidResponse {
                reason: InvalidReason::EntropyExpired,
                details: None
            }),
            resp1
        );

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
        );
        assert_eq!(
            Err(InvalidResponse {
                reason: InvalidReason::NotAsserted,
                details: None
            }),
            resp2
        );

        // test schedule verification is active in the beacon validation list
        let beacon_report3 = valid_beacon_report(PUBKEY1, entropy_start + Duration::minutes(2));
        let last_beacon3 = LastBeacon {
            id: PublicKeyBinary::from_str(PUBKEY1).unwrap(),
            timestamp: Utc::now() - Duration::hours(5),
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
        );
        assert_eq!(
            Err(InvalidResponse {
                reason: InvalidReason::IrregularInterval,
                details: None
            }),
            resp3
        );

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
        );
        assert_eq!(
            Err(InvalidResponse {
                reason: InvalidReason::InvalidCapability,
                details: None
            }),
            resp4
        );

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
        );
        assert_eq!(
            Err(InvalidResponse {
                reason: InvalidReason::InvalidPacket,
                details: None
            }),
            resp5
        );

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
        // one of more verifications being accidentally removed
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
            witness_report1.received_timestamp,
        );
        assert_eq!(
            Err(InvalidResponse {
                reason: InvalidReason::SelfWitness,
                details: None
            }),
            resp1
        );

        // test entropy lifespan verification is active in the witness validation list
        let witness_report2 = valid_witness_report(PUBKEY2, entropy_start + Duration::minutes(5));
        let resp2 = do_witness_verifications(
            &deny_list,
            entropy_start,
            entropy_end,
            &witness_report2,
            &witness_info,
            &beacon_report,
            &beaconer_metadata,
            witness_report2.received_timestamp,
        );
        assert_eq!(
            Err(InvalidResponse {
                reason: InvalidReason::EntropyExpired,
                details: None
            }),
            resp2
        );

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
            witness_report3.received_timestamp,
        );
        assert_eq!(
            Err(InvalidResponse {
                reason: InvalidReason::InvalidPacket,
                details: None
            }),
            resp3
        );

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
            witness_report4.received_timestamp,
        );
        assert_eq!(
            Err(InvalidResponse {
                reason: InvalidReason::NotAsserted,
                details: None
            }),
            resp4
        );

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
            witness_report5.received_timestamp,
        );
        assert_eq!(
            Err(InvalidResponse {
                reason: InvalidReason::InvalidFrequency,
                details: None
            }),
            resp5
        );

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
            witness_report6.received_timestamp,
        );
        assert_eq!(
            Err(InvalidResponse {
                reason: InvalidReason::InvalidRegion,
                details: None
            }),
            resp6
        );

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
            witness_report7.received_timestamp,
        );
        assert_eq!(
            Err(InvalidResponse {
                reason: InvalidReason::BelowMinDistance,
                details: None
            }),
            resp7
        );

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
            witness_report8.received_timestamp,
        );
        assert_eq!(
            Err(InvalidResponse {
                reason: InvalidReason::MaxDistanceExceeded,
                details: None
            }),
            resp8
        );

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
            witness_report9.received_timestamp,
        );
        assert_eq!(
            Err(InvalidResponse {
                reason: InvalidReason::BadRssi,
                details: None
            }),
            resp9
        );

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
            witness_report10.received_timestamp,
        );
        assert_eq!(
            Err(InvalidResponse {
                reason: InvalidReason::InvalidCapability,
                details: None
            }),
            resp10
        );

        // test witness lag from first received event
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
            witness_report11.received_timestamp - Duration::milliseconds(6000),
        );
        assert_eq!(
            Err(InvalidResponse {
                reason: InvalidReason::TooLate,
                details: None
            }),
            resp11
        );

        // for completeness, confirm our valid witness report is sane
        let witness_report12 = valid_witness_report(PUBKEY2, entropy_start + Duration::minutes(2));
        let witness_info12 = witness_gateway_info(Some(LOC4), ProtoRegion::Eu868, true);
        let resp12 = do_witness_verifications(
            &deny_list,
            entropy_start,
            entropy_end,
            &witness_report12,
            &witness_info12,
            &beacon_report,
            &beaconer_metadata,
            witness_report12.received_timestamp,
        );
        assert_eq!(Ok(()), resp12);
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
            0,
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
