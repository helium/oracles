use crate::{last_beacon::LastBeacon, Error, Result};
use chrono::{DateTime, Duration, Utc};
use file_store::{
    lora_beacon_report::LoraBeaconIngestReport, lora_invalid_poc::LoraInvalidWitnessReport,
    lora_valid_poc::LoraValidWitnessReport, lora_witness_report::LoraWitnessIngestReport,
};
use geo::{point, prelude::*};
use h3ron::{to_geo::ToCoordinate, H3Cell, H3DirectedEdge, Index};
use helium_proto::{
    services::poc_lora::{InvalidParticipantSide, InvalidReason},
    GatewayStakingMode,
};
use node_follower::{
    follower_service::FollowerService,
    gateway_resp::{GatewayInfo, GatewayInfoResolver},
};
use sqlx::PgPool;
use std::f64::consts::PI;

/// C is the speed of light in air in meters per second
pub const C: f64 = 2.998e8;
/// R is the (average) radius of the earth
pub const R: f64 = 6.371e6;

/// the cadence in seconds at which hotspots are permitted to beacon
const BEACON_INTERVAL: i64 = (10 * 60) - 10; // 10 mins ( minus 10 sec tolerance )
/// measurement in seconds of an entropy
/// TODO: determine a sane value here, set high for testing
const ENTROPY_LIFESPAN: i64 = 180;
/// max permitted distance of a witness from a beaconer measured in KM
const POC_DISTANCE_LIMIT: i32 = 100;

pub enum VerificationStatus {
    Valid,
    Invalid,
    Failed,
}
pub struct Poc {
    beacon_report: LoraBeaconIngestReport,
    witness_reports: Vec<LoraWitnessIngestReport>,
    entropy_start: DateTime<Utc>,
    entropy_end: DateTime<Utc>,
    follower_service: FollowerService,
    pool: PgPool,
}

pub struct VerifyBeaconResult {
    pub result: VerificationStatus,
    pub invalid_reason: Option<InvalidReason>,
    pub gateway_info: Option<GatewayInfo>,
    pub hex_scale: Option<f32>,
}

pub struct VerifyWitnessResult {
    result: VerificationStatus,
    invalid_reason: Option<InvalidReason>,
    pub gateway_info: Option<GatewayInfo>,
}

pub struct VerifyWitnessesResult {
    pub valid_witnesses: Vec<LoraValidWitnessReport>,
    pub invalid_witnesses: Vec<LoraInvalidWitnessReport>,
    pub failed_witnesses: Vec<LoraInvalidWitnessReport>,
}

impl Poc {
    pub async fn new(
        beacon_report: LoraBeaconIngestReport,
        witness_reports: Vec<LoraWitnessIngestReport>,
        entropy_start: DateTime<Utc>,
        follower_service: FollowerService,
        pool: PgPool,
    ) -> Result<Self> {
        let entropy_end = entropy_start + Duration::seconds(ENTROPY_LIFESPAN);
        Ok(Self {
            beacon_report,
            witness_reports,
            entropy_start,
            entropy_end,
            follower_service,
            pool,
        })
    }

    pub async fn verify_beacon(&mut self) -> Result<VerifyBeaconResult> {
        let beacon = &self.beacon_report.report;
        // use pub key to get GW info from our follower
        let beaconer_pub_key = beacon.pub_key.clone();
        let beacon_received_ts = self.beacon_report.received_timestamp;

        let beaconer_info = match self
            .follower_service
            .resolve_gateway_info(&beaconer_pub_key)
            .await
        {
            Ok(res) => res,
            Err(e) => {
                tracing::debug!("beacon verification failed, reason: {:?}", e);
                let resp = VerifyBeaconResult {
                    result: VerificationStatus::Failed,
                    invalid_reason: Some(InvalidReason::GatewayNotFound),
                    gateway_info: None,
                    hex_scale: None,
                };
                return Ok(resp);
            }
        };
        tracing::debug!("beacon info {:?}", beaconer_info);

        // is beaconer allowed to beacon at this time ?
        // any irregularily timed beacons will be rejected
        match LastBeacon::get(&self.pool, &beaconer_pub_key.to_vec()).await? {
            Some(last_beacon) => {
                let interval_since_last_beacon = beacon_received_ts - last_beacon.timestamp;
                if interval_since_last_beacon < Duration::seconds(BEACON_INTERVAL) {
                    tracing::debug!(
                        "beacon verification failed, reason:
                        IrregularInterval. Seconds since last beacon {:?}, entropy: {:?}",
                        interval_since_last_beacon.num_seconds(),
                        beacon.data
                    );
                    let resp = VerifyBeaconResult {
                        result: VerificationStatus::Invalid,
                        invalid_reason: Some(InvalidReason::IrregularInterval),
                        gateway_info: Some(beaconer_info),
                        hex_scale: None,
                    };
                    return Ok(resp);
                }
            }
            None => {
                tracing::debug!("no last beacon timestamp available for this beaconer, ignoring ");
            }
        }

        // verify the beaconer's remote entropy
        // if beacon received timestamp is outside of entopy start/end then reject the poc
        if beacon_received_ts < self.entropy_start || beacon_received_ts > self.entropy_end {
            tracing::debug!(
                "beacon verification failed, reason: {:?}. beacon_received_ts: {:?}, entropy_start_time: {:?}, entropy_end_time: {:?}",
                InvalidReason::BadEntropy,
                beacon_received_ts,
                self.entropy_start,
                self.entropy_end
            );
            let resp = VerifyBeaconResult {
                result: VerificationStatus::Invalid,
                invalid_reason: Some(InvalidReason::BadEntropy),
                gateway_info: Some(beaconer_info),
                hex_scale: None,
            };
            return Ok(resp);
        }

        //check beaconer has an asserted location
        if beaconer_info.location.is_none() {
            tracing::debug!(
                "beacon verification failed, reason: {:?}",
                InvalidReason::NotAsserted
            );
            let resp = VerifyBeaconResult {
                result: VerificationStatus::Invalid,
                invalid_reason: Some(InvalidReason::NotAsserted),
                gateway_info: Some(beaconer_info),
                hex_scale: None,
            };
            return Ok(resp);
        }

        // check beaconer is permitted to participate in POC
        match beaconer_info.staking_mode {
            GatewayStakingMode::Dataonly => {
                tracing::debug!(
                    "beacon verification failed, reason: {:?}",
                    InvalidReason::InvalidCapability
                );
                let resp = VerifyBeaconResult {
                    result: VerificationStatus::Invalid,
                    invalid_reason: Some(InvalidReason::InvalidCapability),
                    gateway_info: Some(beaconer_info),
                    hex_scale: None,
                };
                return Ok(resp);
            }
            GatewayStakingMode::Full => (),
            GatewayStakingMode::Light => (),
        }

        // TODO: insert hex scale lookup here
        //       value hardcoded to 1.0 temporarily

        tracing::debug!("beacon verification success");
        // all is good with the beacon
        let resp = VerifyBeaconResult {
            result: VerificationStatus::Valid,
            invalid_reason: None,
            gateway_info: Some(beaconer_info),
            hex_scale: Some(1.0),
        };

        Ok(resp)
    }

    pub async fn verify_witnesses(
        &mut self,
        beacon_info: &GatewayInfo,
    ) -> Result<VerifyWitnessesResult> {
        let mut valid_witnesses: Vec<LoraValidWitnessReport> = Vec::new();
        let mut invalid_witnesses: Vec<LoraInvalidWitnessReport> = Vec::new();
        let mut failed_witnesses: Vec<LoraInvalidWitnessReport> = Vec::new();
        let witnesses = self.witness_reports.clone();
        for witness_report in witnesses {
            let witness_result = self.verify_witness(&witness_report, beacon_info).await?;
            match witness_result.result {
                VerificationStatus::Valid => {
                    let gw_info: GatewayInfo = witness_result.gateway_info.ok_or_else(|| {
                        Error::not_found("invalid FollowerGatewayResp for witness")
                    })?;
                    // TODO: perform hex density check here for a valid witness
                    let valid_witness = LoraValidWitnessReport {
                        received_timestamp: witness_report.received_timestamp,
                        location: gw_info.location,
                        hex_scale: 1.0, //TODO: replace with actual hex scale when available
                        report: witness_report.report,
                    };
                    valid_witnesses.push(valid_witness)
                }
                VerificationStatus::Invalid => {
                    let invalid_witness = LoraInvalidWitnessReport {
                        received_timestamp: witness_report.received_timestamp,
                        reason: witness_result
                            .invalid_reason
                            .ok_or_else(|| Error::not_found("invalid invalid_reason"))?,
                        report: witness_report.report,
                        participant_side: InvalidParticipantSide::Witness,
                    };
                    invalid_witnesses.push(invalid_witness)
                }
                VerificationStatus::Failed => {
                    // if a witness check returns failed it suggests something
                    // unexpected has occurred. propogate this back to caller
                    // and allow it to do its things
                    let failed_witness = LoraInvalidWitnessReport {
                        received_timestamp: witness_report.received_timestamp,
                        reason: witness_result
                            .invalid_reason
                            .ok_or_else(|| Error::not_found("invalid invalid_reason"))?,
                        report: witness_report.report,
                        participant_side: InvalidParticipantSide::Witness,
                    };
                    failed_witnesses.push(failed_witness)
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
        witness_report: &LoraWitnessIngestReport,
        beaconer_info: &GatewayInfo,
    ) -> Result<VerifyWitnessResult> {
        // use pub key to get GW info from our follower and verify the witness
        let witness = &witness_report.report;
        let beacon = &self.beacon_report.report;
        let witness_pub_key = witness.pub_key.clone();
        let witness_info = match self
            .follower_service
            .resolve_gateway_info(&witness_pub_key)
            .await
        {
            Ok(res) => res,
            Err(_) => {
                tracing::debug!(
                    "witness verification failed, reason: {:?}",
                    InvalidReason::GatewayNotFound
                );
                let resp = VerifyWitnessResult {
                    result: VerificationStatus::Failed,
                    invalid_reason: Some(InvalidReason::GatewayNotFound),
                    gateway_info: None,
                };
                return Ok(resp);
            }
        };

        // check witness is permitted to participate in POC
        match witness_info.staking_mode {
            GatewayStakingMode::Dataonly => {
                tracing::debug!(
                    "witness verification failed, reason: {:?}",
                    InvalidReason::InvalidCapability
                );
                let resp = VerifyWitnessResult {
                    result: VerificationStatus::Invalid,
                    invalid_reason: Some(InvalidReason::InvalidCapability),
                    gateway_info: Some(witness_info),
                };
                return Ok(resp);
            }
            GatewayStakingMode::Full => (),
            GatewayStakingMode::Light => (),
        }

        // check the beaconer is not self witnessing
        if witness_report.report.pub_key == self.beacon_report.report.pub_key {
            tracing::debug!(
                "witness verification failed, reason: {:?}",
                InvalidReason::SelfWitness
            );
            let resp = VerifyWitnessResult {
                result: VerificationStatus::Invalid,
                invalid_reason: Some(InvalidReason::SelfWitness),
                gateway_info: Some(witness_info),
            };
            return Ok(resp);
        }

        // if witness report received timestamp is outside of entopy start/end then reject the poc
        let witness_received_time = witness_report.received_timestamp;
        if witness_received_time < self.entropy_start || witness_received_time > self.entropy_end {
            tracing::debug!(
                "witness verification failed, reason: {:?}",
                InvalidReason::EntropyExpired
            );
            let resp = VerifyWitnessResult {
                result: VerificationStatus::Invalid,
                invalid_reason: Some(InvalidReason::EntropyExpired),
                gateway_info: Some(witness_info),
            };
            return Ok(resp);
        }

        // check witness has an asserted location
        if witness_info.location.is_none() {
            tracing::debug!(
                "witness verification failed, reason: {:?}",
                InvalidReason::NotAsserted
            );
            let resp = VerifyWitnessResult {
                result: VerificationStatus::Invalid,
                invalid_reason: Some(InvalidReason::NotAsserted),
                gateway_info: Some(witness_info),
            };
            return Ok(resp);
        }

        // check witness is utilizing same freq and that of the beaconer
        // tolerance is 100Khz
        if i32::unsigned_abs((beacon.frequency - witness.frequency) as i32) > 1000 * 100 {
            tracing::debug!(
                "witness verification failed, reason: {:?}",
                InvalidReason::InvalidFrequency
            );
            let resp = VerifyWitnessResult {
                result: VerificationStatus::Invalid,
                invalid_reason: Some(InvalidReason::InvalidFrequency),
                gateway_info: Some(witness_info),
            };
            return Ok(resp);
        }

        // check beaconer & witness are in the same region
        if beaconer_info.region != witness_info.region {
            tracing::debug!(
                "witness verification failed, reason: {:?}",
                InvalidReason::InvalidRegion
            );
            let resp = VerifyWitnessResult {
                result: VerificationStatus::Invalid,
                invalid_reason: Some(InvalidReason::InvalidRegion),
                gateway_info: Some(witness_info),
            };
            return Ok(resp);
        }

        // check witness does not exceed max distance from beaconer
        let beaconer_loc = beaconer_info
            .location
            .ok_or_else(|| Error::not_found("invalid beaconer location"))?;
        let witness_loc = witness_info
            .location
            .ok_or_else(|| Error::not_found("invalid witness location"))?;
        let witness_distance = calc_distance(beaconer_loc, witness_loc)?;
        tracing::debug!("witness distance in mtrs: {:?}", witness_distance);
        if witness_distance.round() as i32 / 1000 > POC_DISTANCE_LIMIT {
            tracing::debug!(
                "witness verification failed, reason: {:?}",
                InvalidReason::MaxDistanceExceeded
            );
            let resp = VerifyWitnessResult {
                result: VerificationStatus::Invalid,
                invalid_reason: Some(InvalidReason::MaxDistanceExceeded),
                gateway_info: Some(witness_info),
            };
            return Ok(resp);
        }

        // check free space path loss
        let tx_power = beacon.tx_power;
        let gain = beaconer_info.gain;
        let witness_signal = witness.signal / 10;
        let min_rcv_signal = calc_fspl(tx_power, witness.frequency, witness_distance, gain);
        tracing::debug!(
            "beaconer tx_power: {tx_power},
            beaconer gain: {gain},
            witness signal: {witness_signal},
            witness freq: {:?},
            min_rcv_signal: {min_rcv_signal}",
            witness.frequency
        );
        // signal is submitted as DBM * 10
        // min_rcv_signal is plain old DBM
        if witness_signal > min_rcv_signal as i32 {
            tracing::debug!(
                "witness verification failed, reason: {:?}",
                InvalidReason::BadRssi
            );
            let resp = VerifyWitnessResult {
                result: VerificationStatus::Invalid,
                invalid_reason: Some(InvalidReason::BadRssi),
                gateway_info: Some(witness_info),
            };
            return Ok(resp);
        }

        //TODO: Plugin Jay's crate here when ready
        let beacon = &self.beacon_report.report;
        if witness.data != beacon.data {
            tracing::debug!(
                "witness verification failed, reason: {:?}",
                InvalidReason::InvalidPacket
            );
            let resp = VerifyWitnessResult {
                result: VerificationStatus::Invalid,
                invalid_reason: Some(InvalidReason::InvalidPacket),
                gateway_info: Some(witness_info),
            };
            return Ok(resp);
        }

        // witness is good
        tracing::debug!("witness verification success");
        let resp = VerifyWitnessResult {
            result: VerificationStatus::Valid,
            invalid_reason: None,
            gateway_info: Some(witness_info),
        };

        Ok(resp)
    }
}

fn calc_fspl(tx_power: i32, freq: u64, distance: f64, gain: i32) -> f64 {
    let gt = 0.0;
    let gl = gain as f64 / 10.0;
    let fpsl = (20.0 * (4.0 * PI * distance * (freq as f64) / C).log10()) - gt - gl;
    (tx_power as f64) - fpsl
}

fn calc_distance(p1: u64, p2: u64) -> Result<f64> {
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

fn hex_adjustment(loc: &H3Cell) -> Result<f64> {
    // Distance from hex center to edge, sqrt(3)*edge_length/2.
    let res = loc.resolution();
    let edge_length = H3DirectedEdge::edge_length_avg_m(res)?;
    Ok(
        edge_length * (f64::round(f64::sqrt(3.0) * f64::powf(10.0, 3.0)) / f64::powf(10.0, 3.0))
            / 2.0,
    )
}
