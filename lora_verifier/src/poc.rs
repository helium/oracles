use crate::{datetime_from_epoch, entropy::Entropy, follower::FollowerService, Result};
use helium_proto::services::{
    follower::FollowerGatewayRespV1,
    poc_lora::{
        InvalidParticipantSide, InvalidReason, LoraBeaconIngestReportV1,
        LoraInvalidWitnessReportV1, LoraValidWitnessReportV1, LoraWitnessIngestReportV1,
    },
};

use chrono::{DateTime, Duration, Utc};
use helium_crypto::PublicKey;
use helium_proto::GatewayStakingMode;

const ENTROPY_LIFESPAN: i64 = 90000; //seconds TODO: determine a sane value here, set hight for testing

pub enum VerificationStatus {
    Valid,
    Invalid,
    Failed,
}
pub struct Poc {
    follower_service: FollowerService,
    beacon_report: LoraBeaconIngestReportV1,
    witness_reports: Vec<LoraWitnessIngestReportV1>,
    entropy_start: DateTime<Utc>,
    entropy_end: DateTime<Utc>,
}

pub struct VerifyBeaconResult {
    pub result: VerificationStatus,
    pub invalid_reason: Option<InvalidReason>,
    pub gateway_info: Option<FollowerGatewayRespV1>,
    pub hex_scale: Option<f32>,
}

pub struct VerifyWitnessResult {
    result: VerificationStatus,
    invalid_reason: Option<InvalidReason>,
    pub gateway_info: Option<FollowerGatewayRespV1>,
}

pub struct VerifyWitnessesResult {
    pub valid_witnesses: Vec<LoraValidWitnessReportV1>,
    pub invalid_witnesses: Vec<LoraInvalidWitnessReportV1>,
    pub failed_witnesses: Vec<LoraInvalidWitnessReportV1>,
}

impl Poc {
    pub async fn new(
        beacon_report: LoraBeaconIngestReportV1,
        witness_reports: Vec<LoraWitnessIngestReportV1>,
        entropy_info: Entropy,
    ) -> Result<Self> {
        let follower_service = FollowerService::from_env()?;
        let entropy_start = entropy_info.timestamp;
        let entropy_end = entropy_info.timestamp + Duration::seconds(ENTROPY_LIFESPAN);
        Ok(Self {
            follower_service,
            beacon_report,
            witness_reports,
            entropy_start,
            entropy_end,
        })
    }

    pub async fn verify_beacon(&mut self) -> Result<VerifyBeaconResult> {
        let beacon = self.beacon_report.report.clone().unwrap();
        // use pub key to get GW info from our follower
        let beaconer_pub_key = PublicKey::try_from(beacon.pub_key.clone())?;
        let beaconer_info = match self
            .follower_service
            .query_gateway_info(&beaconer_pub_key)
            .await
        {
            Ok(res) => res,
            Err(_) => {
                let resp = VerifyBeaconResult {
                    result: VerificationStatus::Failed,
                    invalid_reason: Some(InvalidReason::GatewayNotFound),
                    gateway_info: None,
                    hex_scale: None,
                };
                return Ok(resp);
            }
        };

        // tmp hack below when testing locally with no actual real gateway
        // replace beaconer_info declaration above with that below
        // let beaconer_info = FollowerGatewayRespV1 {
        //     height: 130000,
        //     location: String::from("location1"),
        //     address: beacon.pub_key.clone(),
        //     owner: beacon.pub_key.clone(),
        //     staking_mode: GatewayStakingMode::Full as i32,
        // };

        // verify the beaconer's remote entropy
        // if beacon received timestamp is outside of entopy start/end then reject the poc
        let beacon_received_time = datetime_from_epoch(self.beacon_report.received_timestamp);
        if beacon_received_time < self.entropy_start || beacon_received_time > self.entropy_end {
            let resp = VerifyBeaconResult {
                result: VerificationStatus::Invalid,
                invalid_reason: Some(InvalidReason::BadEntropy),
                gateway_info: Some(beaconer_info),
                hex_scale: None,
            };
            return Ok(resp);
        }

        //check beaconer has an asserted location
        if beaconer_info.location.is_empty() {
            let resp = VerifyBeaconResult {
                result: VerificationStatus::Invalid,
                invalid_reason: Some(InvalidReason::NotAsserted),
                gateway_info: Some(beaconer_info),
                hex_scale: None,
            };
            return Ok(resp);
        }

        // check beaconer is permitted to participate in POC
        // TODO implement capabilities mask
        let staking_mode = GatewayStakingMode::from_i32(beaconer_info.staking_mode);
        if let Some(GatewayStakingMode::Dataonly) = staking_mode {
            let resp = VerifyBeaconResult {
                result: VerificationStatus::Invalid,
                invalid_reason: Some(InvalidReason::InvalidCapability),
                gateway_info: Some(beaconer_info),
                hex_scale: None,
            };
            return Ok(resp);
        }
        // TODO: insert hex scale lookup here
        //       value hardcoded to 1.0 temporarily

        // all is good with the beacon
        let resp = VerifyBeaconResult {
            result: VerificationStatus::Valid,
            invalid_reason: None,
            gateway_info: Some(beaconer_info),
            hex_scale: Some(1.0),
        };

        Ok(resp)
    }

    pub async fn verify_witnesses(&mut self) -> Result<VerifyWitnessesResult> {
        let mut valid_witnesses: Vec<LoraValidWitnessReportV1> = Vec::new();
        let mut invalid_witnesses: Vec<LoraInvalidWitnessReportV1> = Vec::new();
        let mut failed_witnesses: Vec<LoraInvalidWitnessReportV1> = Vec::new();
        let witnesses = self.witness_reports.clone(); // TODO fix this cloning
        for witness_report in witnesses {
            let witness_result = self.verify_witness(&witness_report).await?;
            match witness_result.result {
                VerificationStatus::Valid => {
                    // TODO: perform hex density check here for a valid witness
                    let valid_witness = LoraValidWitnessReportV1 {
                        received_timestamp: witness_report.received_timestamp,
                        location: witness_result.gateway_info.unwrap().location,
                        hex_scale: 1.0,
                        report: Some(witness_report.report.unwrap()),
                    };
                    valid_witnesses.push(valid_witness)
                }
                VerificationStatus::Invalid => {
                    let invalid_witness = LoraInvalidWitnessReportV1 {
                        received_timestamp: witness_report.received_timestamp,
                        reason: witness_result.invalid_reason.unwrap() as i32,
                        report: Some(witness_report.report.unwrap()),
                        participant_side: InvalidParticipantSide::Witness as i32,
                    };
                    invalid_witnesses.push(invalid_witness)
                }
                VerificationStatus::Failed => {
                    // if a witness check returns failed it suggests something
                    // unexpected has occurred. propogate this back to caller
                    // and allow it to do its things
                    let failed_witness = LoraInvalidWitnessReportV1 {
                        received_timestamp: witness_report.received_timestamp,
                        reason: witness_result.invalid_reason.unwrap() as i32,
                        report: Some(witness_report.report.unwrap()),
                        participant_side: InvalidParticipantSide::Witness as i32,
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
        witness_report: &LoraWitnessIngestReportV1,
    ) -> Result<VerifyWitnessResult> {
        // use pub key to get GW info from our follower and verify the witness
        let witness = witness_report.report.clone().unwrap();
        let witness_pub_key = PublicKey::try_from(witness.pub_key.clone())?;
        let witness_info = match self
            .follower_service
            .query_gateway_info(&witness_pub_key)
            .await
        {
            Ok(res) => res,
            Err(_) => {
                let resp = VerifyWitnessResult {
                    result: VerificationStatus::Failed,
                    invalid_reason: Some(InvalidReason::GatewayNotFound),
                    gateway_info: None,
                };
                return Ok(resp);
            }
        };

        // tmp hack below when testing locally with no actual real gateway
        // replace witness_info declaration above with that below
        // let witness_info = FollowerGatewayRespV1 {
        //     height: 130000,
        //     location: String::from("location1"),
        //     address: witness.pub_key.clone(),
        //     owner: witness.pub_key.clone(),
        //     staking_mode: GatewayStakingMode::Full as i32,
        // };

        // if beacon timestamp is outside of entopy start/end then reject the poc
        let witness_packet_time = datetime_from_epoch(witness.timestamp);
        if witness_packet_time < self.entropy_start || witness_packet_time > self.entropy_end {
            let resp = VerifyWitnessResult {
                result: VerificationStatus::Invalid,
                invalid_reason: Some(InvalidReason::EntropyExpired),
                gateway_info: Some(witness_info),
            };
            return Ok(resp);
        }

        // check beaconer has an asserted location
        if witness_info.location.is_empty() {
            let resp = VerifyWitnessResult {
                result: VerificationStatus::Invalid,
                invalid_reason: Some(InvalidReason::NotAsserted),
                gateway_info: Some(witness_info),
            };
            return Ok(resp);
        }

        // check beaconer is permitted to participate in POC
        // TODO implement capabilities mask or is mode check sufficient these days?
        let staking_mode = GatewayStakingMode::from_i32(witness_info.staking_mode);
        if let Some(GatewayStakingMode::Dataonly) = staking_mode {
            let resp = VerifyWitnessResult {
                result: VerificationStatus::Invalid,
                invalid_reason: Some(InvalidReason::InvalidCapability),
                gateway_info: Some(witness_info),
            };
            return Ok(resp);
        }

        //TODO: Plugin Jay's crate here when ready
        let beacon = self.beacon_report.report.clone().unwrap();
        if witness.data != beacon.data {
            let resp = VerifyWitnessResult {
                result: VerificationStatus::Invalid,
                invalid_reason: Some(InvalidReason::InvalidPacket),
                gateway_info: Some(witness_info),
            };
            return Ok(resp);
        }

        // witness is good
        let resp = VerifyWitnessResult {
            result: VerificationStatus::Valid,
            invalid_reason: None,
            gateway_info: Some(witness_info),
        };

        Ok(resp)
    }
}
