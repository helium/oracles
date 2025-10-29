use chrono::{DateTime, Utc};
use file_store::traits::{
    MsgDecode, TimestampDecode, TimestampDecodeError, TimestampDecodeResult, TimestampEncode,
};
use helium_proto::services::poc_lora::{
    InvalidDetails, InvalidParticipantSide, InvalidReason, LoraBeaconReportReqV1, LoraPocV1,
    LoraValidBeaconReportV1, LoraVerifiedWitnessReportV1, LoraWitnessReportReqV1,
    VerificationStatus,
};
use rust_decimal::{dec, prelude::ToPrimitive, Decimal};
use serde::Serialize;

use crate::{
    iot_beacon_report::{IotBeaconError, IotBeaconReport},
    iot_witness_report::{IotWitnessError, IotWitnessReport},
    prost_enum,
    traits::MsgTimestamp,
};

const SCALE_MULTIPLIER: Decimal = dec!(10000);
pub const SCALING_PRECISION: u32 = 4;

#[derive(thiserror::Error, Debug)]
pub enum IotPocError {
    #[error("invalid timestamp: {0}")]
    Timestamp(#[from] TimestampDecodeError),

    #[error("invalid witness report: {0}")]
    WitnessReport(#[from] IotWitnessError),

    #[error("invalid beacon report: {0}")]
    BeaconReport(#[from] IotBeaconError),

    #[error("missing field: {0}")]
    MissingField(&'static str),

    #[error("unsupported verification status: {0}")]
    VerificationStatus(prost::UnknownEnumValue),

    #[error("unsupported invalid reason: {0}")]
    InvalidReason(prost::UnknownEnumValue),

    #[error("unsupported participant side: {0}")]
    ParticipantSide(prost::UnknownEnumValue),
}

#[derive(Serialize, Clone, Debug)]
pub struct IotValidBeaconReport {
    pub received_timestamp: DateTime<Utc>,
    pub location: Option<u64>,
    pub gain: i32,
    pub elevation: i32,
    pub hex_scale: Decimal,
    pub report: IotBeaconReport,
    pub reward_unit: Decimal,
}

#[derive(Serialize, Clone, Debug)]
pub struct IotValidWitnessReport {
    pub received_timestamp: DateTime<Utc>,
    pub location: Option<u64>,
    pub hex_scale: Decimal,
    pub report: IotWitnessReport,
    pub reward_unit: Decimal,
}

#[derive(Serialize, Clone, Debug)]
pub struct IotVerifiedWitnessReport {
    pub received_timestamp: DateTime<Utc>,
    pub status: VerificationStatus,
    pub report: IotWitnessReport,
    pub location: Option<u64>,
    pub gain: i32,
    pub elevation: i32,
    pub hex_scale: Decimal,
    pub reward_unit: Decimal,
    pub invalid_reason: InvalidReason,
    pub participant_side: InvalidParticipantSide,
    pub invalid_details: Option<InvalidDetails>,
}

#[derive(Serialize, Clone, Debug)]
pub struct IotPoc {
    pub poc_id: Vec<u8>,
    pub beacon_report: IotValidBeaconReport,
    pub selected_witnesses: Vec<IotVerifiedWitnessReport>,
    pub unselected_witnesses: Vec<IotVerifiedWitnessReport>,
}

impl MsgDecode for IotPoc {
    type Msg = LoraPocV1;
}

impl MsgTimestamp<TimestampDecodeResult> for LoraValidBeaconReportV1 {
    fn timestamp(&self) -> TimestampDecodeResult {
        self.received_timestamp.to_timestamp_millis()
    }
}

impl MsgTimestamp<u64> for IotValidBeaconReport {
    fn timestamp(&self) -> u64 {
        self.received_timestamp.encode_timestamp_millis()
    }
}

impl MsgTimestamp<TimestampDecodeResult> for LoraVerifiedWitnessReportV1 {
    fn timestamp(&self) -> TimestampDecodeResult {
        self.received_timestamp.to_timestamp_millis()
    }
}

impl MsgTimestamp<u64> for IotVerifiedWitnessReport {
    fn timestamp(&self) -> u64 {
        self.received_timestamp.encode_timestamp_millis()
    }
}

impl TryFrom<LoraPocV1> for IotPoc {
    type Error = IotPocError;

    fn try_from(v: LoraPocV1) -> Result<Self, Self::Error> {
        let selected_witnesses = v
            .selected_witnesses
            .into_iter()
            .map(IotVerifiedWitnessReport::try_from)
            .collect::<Result<Vec<_>, _>>()?;
        let unselected_witnesses = v
            .unselected_witnesses
            .into_iter()
            .map(IotVerifiedWitnessReport::try_from)
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self {
            poc_id: v.poc_id,
            beacon_report: v
                .beacon_report
                .ok_or(IotPocError::MissingField("iot_poc.beacon_report"))?
                .try_into()?,
            selected_witnesses,
            unselected_witnesses,
        })
    }
}

impl From<IotPoc> for LoraPocV1 {
    fn from(v: IotPoc) -> Self {
        let selected_witnesses = v.selected_witnesses.into_iter().map(From::from).collect();
        let unselected_witnesses = v.unselected_witnesses.into_iter().map(From::from).collect();
        Self {
            poc_id: v.poc_id,
            beacon_report: Some(v.beacon_report.into()),
            selected_witnesses,
            unselected_witnesses,
        }
    }
}

impl TryFrom<LoraValidBeaconReportV1> for IotValidBeaconReport {
    type Error = IotPocError;

    fn try_from(v: LoraValidBeaconReportV1) -> Result<Self, Self::Error> {
        Ok(Self {
            received_timestamp: v.timestamp()?,
            location: v.location.parse().ok(),
            gain: v.gain,
            elevation: v.elevation,
            hex_scale: Decimal::new(v.hex_scale as i64, SCALING_PRECISION),
            report: v
                .report
                .ok_or(IotPocError::MissingField("iot_valid_beacon_report.report"))?
                .try_into()?,
            reward_unit: Decimal::new(v.reward_unit as i64, SCALING_PRECISION),
        })
    }
}

impl From<IotValidBeaconReport> for LoraValidBeaconReportV1 {
    fn from(v: IotValidBeaconReport) -> Self {
        let received_timestamp = v.timestamp();
        let report: LoraBeaconReportReqV1 = v.report.into();

        Self {
            received_timestamp,
            location: v.location.map(|l| l.to_string()).unwrap_or_default(),
            gain: v.gain,
            elevation: v.elevation,
            hex_scale: (v.hex_scale * SCALE_MULTIPLIER).to_u32().unwrap_or(0),
            report: Some(report),
            reward_unit: (v.reward_unit * SCALE_MULTIPLIER).to_u32().unwrap_or(0),
        }
    }
}

impl TryFrom<LoraVerifiedWitnessReportV1> for IotVerifiedWitnessReport {
    type Error = IotPocError;

    fn try_from(v: LoraVerifiedWitnessReportV1) -> Result<Self, Self::Error> {
        Ok(Self {
            received_timestamp: v.timestamp()?,
            status: prost_enum(v.status, IotPocError::VerificationStatus)?,
            report: v
                .report
                .ok_or(IotPocError::MissingField(
                    "iot_verified_witness_report.report",
                ))?
                .try_into()?,
            location: v.location.parse().ok(),
            gain: v.gain,
            elevation: v.elevation,
            hex_scale: Decimal::new(v.hex_scale as i64, SCALING_PRECISION),
            reward_unit: Decimal::new(v.reward_unit as i64, SCALING_PRECISION),
            invalid_reason: prost_enum(v.invalid_reason, IotPocError::InvalidReason)?,
            participant_side: prost_enum(v.participant_side, IotPocError::ParticipantSide)?,
            invalid_details: v.invalid_details,
        })
    }
}

impl From<IotVerifiedWitnessReport> for LoraVerifiedWitnessReportV1 {
    fn from(v: IotVerifiedWitnessReport) -> Self {
        let received_timestamp = v.timestamp();
        let report: LoraWitnessReportReqV1 = v.report.into();
        Self {
            received_timestamp,
            status: v.status.into(),
            report: Some(report),
            location: v.location.map(|l| l.to_string()).unwrap_or_default(),
            gain: v.gain,
            elevation: v.elevation,
            hex_scale: (v.hex_scale * SCALE_MULTIPLIER).to_u32().unwrap_or(0),
            reward_unit: (v.reward_unit * SCALE_MULTIPLIER).to_u32().unwrap_or(0),
            invalid_reason: v.invalid_reason as i32,
            participant_side: v.participant_side as i32,
            invalid_details: v.invalid_details,
        }
    }
}

impl IotVerifiedWitnessReport {
    pub fn valid(
        report: &IotWitnessReport,
        received_timestamp: DateTime<Utc>,
        location: Option<u64>,
        gain: i32,
        elevation: i32,
        hex_scale: Decimal,
    ) -> IotVerifiedWitnessReport {
        Self {
            received_timestamp,
            status: VerificationStatus::Valid,
            invalid_reason: InvalidReason::ReasonNone,
            report: report.clone(),
            location,
            gain,
            elevation,
            hex_scale,
            // default reward units to zero until we've got the full count of
            // valid, non-failed witnesses for the final validated poc report
            reward_unit: Decimal::ZERO,
            participant_side: InvalidParticipantSide::SideNone,
            invalid_details: None,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn invalid(
        invalid_reason: InvalidReason,
        invalid_details: Option<InvalidDetails>,
        report: &IotWitnessReport,
        received_timestamp: DateTime<Utc>,
        location: Option<u64>,
        gain: i32,
        elevation: i32,
        participant_side: InvalidParticipantSide,
    ) -> IotVerifiedWitnessReport {
        Self {
            received_timestamp,
            status: VerificationStatus::Invalid,
            invalid_reason,
            invalid_details,
            report: report.clone(),
            location,
            gain,
            elevation,
            hex_scale: Decimal::ZERO,
            // default reward units to zero until we've got the full count of
            // valid, non-failed witnesses for the final validated poc report
            reward_unit: Decimal::ZERO,
            participant_side,
        }
    }
}
