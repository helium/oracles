use chrono::{DateTime, Utc};
use file_store::traits::{
    MsgDecode, TimestampDecode, TimestampDecodeError, TimestampDecodeResult, TimestampEncode,
};
use helium_proto::services::poc_lora::{
    InvalidDetails, InvalidParticipantSide, InvalidReason, LoraBeaconReportReqV1,
    LoraInvalidBeaconReportV1, LoraInvalidWitnessReportV1, LoraWitnessReportReqV1,
};
use serde::Serialize;

use crate::{
    iot_beacon_report::{IotBeaconError, IotBeaconReport},
    iot_witness_report::{IotWitnessError, IotWitnessReport},
    prost_enum,
    traits::MsgTimestamp,
};

#[derive(thiserror::Error, Debug)]
pub enum IotInvalidBeaconError {
    #[error("invalid timestamp: {0}")]
    Timestamp(#[from] TimestampDecodeError),

    #[error("missing field: {0}")]
    MissingField(&'static str),

    #[error("unsupported reason: {0}")]
    InvalidReason(prost::UnknownEnumValue),

    #[error("unsupported participant side: {0}")]
    InvalidParticipantSide(prost::UnknownEnumValue),

    #[error("invalid witness: {0}")]
    InvalidWitness(#[from] IotWitnessError),

    #[error("invalid beacon: {0}")]
    InvalidBeacon(#[from] IotBeaconError),
}

#[derive(Serialize, Clone)]
pub struct IotInvalidBeaconReport {
    pub received_timestamp: DateTime<Utc>,
    pub reason: InvalidReason,
    pub invalid_details: Option<InvalidDetails>,
    pub report: IotBeaconReport,
    pub location: Option<u64>,
    pub gain: i32,
    pub elevation: i32,
}

#[derive(Serialize, Clone)]
pub struct IotInvalidWitnessReport {
    pub received_timestamp: DateTime<Utc>,
    pub reason: InvalidReason,
    pub invalid_details: Option<InvalidDetails>,
    pub report: IotWitnessReport,
    pub participant_side: InvalidParticipantSide,
}

impl MsgDecode for IotInvalidBeaconReport {
    type Msg = LoraInvalidBeaconReportV1;
}

impl MsgDecode for IotInvalidWitnessReport {
    type Msg = LoraInvalidWitnessReportV1;
}

impl MsgTimestamp<TimestampDecodeResult> for LoraInvalidBeaconReportV1 {
    fn timestamp(&self) -> TimestampDecodeResult {
        self.received_timestamp.to_timestamp_millis()
    }
}

impl MsgTimestamp<u64> for IotInvalidBeaconReport {
    fn timestamp(&self) -> u64 {
        self.received_timestamp.encode_timestamp_millis()
    }
}

impl MsgTimestamp<TimestampDecodeResult> for LoraInvalidWitnessReportV1 {
    fn timestamp(&self) -> TimestampDecodeResult {
        self.received_timestamp.to_timestamp_millis()
    }
}

impl MsgTimestamp<u64> for IotInvalidWitnessReport {
    fn timestamp(&self) -> u64 {
        self.received_timestamp.encode_timestamp_millis()
    }
}

impl TryFrom<LoraInvalidBeaconReportV1> for IotInvalidBeaconReport {
    type Error = IotInvalidBeaconError;

    fn try_from(v: LoraInvalidBeaconReportV1) -> Result<Self, Self::Error> {
        Ok(Self {
            received_timestamp: v.timestamp()?,
            reason: prost_enum(v.reason, IotInvalidBeaconError::InvalidReason)?,
            report: v
                .report
                .ok_or(IotInvalidBeaconError::MissingField(
                    "iot_invalid_beacon_report.report",
                ))?
                .try_into()?,
            location: v.location.parse().ok(),
            gain: v.gain,
            elevation: v.elevation,
            invalid_details: v.invalid_details,
        })
    }
}

impl From<IotInvalidBeaconReport> for LoraInvalidBeaconReportV1 {
    fn from(v: IotInvalidBeaconReport) -> Self {
        let received_timestamp = v.timestamp();
        let report: LoraBeaconReportReqV1 = v.report.into();
        Self {
            received_timestamp,
            reason: v.reason as i32,
            report: Some(report),
            location: v.location.map(|l| l.to_string()).unwrap_or_default(),
            gain: v.gain,
            elevation: v.elevation,
            invalid_details: v.invalid_details,
        }
    }
}

impl TryFrom<LoraInvalidWitnessReportV1> for IotInvalidWitnessReport {
    type Error = IotInvalidBeaconError;

    fn try_from(v: LoraInvalidWitnessReportV1) -> Result<Self, Self::Error> {
        Ok(Self {
            received_timestamp: v.timestamp()?,
            reason: prost_enum(v.reason, IotInvalidBeaconError::InvalidReason)?,
            participant_side: prost_enum(
                v.participant_side,
                IotInvalidBeaconError::InvalidParticipantSide,
            )?,

            report: v
                .report
                .ok_or(IotInvalidBeaconError::MissingField(
                    "iot_invalid_witness_report.report",
                ))?
                .try_into()?,
            invalid_details: v.invalid_details,
        })
    }
}

impl From<IotInvalidWitnessReport> for LoraInvalidWitnessReportV1 {
    fn from(v: IotInvalidWitnessReport) -> Self {
        let received_timestamp = v.timestamp();
        let report: LoraWitnessReportReqV1 = v.report.into();
        Self {
            received_timestamp,
            reason: v.reason as i32,
            report: Some(report),
            participant_side: v.participant_side as i32,
            invalid_details: v.invalid_details,
        }
    }
}
