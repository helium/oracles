use crate::{
    error::DecodeError,
    iot_beacon_report::IotBeaconReport,
    iot_witness_report::IotWitnessReport,
    traits::{MsgDecode, MsgTimestamp, TimestampDecode, TimestampEncode},
    Error, Result,
};
use chrono::{DateTime, Utc};
use helium_proto::services::poc_lora::{
    InvalidDetails, InvalidParticipantSide, InvalidReason, LoraBeaconReportReqV1,
    LoraInvalidBeaconReportV1, LoraInvalidWitnessReportV1, LoraWitnessReportReqV1,
};
use serde::Serialize;

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

impl MsgTimestamp<Result<DateTime<Utc>>> for LoraInvalidBeaconReportV1 {
    fn timestamp(&self) -> Result<DateTime<Utc>> {
        self.received_timestamp.to_timestamp_millis()
    }
}

impl MsgTimestamp<u64> for IotInvalidBeaconReport {
    fn timestamp(&self) -> u64 {
        self.received_timestamp.encode_timestamp_millis()
    }
}

impl MsgTimestamp<Result<DateTime<Utc>>> for LoraInvalidWitnessReportV1 {
    fn timestamp(&self) -> Result<DateTime<Utc>> {
        self.received_timestamp.to_timestamp_millis()
    }
}

impl MsgTimestamp<u64> for IotInvalidWitnessReport {
    fn timestamp(&self) -> u64 {
        self.received_timestamp.encode_timestamp_millis()
    }
}

impl TryFrom<LoraInvalidBeaconReportV1> for IotInvalidBeaconReport {
    type Error = Error;
    fn try_from(v: LoraInvalidBeaconReportV1) -> Result<Self> {
        let inv_reason = v.reason;
        let invalid_reason: InvalidReason =
            InvalidReason::from_i32(inv_reason).ok_or_else(|| {
                DecodeError::unsupported_invalid_reason("iot_invalid_beacon_report_v1", inv_reason)
            })?;
        Ok(Self {
            received_timestamp: v.timestamp()?,
            reason: invalid_reason,
            report: v
                .report
                .ok_or_else(|| Error::not_found("iot invalid beacon report v1"))?
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
            location: v
                .location
                .map(|l| l.to_string())
                .unwrap_or_default(),
            gain: v.gain,
            elevation: v.elevation,
            invalid_details: v.invalid_details,
        }
    }
}

impl TryFrom<LoraInvalidWitnessReportV1> for IotInvalidWitnessReport {
    type Error = Error;
    fn try_from(v: LoraInvalidWitnessReportV1) -> Result<Self> {
        let inv_reason = v.reason;
        let invalid_reason: InvalidReason =
            InvalidReason::from_i32(inv_reason).ok_or_else(|| {
                DecodeError::unsupported_invalid_reason("iot_invalid_witness_report_v1", inv_reason)
            })?;
        let participant_side = v.participant_side;
        let side: InvalidParticipantSide = InvalidParticipantSide::from_i32(participant_side)
            .ok_or_else(|| {
                DecodeError::unsupported_participant_side(
                    "iot_invalid_witness_report_v1",
                    participant_side,
                )
            })?;
        let received_timestamp = v.timestamp()?;

        Ok(Self {
            received_timestamp,
            reason: invalid_reason,
            participant_side: side,
            report: v
                .report
                .ok_or_else(|| Error::not_found("iot invalid witness report"))?
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
