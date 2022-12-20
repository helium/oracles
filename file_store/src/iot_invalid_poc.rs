use crate::{
    error::DecodeError,
    iot_beacon_report::IotBeaconReport,
    iot_witness_report::IotWitnessReport,
    traits::{MsgDecode, MsgTimestamp, TimestampDecode, TimestampEncode},
    Error, Result,
};
use chrono::{DateTime, Utc};
use helium_proto::services::poc_iot::{
    InvalidParticipantSide, InvalidReason, IotBeaconReportReqV1, IotInvalidBeaconReportV1,
    IotInvalidWitnessReportV1, IotWitnessReportReqV1,
};
use serde::Serialize;

#[derive(Serialize, Clone)]
pub struct IotInvalidBeaconReport {
    pub received_timestamp: DateTime<Utc>,
    pub reason: InvalidReason,
    pub report: IotBeaconReport,
}

#[derive(Serialize, Clone)]
pub struct IotInvalidWitnessReport {
    pub received_timestamp: DateTime<Utc>,
    pub reason: InvalidReason,
    pub report: IotWitnessReport,
    pub participant_side: InvalidParticipantSide,
}

impl MsgDecode for IotInvalidBeaconReport {
    type Msg = IotInvalidBeaconReportV1;
}

impl MsgDecode for IotInvalidWitnessReport {
    type Msg = IotInvalidWitnessReportV1;
}

impl MsgTimestamp<Result<DateTime<Utc>>> for IotInvalidBeaconReportV1 {
    fn timestamp(&self) -> Result<DateTime<Utc>> {
        self.received_timestamp.to_timestamp_millis()
    }
}

impl MsgTimestamp<u64> for IotInvalidBeaconReport {
    fn timestamp(&self) -> u64 {
        self.received_timestamp.encode_timestamp_millis()
    }
}

impl MsgTimestamp<Result<DateTime<Utc>>> for IotInvalidWitnessReportV1 {
    fn timestamp(&self) -> Result<DateTime<Utc>> {
        self.received_timestamp.to_timestamp_millis()
    }
}

impl MsgTimestamp<u64> for IotInvalidWitnessReport {
    fn timestamp(&self) -> u64 {
        self.received_timestamp.encode_timestamp_millis()
    }
}

impl TryFrom<IotInvalidBeaconReportV1> for IotInvalidBeaconReport {
    type Error = Error;
    fn try_from(v: IotInvalidBeaconReportV1) -> Result<Self> {
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
        })
    }
}

impl From<IotInvalidBeaconReport> for IotInvalidBeaconReportV1 {
    fn from(v: IotInvalidBeaconReport) -> Self {
        let received_timestamp = v.timestamp();
        let report: IotBeaconReportReqV1 = v.report.into();
        Self {
            received_timestamp,
            reason: v.reason as i32,
            report: Some(report),
        }
    }
}

impl TryFrom<IotInvalidWitnessReportV1> for IotInvalidWitnessReport {
    type Error = Error;
    fn try_from(v: IotInvalidWitnessReportV1) -> Result<Self> {
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
        })
    }
}

impl From<IotInvalidWitnessReport> for IotInvalidWitnessReportV1 {
    fn from(v: IotInvalidWitnessReport) -> Self {
        let received_timestamp = v.timestamp();
        let report: IotWitnessReportReqV1 = v.report.into();
        Self {
            received_timestamp,
            reason: v.reason as i32,
            report: Some(report),
            participant_side: v.participant_side as i32,
        }
    }
}
