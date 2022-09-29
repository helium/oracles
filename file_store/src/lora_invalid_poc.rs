use crate::{
    datetime_from_epoch, lora_beacon_report::LoraBeaconReport,
    lora_witness_report::LoraWitnessReport, traits::MsgDecode, Error, Result,
};
use chrono::{DateTime, Utc};
use helium_proto::services::poc_lora::{
    InvalidParticipantSide, InvalidReason, LoraBeaconReportReqV1, LoraInvalidBeaconReportV1,
    LoraInvalidWitnessReportV1, LoraWitnessReportReqV1,
};
use serde::Serialize;

#[derive(Serialize, Clone)]
pub struct LoraInvalidBeaconReport {
    pub received_timestamp: DateTime<Utc>,
    pub reason: InvalidReason,
    pub report: LoraBeaconReport,
}

#[derive(Serialize, Clone)]
pub struct LoraInvalidWitnessReport {
    pub received_timestamp: DateTime<Utc>,
    pub reason: InvalidReason,
    pub report: LoraWitnessReport,
    pub participant_side: InvalidParticipantSide,
}

impl MsgDecode for LoraInvalidBeaconReport {
    type Msg = LoraInvalidBeaconReportV1;
}

impl MsgDecode for LoraInvalidWitnessReport {
    type Msg = LoraInvalidWitnessReportV1;
}

impl TryFrom<LoraInvalidBeaconReportV1> for LoraInvalidBeaconReport {
    type Error = Error;
    fn try_from(v: LoraInvalidBeaconReportV1) -> Result<Self> {
        let invalid_reason: InvalidReason = InvalidReason::from_i32(v.reason)
            .ok_or_else(|| Error::custom("unsupported invalid_reason"))?;
        Ok(Self {
            received_timestamp: datetime_from_epoch(v.received_timestamp),
            reason: invalid_reason,
            report: LoraBeaconReport::try_from(v.report.unwrap())?,
        })
    }
}

impl From<LoraInvalidBeaconReport> for LoraInvalidBeaconReportV1 {
    fn from(v: LoraInvalidBeaconReport) -> Self {
        let report: LoraBeaconReportReqV1 = v.report.into();
        Self {
            received_timestamp: v.received_timestamp.timestamp() as u64,
            reason: v.reason as i32,
            report: Some(report),
        }
    }
}

impl TryFrom<LoraInvalidWitnessReportV1> for LoraInvalidWitnessReport {
    type Error = Error;
    fn try_from(v: LoraInvalidWitnessReportV1) -> Result<Self> {
        let invalid_reason: InvalidReason = InvalidReason::from_i32(v.reason)
            .ok_or_else(|| Error::custom("unsupported invalid_reason"))?;
        let side: InvalidParticipantSide = InvalidParticipantSide::from_i32(v.participant_side)
            .ok_or_else(|| Error::custom("unsupported participant_side"))?;

        Ok(Self {
            received_timestamp: datetime_from_epoch(v.received_timestamp),
            reason: invalid_reason,
            report: LoraWitnessReport::try_from(v.report.unwrap())?,
            participant_side: side,
        })
    }
}
impl From<LoraInvalidWitnessReport> for LoraInvalidWitnessReportV1 {
    fn from(v: LoraInvalidWitnessReport) -> Self {
        let report: LoraWitnessReportReqV1 = v.report.into();
        Self {
            received_timestamp: v.received_timestamp.timestamp() as u64,
            reason: v.reason as i32,
            report: Some(report),
            participant_side: v.participant_side as i32,
        }
    }
}
