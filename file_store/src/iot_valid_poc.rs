use crate::{
    error::DecodeError,
    iot_beacon_report::IotBeaconReport,
    iot_witness_report::IotWitnessReport,
    traits::{MsgDecode, MsgTimestamp, TimestampDecode, TimestampEncode},
    Error, Result,
};
use chrono::{DateTime, Utc};
use density_scaler::SCALING_PRECISION;
use helium_proto::services::poc_lora::{
    InvalidParticipantSide, InvalidReason, LoraBeaconReportReqV1, LoraPocV1,
    LoraValidBeaconReportV1, LoraVerifiedWitnessReportV1, LoraWitnessReportReqV1,
    VerificationStatus,
};

use rust_decimal::{prelude::ToPrimitive, Decimal};
use rust_decimal_macros::dec;
use serde::Serialize;

const SCALE_MULTIPLIER: Decimal = dec!(10000);

#[derive(Serialize, Clone, Debug)]
pub struct IotValidBeaconReport {
    pub received_timestamp: DateTime<Utc>,
    pub location: Option<u64>,
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
    pub hex_scale: Decimal,
    pub reward_unit: Decimal,
    pub invalid_reason: InvalidReason,
    pub participant_side: InvalidParticipantSide,
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

impl MsgTimestamp<Result<DateTime<Utc>>> for LoraValidBeaconReportV1 {
    fn timestamp(&self) -> Result<DateTime<Utc>> {
        self.received_timestamp.to_timestamp_millis()
    }
}

impl MsgTimestamp<u64> for IotValidBeaconReport {
    fn timestamp(&self) -> u64 {
        self.received_timestamp.encode_timestamp_millis()
    }
}

impl MsgTimestamp<Result<DateTime<Utc>>> for LoraVerifiedWitnessReportV1 {
    fn timestamp(&self) -> Result<DateTime<Utc>> {
        self.received_timestamp.to_timestamp_millis()
    }
}

impl MsgTimestamp<u64> for IotVerifiedWitnessReport {
    fn timestamp(&self) -> u64 {
        self.received_timestamp.encode_timestamp_millis()
    }
}

impl TryFrom<LoraPocV1> for IotPoc {
    type Error = Error;
    fn try_from(v: LoraPocV1) -> Result<Self> {
        let selected_witnesses = v
            .selected_witnesses
            .into_iter()
            .map(IotVerifiedWitnessReport::try_from)
            .collect::<Result<Vec<IotVerifiedWitnessReport>>>()?;
        let unselected_witnesses = v
            .unselected_witnesses
            .into_iter()
            .map(IotVerifiedWitnessReport::try_from)
            .collect::<Result<Vec<IotVerifiedWitnessReport>>>()?;

        Ok(Self {
            poc_id: v.poc_id,
            beacon_report: v
                .beacon_report
                .ok_or_else(|| Error::not_found("iot poc v1"))?
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
    type Error = Error;
    fn try_from(v: LoraValidBeaconReportV1) -> Result<Self> {
        Ok(Self {
            received_timestamp: v.timestamp()?,
            location: v.location.parse().ok(),
            hex_scale: Decimal::new(v.hex_scale as i64, SCALING_PRECISION),
            report: v
                .report
                .ok_or_else(|| Error::not_found("iot valid beacon report v1"))?
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
            location: v
                .location
                .map(|l| l.to_string())
                .unwrap_or_else(String::new),
            hex_scale: (v.hex_scale * SCALE_MULTIPLIER).to_u32().unwrap_or(0),
            report: Some(report),
            reward_unit: (v.reward_unit * SCALE_MULTIPLIER).to_u32().unwrap_or(0),
        }
    }
}

impl TryFrom<LoraVerifiedWitnessReportV1> for IotVerifiedWitnessReport {
    type Error = Error;
    fn try_from(v: LoraVerifiedWitnessReportV1) -> Result<Self> {
        let received_timestamp = v.timestamp()?;
        let status = VerificationStatus::from_i32(v.status).ok_or_else(|| {
            DecodeError::unsupported_status_reason("iot_verified_witness_report_v1", v.status)
        })?;
        let invalid_reason = InvalidReason::from_i32(v.invalid_reason).ok_or_else(|| {
            DecodeError::unsupported_invalid_reason(
                "iot_verified_witness_report_v1",
                v.invalid_reason,
            )
        })?;
        let participant_side =
            InvalidParticipantSide::from_i32(v.participant_side).ok_or_else(|| {
                DecodeError::unsupported_participant_side(
                    "iot_verified_witness_report_v1",
                    v.participant_side,
                )
            })?;

        Ok(Self {
            received_timestamp,
            status,
            report: v
                .report
                .ok_or_else(|| Error::not_found("iot valid witness port v1"))?
                .try_into()?,
            location: v.location.parse().ok(),
            hex_scale: Decimal::new(v.hex_scale as i64, SCALING_PRECISION),
            reward_unit: Decimal::new(v.reward_unit as i64, SCALING_PRECISION),
            invalid_reason,
            participant_side,
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
            location: v
                .location
                .map(|l| l.to_string())
                .unwrap_or_else(String::new),
            hex_scale: (v.hex_scale * SCALE_MULTIPLIER).to_u32().unwrap_or(0),
            reward_unit: (v.reward_unit * SCALE_MULTIPLIER).to_u32().unwrap_or(0),
            invalid_reason: v.invalid_reason as i32,
            participant_side: v.participant_side as i32,
        }
    }
}
