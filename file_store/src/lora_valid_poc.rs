use crate::{
    lora_beacon_report::LoraBeaconReport,
    lora_witness_report::LoraWitnessReport,
    traits::{MsgDecode, MsgTimestamp, TimestampDecode, TimestampEncode},
    Error, Result,
};
use chrono::{DateTime, Utc};
use density_scaler::SCALING_PRECISION;
use helium_proto::services::poc_lora::{
    LoraBeaconReportReqV1, LoraValidBeaconReportV1, LoraValidPocV1, LoraValidWitnessReportV1,
    LoraWitnessReportReqV1,
};
use rust_decimal::{prelude::ToPrimitive, Decimal};
use serde::Serialize;

const SCALE_MULTIPLIER: Decimal = Decimal::ONE_HUNDRED;

#[derive(Serialize, Clone, Debug)]
pub struct LoraValidBeaconReport {
    pub received_timestamp: DateTime<Utc>,
    pub location: Option<u64>,
    pub hex_scale: Decimal,
    pub report: LoraBeaconReport,
    pub reward_unit: Decimal,
}

#[derive(Serialize, Clone, Debug)]
pub struct LoraValidWitnessReport {
    pub received_timestamp: DateTime<Utc>,
    pub location: Option<u64>,
    pub hex_scale: Decimal,
    pub report: LoraWitnessReport,
    pub reward_unit: Decimal,
}

#[derive(Serialize, Clone, Debug)]
pub struct LoraValidPoc {
    pub poc_id: Vec<u8>,
    pub beacon_report: LoraValidBeaconReport,
    pub witness_reports: Vec<LoraValidWitnessReport>,
}

impl MsgDecode for LoraValidPoc {
    type Msg = LoraValidPocV1;
}

impl MsgTimestamp<Result<DateTime<Utc>>> for LoraValidBeaconReportV1 {
    fn timestamp(&self) -> Result<DateTime<Utc>> {
        self.received_timestamp.to_timestamp_millis()
    }
}

impl MsgTimestamp<u64> for LoraValidBeaconReport {
    fn timestamp(&self) -> u64 {
        self.received_timestamp.encode_timestamp_millis()
    }
}

impl MsgTimestamp<Result<DateTime<Utc>>> for LoraValidWitnessReportV1 {
    fn timestamp(&self) -> Result<DateTime<Utc>> {
        self.received_timestamp.to_timestamp_millis()
    }
}

impl MsgTimestamp<u64> for LoraValidWitnessReport {
    fn timestamp(&self) -> u64 {
        self.received_timestamp.encode_timestamp_millis()
    }
}

impl TryFrom<LoraValidPocV1> for LoraValidPoc {
    type Error = Error;
    fn try_from(v: LoraValidPocV1) -> Result<Self> {
        let witnesses = v
            .witness_reports
            .into_iter()
            .map(LoraValidWitnessReport::try_from)
            .collect::<Result<Vec<LoraValidWitnessReport>>>()?;

        Ok(Self {
            poc_id: v.poc_id,
            witness_reports: witnesses,
            beacon_report: v
                .beacon_report
                .ok_or_else(|| Error::not_found("lora valid poc v1"))?
                .try_into()?,
        })
    }
}

impl From<LoraValidPoc> for LoraValidPocV1 {
    fn from(v: LoraValidPoc) -> Self {
        let witnesses = v.witness_reports.into_iter().map(From::from).collect();

        Self {
            poc_id: v.poc_id,
            beacon_report: Some(v.beacon_report.into()),
            witness_reports: witnesses,
        }
    }
}

impl TryFrom<LoraValidBeaconReportV1> for LoraValidBeaconReport {
    type Error = Error;
    fn try_from(v: LoraValidBeaconReportV1) -> Result<Self> {
        Ok(Self {
            received_timestamp: v.timestamp()?,
            location: v.location.parse().ok(),
            hex_scale: Decimal::new(v.hex_scale as i64, SCALING_PRECISION),
            report: v
                .report
                .ok_or_else(|| Error::not_found("lora valid beacon report v1"))?
                .try_into()?,
            reward_unit: Decimal::new(v.reward_unit as i64, SCALING_PRECISION),
        })
    }
}

impl From<LoraValidBeaconReport> for LoraValidBeaconReportV1 {
    fn from(v: LoraValidBeaconReport) -> Self {
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

impl TryFrom<LoraValidWitnessReportV1> for LoraValidWitnessReport {
    type Error = Error;
    fn try_from(v: LoraValidWitnessReportV1) -> Result<Self> {
        let received_timestamp = v.timestamp()?;
        Ok(Self {
            received_timestamp,
            location: v.location.parse().ok(),
            hex_scale: Decimal::new(v.hex_scale as i64, SCALING_PRECISION),
            report: v
                .report
                .ok_or_else(|| Error::not_found("lora valid witness port v1"))?
                .try_into()?,
            reward_unit: Decimal::new(v.reward_unit as i64, SCALING_PRECISION),
        })
    }
}
impl From<LoraValidWitnessReport> for LoraValidWitnessReportV1 {
    fn from(v: LoraValidWitnessReport) -> Self {
        let received_timestamp = v.timestamp();
        let report: LoraWitnessReportReqV1 = v.report.into();

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
