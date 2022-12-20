use crate::{
    iot_beacon_report::IotBeaconReport,
    iot_witness_report::IotWitnessReport,
    traits::{MsgDecode, MsgTimestamp, TimestampDecode, TimestampEncode},
    Error, Result,
};
use chrono::{DateTime, Utc};
use density_scaler::SCALING_PRECISION;
use helium_proto::services::poc_iot::{
    IotBeaconReportReqV1, IotValidBeaconReportV1, IotValidPocV1, IotValidWitnessReportV1,
    IotWitnessReportReqV1,
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
pub struct IotValidPoc {
    pub poc_id: Vec<u8>,
    pub beacon_report: IotValidBeaconReport,
    pub witness_reports: Vec<IotValidWitnessReport>,
}

impl MsgDecode for IotValidPoc {
    type Msg = IotValidPocV1;
}

impl MsgTimestamp<Result<DateTime<Utc>>> for IotValidBeaconReportV1 {
    fn timestamp(&self) -> Result<DateTime<Utc>> {
        self.received_timestamp.to_timestamp_millis()
    }
}

impl MsgTimestamp<u64> for IotValidBeaconReport {
    fn timestamp(&self) -> u64 {
        self.received_timestamp.encode_timestamp_millis()
    }
}

impl MsgTimestamp<Result<DateTime<Utc>>> for IotValidWitnessReportV1 {
    fn timestamp(&self) -> Result<DateTime<Utc>> {
        self.received_timestamp.to_timestamp_millis()
    }
}

impl MsgTimestamp<u64> for IotValidWitnessReport {
    fn timestamp(&self) -> u64 {
        self.received_timestamp.encode_timestamp_millis()
    }
}

impl TryFrom<IotValidPocV1> for IotValidPoc {
    type Error = Error;
    fn try_from(v: IotValidPocV1) -> Result<Self> {
        let witnesses = v
            .witness_reports
            .into_iter()
            .map(IotValidWitnessReport::try_from)
            .collect::<Result<Vec<IotValidWitnessReport>>>()?;

        Ok(Self {
            poc_id: v.poc_id,
            witness_reports: witnesses,
            beacon_report: v
                .beacon_report
                .ok_or_else(|| Error::not_found("iot valid poc v1"))?
                .try_into()?,
        })
    }
}

impl From<IotValidPoc> for IotValidPocV1 {
    fn from(v: IotValidPoc) -> Self {
        let witnesses = v.witness_reports.into_iter().map(From::from).collect();

        Self {
            poc_id: v.poc_id,
            beacon_report: Some(v.beacon_report.into()),
            witness_reports: witnesses,
        }
    }
}

impl TryFrom<IotValidBeaconReportV1> for IotValidBeaconReport {
    type Error = Error;
    fn try_from(v: IotValidBeaconReportV1) -> Result<Self> {
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

impl From<IotValidBeaconReport> for IotValidBeaconReportV1 {
    fn from(v: IotValidBeaconReport) -> Self {
        let received_timestamp = v.timestamp();
        let report: IotBeaconReportReqV1 = v.report.into();

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

impl TryFrom<IotValidWitnessReportV1> for IotValidWitnessReport {
    type Error = Error;
    fn try_from(v: IotValidWitnessReportV1) -> Result<Self> {
        let received_timestamp = v.timestamp()?;
        Ok(Self {
            received_timestamp,
            location: v.location.parse().ok(),
            hex_scale: Decimal::new(v.hex_scale as i64, SCALING_PRECISION),
            report: v
                .report
                .ok_or_else(|| Error::not_found("iot valid witness port v1"))?
                .try_into()?,
            reward_unit: Decimal::new(v.reward_unit as i64, SCALING_PRECISION),
        })
    }
}
impl From<IotValidWitnessReport> for IotValidWitnessReportV1 {
    fn from(v: IotValidWitnessReport) -> Self {
        let received_timestamp = v.timestamp();
        let report: IotWitnessReportReqV1 = v.report.into();

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
