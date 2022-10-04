use crate::{
    lora_beacon_report::LoraBeaconReport,
    lora_witness_report::LoraWitnessReport,
    traits::{MsgDecode, MsgTimestamp},
    Error, Result,
};
use chrono::{DateTime, Utc};
use helium_proto::services::poc_lora::{
    LoraBeaconReportReqV1, LoraValidBeaconReportV1, LoraValidPocV1, LoraValidWitnessReportV1,
    LoraWitnessReportReqV1,
};
use serde::Serialize;

#[derive(Serialize, Clone)]
pub struct LoraValidBeaconReport {
    pub received_timestamp: DateTime<Utc>,
    pub location: Option<u64>,
    pub hex_scale: f32,
    pub report: LoraBeaconReport,
}

#[derive(Serialize, Clone)]
pub struct LoraValidWitnessReport {
    pub received_timestamp: DateTime<Utc>,
    pub location: Option<u64>,
    pub hex_scale: f32,
    pub report: LoraWitnessReport,
}

#[derive(Serialize, Clone)]
pub struct LoraValidPoc {
    pub poc_id: Vec<u8>,
    pub beacon_report: LoraValidBeaconReport,
    pub witness_reports: Vec<LoraValidWitnessReport>,
}

impl MsgDecode for LoraValidPoc {
    type Msg = LoraValidPocV1;
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
            received_timestamp: v.received_timestamp.to_timestamp_millis()?,
            location: v.location.parse().ok(),
            hex_scale: v.hex_scale,
            report: v
                .report
                .ok_or_else(|| Error::not_found("lora valid beacon report v1"))?
                .try_into()?,
        })
    }
}

impl From<LoraValidBeaconReport> for LoraValidBeaconReportV1 {
    fn from(v: LoraValidBeaconReport) -> Self {
        let report: LoraBeaconReportReqV1 = v.report.into();

        Self {
            received_timestamp: v.received_timestamp.timestamp_millis() as u64,
            location: v
                .location
                .map(|l| l.to_string())
                .unwrap_or_else(String::new),
            hex_scale: v.hex_scale,
            report: Some(report),
        }
    }
}

impl TryFrom<LoraValidWitnessReportV1> for LoraValidWitnessReport {
    type Error = Error;
    fn try_from(v: LoraValidWitnessReportV1) -> Result<Self> {
        Ok(Self {
            received_timestamp: v.received_timestamp.to_timestamp_millis()?,
            location: v.location.parse().ok(),
            hex_scale: v.hex_scale,
            report: v
                .report
                .ok_or_else(|| Error::not_found("lora valid witness port v1"))?
                .try_into()?,
        })
    }
}

impl From<LoraValidWitnessReport> for LoraValidWitnessReportV1 {
    fn from(v: LoraValidWitnessReport) -> Self {
        let report: LoraWitnessReportReqV1 = v.report.into();

        Self {
            received_timestamp: v.received_timestamp.timestamp_millis() as u64,
            location: v
                .location
                .map(|l| l.to_string())
                .unwrap_or_else(String::new),
            hex_scale: v.hex_scale,
            report: Some(report),
        }
    }
}
