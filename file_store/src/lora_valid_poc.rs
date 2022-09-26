use crate::{
    datetime_from_epoch, lora_beacon_report::LoraBeaconReport,
    lora_witness_report::LoraWitnessReport, traits::MsgDecode, Error, Result,
};
use chrono::{DateTime, Utc};
use helium_proto::services::poc_lora::{
    LoraBeaconReportReqV1, LoraValidBeaconReportV1, LoraValidPocV1, LoraValidWitnessReportV1,
    LoraWitnessReportReqV1,
};
use serde::Serialize;

#[derive(Serialize)]
pub struct LoraValidBeaconReport {
    pub received_timestamp: DateTime<Utc>,
    pub location: String,
    pub hex_scale: f32,
    pub report: LoraBeaconReport,
}

#[derive(Serialize)]
pub struct LoraValidWitnessReport {
    pub received_timestamp: DateTime<Utc>,
    pub location: String,
    pub hex_scale: f32,
    pub report: LoraWitnessReport,
}

#[derive(Serialize)]
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
        let beacon_report: LoraValidBeaconReport = v.beacon_report.unwrap().try_into()?;
        let mut witnesses: Vec<LoraValidWitnessReport> = Vec::new();
        for witness_proto in v.witness_reports {
            let witness_report: LoraValidWitnessReport = witness_proto.try_into()?;
            witnesses.push(witness_report)
        }
        Ok(Self {
            poc_id: v.poc_id,
            beacon_report,
            witness_reports: witnesses,
        })
    }
}

impl TryFrom<LoraValidBeaconReportV1> for LoraValidBeaconReport {
    type Error = Error;
    fn try_from(v: LoraValidBeaconReportV1) -> Result<Self> {
        Ok(Self {
            received_timestamp: datetime_from_epoch(v.received_timestamp),
            location: v.location,
            hex_scale: v.hex_scale,
            report: LoraBeaconReport::try_from(v.report.unwrap())?,
        })
    }
}

impl TryFrom<LoraValidBeaconReport> for LoraValidBeaconReportV1 {
    type Error = Error;
    fn try_from(v: LoraValidBeaconReport) -> Result<Self> {
        let report: LoraBeaconReportReqV1 = v.report.into();
        Ok(Self {
            received_timestamp: v.received_timestamp.timestamp() as u64,
            location: v.location,
            hex_scale: v.hex_scale,
            report: Some(report),
        })
    }
}

impl TryFrom<LoraValidWitnessReportV1> for LoraValidWitnessReport {
    type Error = Error;
    fn try_from(v: LoraValidWitnessReportV1) -> Result<Self> {
        Ok(Self {
            received_timestamp: datetime_from_epoch(v.received_timestamp),
            location: v.location,
            hex_scale: v.hex_scale,
            report: LoraWitnessReport::try_from(v.report.unwrap())?,
        })
    }
}

impl TryFrom<LoraValidWitnessReport> for LoraValidWitnessReportV1 {
    type Error = Error;
    fn try_from(v: LoraValidWitnessReport) -> Result<Self> {
        let report: LoraWitnessReportReqV1 = v.report.into();
        Ok(Self {
            received_timestamp: v.received_timestamp.timestamp() as u64,
            location: v.location,
            hex_scale: v.hex_scale,
            report: Some(report),
        })
    }
}
