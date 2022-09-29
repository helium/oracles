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
use std::ops::Not;

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

impl From<LoraValidPoc> for LoraValidPocV1 {
    fn from(v: LoraValidPoc) -> Self {
        let beacon_report: LoraValidBeaconReportV1 = v.beacon_report.into();
        let mut witnesses: Vec<LoraValidWitnessReportV1> = Vec::new();
        for witness_proto in v.witness_reports {
            let witness_report: LoraValidWitnessReportV1 = witness_proto.into();
            witnesses.push(witness_report)
        }
        Self {
            poc_id: v.poc_id,
            beacon_report: Some(beacon_report),
            witness_reports: witnesses,
        }
    }
}

impl TryFrom<LoraValidBeaconReportV1> for LoraValidBeaconReport {
    type Error = Error;
    fn try_from(v: LoraValidBeaconReportV1) -> Result<Self> {
        let location = v
            .location
            .is_empty()
            .not()
            .then(|| v.location.parse::<u64>().unwrap());
        Ok(Self {
            received_timestamp: datetime_from_epoch(v.received_timestamp),
            location,
            hex_scale: v.hex_scale,
            report: LoraBeaconReport::try_from(v.report.unwrap())?,
        })
    }
}

impl From<LoraValidBeaconReport> for LoraValidBeaconReportV1 {
    fn from(v: LoraValidBeaconReport) -> Self {
        let report: LoraBeaconReportReqV1 = v.report.into();
        let location = match v.location {
            None => String::new(),
            Some(loc) => loc.to_string(),
        };
        Self {
            received_timestamp: v.received_timestamp.timestamp() as u64,
            location,
            hex_scale: v.hex_scale,
            report: Some(report),
        }
    }
}

impl TryFrom<LoraValidWitnessReportV1> for LoraValidWitnessReport {
    type Error = Error;
    fn try_from(v: LoraValidWitnessReportV1) -> Result<Self> {
        let location = v
            .location
            .is_empty()
            .not()
            .then(|| v.location.parse::<u64>().unwrap());
        Ok(Self {
            received_timestamp: datetime_from_epoch(v.received_timestamp),
            location,
            hex_scale: v.hex_scale,
            report: LoraWitnessReport::try_from(v.report.unwrap())?,
        })
    }
}

impl From<LoraValidWitnessReport> for LoraValidWitnessReportV1 {
    fn from(v: LoraValidWitnessReport) -> Self {
        let report: LoraWitnessReportReqV1 = v.report.into();
        let location = match v.location {
            None => String::new(),
            Some(loc) => loc.to_string(),
        };
        Self {
            received_timestamp: v.received_timestamp.timestamp() as u64,
            location,
            hex_scale: v.hex_scale,
            report: Some(report),
        }
    }
}
