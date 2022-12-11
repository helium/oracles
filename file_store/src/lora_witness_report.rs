use crate::{
    error::DecodeError,
    traits::MsgDecode,
    traits::{MsgTimestamp, TimestampDecode, TimestampEncode},
    Error, Result,
};
use chrono::{DateTime, Utc};
use helium_crypto::PublicKeyBinary;
use helium_proto::services::poc_lora::{LoraWitnessIngestReportV1, LoraWitnessReportReqV1};
use helium_proto::DataRate;
use serde::Serialize;

#[derive(Serialize, Clone, Debug)]
pub struct LoraWitnessReport {
    pub pub_key: PublicKeyBinary,
    pub data: Vec<u8>,
    pub timestamp: DateTime<Utc>,
    pub tmst: u32,
    pub signal: i32,
    pub snr: i32,
    pub frequency: u64,
    pub datarate: DataRate,
    pub signature: Vec<u8>,
}

#[derive(Serialize, Clone, Debug)]
pub struct LoraWitnessIngestReport {
    pub received_timestamp: DateTime<Utc>,
    pub report: LoraWitnessReport,
}

impl MsgDecode for LoraWitnessIngestReport {
    type Msg = LoraWitnessIngestReportV1;
}

impl TryFrom<LoraWitnessReportReqV1> for LoraWitnessIngestReport {
    type Error = Error;
    fn try_from(v: LoraWitnessReportReqV1) -> Result<Self> {
        Ok(Self {
            received_timestamp: Utc::now(),
            report: v.try_into()?,
        })
    }
}

impl TryFrom<LoraWitnessIngestReportV1> for LoraWitnessIngestReport {
    type Error = Error;
    fn try_from(v: LoraWitnessIngestReportV1) -> Result<Self> {
        Ok(Self {
            received_timestamp: v.timestamp()?,
            report: v
                .report
                .ok_or_else(|| Error::not_found("lora witness ingest report v1"))?
                .try_into()?,
        })
    }
}

impl From<LoraWitnessIngestReport> for LoraWitnessReportReqV1 {
    fn from(v: LoraWitnessIngestReport) -> Self {
        let timestamp = v.report.timestamp();
        Self {
            pub_key: v.report.pub_key.into(),
            data: v.report.data,
            timestamp,
            signal: v.report.signal,
            snr: v.report.snr,
            frequency: v.report.frequency,
            datarate: 0,
            signature: vec![],
            tmst: v.report.tmst,
        }
    }
}

impl TryFrom<LoraWitnessReportReqV1> for LoraWitnessReport {
    type Error = Error;
    fn try_from(v: LoraWitnessReportReqV1) -> Result<Self> {
        let dr = v.datarate;
        let data_rate: DataRate = DataRate::from_i32(dr)
            .ok_or_else(|| DecodeError::unsupported_datarate("lora_witness_report_req_v1", dr))?;
        let timestamp = v.timestamp()?;

        Ok(Self {
            pub_key: v.pub_key.into(),
            data: v.data,
            timestamp,
            signal: v.signal,
            snr: v.snr,
            frequency: v.frequency,
            datarate: data_rate,
            signature: v.signature,
            tmst: v.tmst,
        })
    }
}

impl MsgTimestamp<Result<DateTime<Utc>>> for LoraWitnessReportReqV1 {
    fn timestamp(&self) -> Result<DateTime<Utc>> {
        self.timestamp.to_timestamp_nanos()
    }
}

impl MsgTimestamp<u64> for LoraWitnessReport {
    fn timestamp(&self) -> u64 {
        self.timestamp.encode_timestamp_nanos()
    }
}

impl MsgTimestamp<Result<DateTime<Utc>>> for LoraWitnessIngestReportV1 {
    fn timestamp(&self) -> Result<DateTime<Utc>> {
        self.received_timestamp.to_timestamp_millis()
    }
}

impl MsgTimestamp<u64> for LoraWitnessIngestReport {
    fn timestamp(&self) -> u64 {
        self.received_timestamp.encode_timestamp_millis()
    }
}

impl From<LoraWitnessReport> for LoraWitnessReportReqV1 {
    fn from(v: LoraWitnessReport) -> Self {
        let timestamp = v.timestamp();
        Self {
            pub_key: v.pub_key.into(),
            data: v.data,
            timestamp,
            signal: v.signal,
            snr: v.snr,
            frequency: v.frequency,
            datarate: 0,
            signature: vec![],
            tmst: v.tmst,
        }
    }
}
