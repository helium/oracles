use crate::{
    error::DecodeError,
    traits::MsgDecode,
    traits::{MsgTimestamp, TimestampDecode, TimestampEncode},
    Error, Result,
};
use chrono::{DateTime, Utc};
use helium_crypto::PublicKeyBinary;
use helium_proto::services::poc_lora::{LoraBeaconIngestReportV1, LoraBeaconReportReqV1};
use helium_proto::DataRate;
use serde::Serialize;

#[derive(Serialize, Clone, Debug)]
pub struct LoraBeaconReport {
    pub pub_key: PublicKeyBinary,
    pub local_entropy: Vec<u8>,
    pub remote_entropy: Vec<u8>,
    pub data: Vec<u8>,
    pub frequency: u64,
    pub channel: i32,
    pub datarate: DataRate,
    pub tx_power: i32,
    pub timestamp: DateTime<Utc>,
    pub signature: Vec<u8>,
    pub tmst: u32,
}

#[derive(Serialize, Clone, Debug)]
pub struct LoraBeaconIngestReport {
    pub received_timestamp: DateTime<Utc>,
    pub report: LoraBeaconReport,
}

impl MsgDecode for LoraBeaconIngestReport {
    type Msg = LoraBeaconIngestReportV1;
}

impl TryFrom<LoraBeaconReportReqV1> for LoraBeaconIngestReport {
    type Error = Error;
    fn try_from(v: LoraBeaconReportReqV1) -> Result<Self> {
        Ok(Self {
            received_timestamp: Utc::now(),
            report: v.try_into()?,
        })
    }
}

impl MsgTimestamp<Result<DateTime<Utc>>> for LoraBeaconReportReqV1 {
    fn timestamp(&self) -> Result<DateTime<Utc>> {
        self.timestamp.to_timestamp_nanos()
    }
}

impl MsgTimestamp<u64> for LoraBeaconReport {
    fn timestamp(&self) -> u64 {
        self.timestamp.encode_timestamp_nanos()
    }
}

impl MsgTimestamp<Result<DateTime<Utc>>> for LoraBeaconIngestReportV1 {
    fn timestamp(&self) -> Result<DateTime<Utc>> {
        self.received_timestamp.to_timestamp_millis()
    }
}

impl MsgTimestamp<u64> for LoraBeaconIngestReport {
    fn timestamp(&self) -> u64 {
        self.received_timestamp.encode_timestamp_millis()
    }
}

impl TryFrom<LoraBeaconIngestReportV1> for LoraBeaconIngestReport {
    type Error = Error;
    fn try_from(v: LoraBeaconIngestReportV1) -> Result<Self> {
        Ok(Self {
            received_timestamp: v.timestamp()?,
            report: v
                .report
                .ok_or_else(|| Error::not_found("lora beacon ingest report"))?
                .try_into()?,
        })
    }
}

impl From<LoraBeaconIngestReport> for LoraBeaconReportReqV1 {
    fn from(v: LoraBeaconIngestReport) -> Self {
        let timestamp = v.report.timestamp();
        Self {
            pub_key: v.report.pub_key.into(),
            local_entropy: v.report.local_entropy,
            remote_entropy: v.report.remote_entropy,
            data: v.report.data,
            frequency: v.report.frequency,
            channel: v.report.channel,
            datarate: v.report.datarate as i32,
            tx_power: v.report.tx_power,
            timestamp,
            signature: vec![],
            tmst: v.report.tmst,
        }
    }
}

impl TryFrom<LoraBeaconReportReqV1> for LoraBeaconReport {
    type Error = Error;
    fn try_from(v: LoraBeaconReportReqV1) -> Result<Self> {
        let dr = v.datarate;
        let data_rate: DataRate = DataRate::from_i32(dr)
            .ok_or_else(|| DecodeError::unsupported_datarate("lora_beacon_report_req_v1", dr))?;
        let timestamp = v.timestamp()?;

        Ok(Self {
            pub_key: v.pub_key.into(),
            local_entropy: v.local_entropy,
            remote_entropy: v.remote_entropy,
            data: v.data,
            frequency: v.frequency,
            channel: v.channel,
            datarate: data_rate,
            tx_power: v.tx_power,
            timestamp,
            signature: v.signature,
            tmst: v.tmst,
        })
    }
}

impl From<LoraBeaconReport> for LoraBeaconReportReqV1 {
    fn from(v: LoraBeaconReport) -> Self {
        let timestamp = v.timestamp();
        Self {
            pub_key: v.pub_key.into(),
            local_entropy: v.local_entropy,
            remote_entropy: v.remote_entropy,
            data: v.data,
            frequency: v.frequency,
            channel: v.channel,
            //TODO: fix datarate
            datarate: 0,
            tx_power: v.tx_power,
            timestamp,
            signature: vec![],
            tmst: v.tmst,
        }
    }
}
