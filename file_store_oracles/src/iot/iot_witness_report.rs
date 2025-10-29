use chrono::{DateTime, Utc};
use file_store::traits::{
    MsgDecode, TimestampDecode, TimestampDecodeError, TimestampDecodeResult, TimestampEncode,
};
use helium_crypto::PublicKeyBinary;
use helium_proto::services::poc_lora::{LoraWitnessIngestReportV1, LoraWitnessReportReqV1};
use helium_proto::DataRate;
use serde::Serialize;

use crate::prost_enum;
use crate::traits::MsgTimestamp;

#[derive(thiserror::Error, Debug)]
pub enum IotWitnessError {
    #[error("invalid timestamp: {0}")]
    Timestamp(#[from] TimestampDecodeError),

    #[error("missing field: {0}")]
    MissingField(&'static str),

    #[error("unsupported datarate: {0}")]
    DataRate(prost::UnknownEnumValue),
}

#[derive(Serialize, Clone, Debug)]
pub struct IotWitnessReport {
    #[serde(alias = "pubKey")]
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
pub struct IotWitnessIngestReport {
    pub received_timestamp: DateTime<Utc>,
    pub report: IotWitnessReport,
}

impl MsgDecode for IotWitnessIngestReport {
    type Msg = LoraWitnessIngestReportV1;
}

impl TryFrom<LoraWitnessReportReqV1> for IotWitnessIngestReport {
    type Error = IotWitnessError;

    fn try_from(v: LoraWitnessReportReqV1) -> Result<Self, Self::Error> {
        Ok(Self {
            received_timestamp: Utc::now(),
            report: v.try_into()?,
        })
    }
}

impl TryFrom<LoraWitnessIngestReportV1> for IotWitnessIngestReport {
    type Error = IotWitnessError;

    fn try_from(v: LoraWitnessIngestReportV1) -> Result<Self, Self::Error> {
        Ok(Self {
            received_timestamp: v.timestamp()?,
            report: v
                .report
                .ok_or(IotWitnessError::MissingField(
                    "iot_witness_ingest_report.report",
                ))?
                .try_into()?,
        })
    }
}

impl From<IotWitnessIngestReport> for LoraWitnessReportReqV1 {
    fn from(v: IotWitnessIngestReport) -> Self {
        let timestamp = v.report.timestamp();
        Self {
            pub_key: v.report.pub_key.into(),
            data: v.report.data,
            timestamp,
            signal: v.report.signal,
            snr: v.report.snr,
            frequency: v.report.frequency,
            datarate: 0,
            signature: v.report.signature,
            tmst: v.report.tmst,
        }
    }
}

impl TryFrom<LoraWitnessReportReqV1> for IotWitnessReport {
    type Error = IotWitnessError;

    fn try_from(v: LoraWitnessReportReqV1) -> Result<Self, Self::Error> {
        Ok(Self {
            datarate: prost_enum(v.datarate, IotWitnessError::DataRate)?,
            timestamp: v.timestamp()?,
            pub_key: v.pub_key.into(),
            data: v.data,
            signal: v.signal,
            snr: v.snr,
            frequency: v.frequency,
            signature: v.signature,
            tmst: v.tmst,
        })
    }
}

impl MsgTimestamp<TimestampDecodeResult> for LoraWitnessReportReqV1 {
    fn timestamp(&self) -> TimestampDecodeResult {
        self.timestamp.to_timestamp_nanos()
    }
}

impl MsgTimestamp<u64> for IotWitnessReport {
    fn timestamp(&self) -> u64 {
        self.timestamp.encode_timestamp_nanos()
    }
}

impl MsgTimestamp<TimestampDecodeResult> for LoraWitnessIngestReportV1 {
    fn timestamp(&self) -> TimestampDecodeResult {
        self.received_timestamp.to_timestamp_millis()
    }
}

impl MsgTimestamp<u64> for IotWitnessIngestReport {
    fn timestamp(&self) -> u64 {
        self.received_timestamp.encode_timestamp_millis()
    }
}

impl From<IotWitnessReport> for LoraWitnessReportReqV1 {
    fn from(v: IotWitnessReport) -> Self {
        let timestamp = v.timestamp();
        Self {
            pub_key: v.pub_key.into(),
            data: v.data,
            timestamp,
            signal: v.signal,
            snr: v.snr,
            frequency: v.frequency,
            datarate: v.datarate as i32,
            signature: v.signature,
            tmst: v.tmst,
        }
    }
}
