use beacon;
use chrono::{DateTime, Utc};
use file_store::traits::{
    MsgDecode, TimestampDecode, TimestampDecodeError, TimestampDecodeResult, TimestampEncode,
};
use helium_crypto::PublicKeyBinary;
use helium_proto::{
    services::poc_lora::{LoraBeaconIngestReportV1, LoraBeaconReportReqV1},
    DataRate,
};
use serde::Serialize;

use crate::{prost_enum, traits::MsgTimestamp};

#[derive(thiserror::Error, Debug)]
pub enum IotBeaconError {
    #[error("invalid timestamp: {0}")]
    Timestamp(#[from] TimestampDecodeError),

    #[error("missing field: {0}")]
    MissingField(&'static str),

    #[error("unsupported datarate: {0}")]
    DataRate(prost::UnknownEnumValue),
}

#[derive(Serialize, Clone, Debug)]
pub struct IotBeaconReport {
    #[serde(alias = "pubKey")]
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
pub struct IotBeaconIngestReport {
    pub received_timestamp: DateTime<Utc>,
    pub report: IotBeaconReport,
}

impl MsgDecode for IotBeaconIngestReport {
    type Msg = LoraBeaconIngestReportV1;
}

impl TryFrom<LoraBeaconReportReqV1> for IotBeaconIngestReport {
    type Error = IotBeaconError;

    fn try_from(v: LoraBeaconReportReqV1) -> Result<Self, Self::Error> {
        Ok(Self {
            received_timestamp: Utc::now(),
            report: v.try_into()?,
        })
    }
}

impl MsgTimestamp<TimestampDecodeResult> for LoraBeaconReportReqV1 {
    fn timestamp(&self) -> TimestampDecodeResult {
        self.timestamp.to_timestamp_nanos()
    }
}

impl MsgTimestamp<u64> for IotBeaconReport {
    fn timestamp(&self) -> u64 {
        self.timestamp.encode_timestamp_nanos()
    }
}

impl MsgTimestamp<TimestampDecodeResult> for LoraBeaconIngestReportV1 {
    fn timestamp(&self) -> TimestampDecodeResult {
        self.received_timestamp.to_timestamp_millis()
    }
}

impl MsgTimestamp<u64> for IotBeaconIngestReport {
    fn timestamp(&self) -> u64 {
        self.received_timestamp.encode_timestamp_millis()
    }
}

impl TryFrom<LoraBeaconIngestReportV1> for IotBeaconIngestReport {
    type Error = IotBeaconError;

    fn try_from(v: LoraBeaconIngestReportV1) -> Result<Self, Self::Error> {
        Ok(Self {
            received_timestamp: v.timestamp()?,
            report: v
                .report
                .ok_or(IotBeaconError::MissingField(
                    "iot_beacon_ingest_report.report",
                ))?
                .try_into()?,
        })
    }
}

impl From<IotBeaconIngestReport> for LoraBeaconReportReqV1 {
    fn from(v: IotBeaconIngestReport) -> Self {
        Self {
            timestamp: v.report.timestamp(),
            pub_key: v.report.pub_key.into(),
            local_entropy: v.report.local_entropy,
            remote_entropy: v.report.remote_entropy,
            data: v.report.data,
            frequency: v.report.frequency,
            channel: v.report.channel,
            datarate: v.report.datarate as i32,
            tx_power: v.report.tx_power,
            signature: v.report.signature,
            tmst: v.report.tmst,
        }
    }
}

impl TryFrom<LoraBeaconReportReqV1> for IotBeaconReport {
    type Error = IotBeaconError;

    fn try_from(v: LoraBeaconReportReqV1) -> Result<Self, Self::Error> {
        Ok(Self {
            timestamp: v.timestamp()?,
            datarate: prost_enum(v.datarate, IotBeaconError::DataRate)?,
            pub_key: v.pub_key.into(),
            local_entropy: v.local_entropy,
            remote_entropy: v.remote_entropy,
            data: v.data,
            frequency: v.frequency,
            channel: v.channel,
            tx_power: v.tx_power,
            signature: v.signature,
            tmst: v.tmst,
        })
    }
}

impl From<IotBeaconReport> for LoraBeaconReportReqV1 {
    fn from(v: IotBeaconReport) -> Self {
        let timestamp = v.timestamp();
        Self {
            pub_key: v.pub_key.into(),
            local_entropy: v.local_entropy,
            remote_entropy: v.remote_entropy,
            data: v.data,
            frequency: v.frequency,
            channel: v.channel,
            datarate: v.datarate as i32,
            tx_power: v.tx_power,
            timestamp,
            signature: v.signature,
            tmst: v.tmst,
        }
    }
}

impl IotBeaconReport {
    pub fn to_beacon(&self, entropy_start: DateTime<Utc>, entropy_version: u32) -> beacon::Beacon {
        let remote_entropy = beacon::Entropy {
            timestamp: entropy_start.timestamp(),
            data: self.remote_entropy.clone(),
            version: entropy_version,
        };
        let local_entropy = beacon::Entropy {
            timestamp: 0,
            data: self.local_entropy.clone(),
            version: entropy_version,
        };
        beacon::Beacon {
            data: self.data.clone(),
            frequency: self.frequency,
            datarate: self.datarate,
            remote_entropy,
            local_entropy,
            conducted_power: self.tx_power as u32,
        }
    }
}
