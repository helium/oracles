use crate::{
    traits::MsgDecode,
    traits::{MsgTimestamp, TimestampDecode, TimestampEncode},
    Error, Result,
};
use chrono::{DateTime, Utc};
use helium_crypto::PublicKey;
use helium_proto::services::poc_lora::{LoraBeaconIngestReportV1, LoraBeaconReportReqV1};
use helium_proto::DataRate;
use serde::Serialize;
use sha2::{Digest, Sha256};

#[derive(Serialize, Clone, Debug)]
pub struct LoraBeaconReport {
    #[serde(alias = "pubKey")]
    pub pub_key: PublicKey,
    pub local_entropy: Vec<u8>,
    pub remote_entropy: Vec<u8>,
    pub data: Vec<u8>,
    pub frequency: u64,
    pub channel: i32,
    pub datarate: DataRate,
    pub tx_power: i32,
    pub timestamp: DateTime<Utc>,
    pub signature: Vec<u8>,
}

#[derive(Serialize, Clone, Debug)]
pub struct LoraBeaconIngestReport {
    pub received_timestamp: DateTime<Utc>,
    pub report: LoraBeaconReport,
}

impl MsgDecode for LoraBeaconIngestReport {
    type Msg = LoraBeaconIngestReportV1;
}

impl LoraBeaconIngestReport {
    pub fn generate_id(&self) -> Vec<u8> {
        let mut id: Vec<u8> = self.report.data.clone();
        let mut public_key = self.report.pub_key.to_vec();
        id.append(&mut self.received_timestamp.to_string().as_bytes().to_vec());
        id.append(&mut public_key);
        Sha256::digest(&id).to_vec()
    }
}

impl LoraBeaconReport {
    pub fn generate_id(&self, received_ts: DateTime<Utc>) -> Vec<u8> {
        let mut id: Vec<u8> = self.data.clone();
        let mut public_key = self.pub_key.to_vec();
        id.append(&mut received_ts.to_string().as_bytes().to_vec());
        id.append(&mut public_key);
        Sha256::digest(&id).to_vec()
    }
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
            pub_key: v.report.pub_key.to_vec(),
            local_entropy: v.report.local_entropy,
            remote_entropy: v.report.remote_entropy,
            data: v.report.data,
            frequency: v.report.frequency,
            channel: v.report.channel,
            datarate: v.report.datarate as i32,
            tx_power: v.report.tx_power,
            timestamp,
            signature: vec![],
        }
    }
}

impl TryFrom<LoraBeaconReportReqV1> for LoraBeaconReport {
    type Error = Error;
    fn try_from(v: LoraBeaconReportReqV1) -> Result<Self> {
        let data_rate: DataRate =
            DataRate::from_i32(v.datarate).ok_or_else(|| Error::custom("unsupported datarate"))?;
        let timestamp = v.timestamp()?;

        Ok(Self {
            pub_key: PublicKey::try_from(v.pub_key)?,
            local_entropy: v.local_entropy,
            remote_entropy: v.remote_entropy,
            data: v.data,
            frequency: v.frequency,
            channel: v.channel,
            datarate: data_rate,
            tx_power: v.tx_power,
            timestamp,
            signature: v.signature,
        })
    }
}

impl From<LoraBeaconReport> for LoraBeaconReportReqV1 {
    fn from(v: LoraBeaconReport) -> Self {
        let timestamp = v.timestamp();
        Self {
            pub_key: v.pub_key.to_vec(),
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
        }
    }
}
