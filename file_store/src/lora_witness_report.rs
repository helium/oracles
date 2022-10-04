use crate::{traits::MsgDecode, traits::MsgTimestamp, Error, Result};
use chrono::{DateTime, Utc};
use helium_crypto::PublicKey;
use helium_proto::services::poc_lora::{LoraWitnessIngestReportV1, LoraWitnessReportReqV1};
use helium_proto::DataRate;
use serde::Serialize;

#[derive(Serialize, Clone)]
pub struct LoraWitnessReport {
    #[serde(alias = "pubKey")]
    pub pub_key: PublicKey,
    pub data: Vec<u8>,
    pub timestamp: DateTime<Utc>,
    pub ts_res: u32,
    pub signal: u32,
    pub snr: f32,
    pub frequency: u64,
    pub datarate: DataRate,
    pub signature: Vec<u8>,
}

#[derive(Serialize, Clone)]
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
            received_timestamp: v.received_timestamp.to_timestamp_millis()?,
            report: v
                .report
                .ok_or_else(|| Error::not_found("lora witness ingest report v1"))?
                .try_into()?,
        })
    }
}

impl From<LoraWitnessIngestReport> for LoraWitnessReportReqV1 {
    fn from(v: LoraWitnessIngestReport) -> Self {
        Self {
            pub_key: v.report.pub_key.to_vec(),
            data: v.report.data,
            timestamp: v.report.timestamp.timestamp_nanos() as u64,
            ts_res: v.report.ts_res,
            signal: v.report.signal,
            snr: v.report.snr,
            frequency: v.report.frequency,
            datarate: 0,
            signature: vec![],
        }
    }
}

impl TryFrom<LoraWitnessReportReqV1> for LoraWitnessReport {
    type Error = Error;
    fn try_from(v: LoraWitnessReportReqV1) -> Result<Self> {
        let data_rate: DataRate = DataRate::from_i32(v.datarate)
            .ok_or_else(|| Error::Custom("unsupported datarate".to_string()))?;

        Ok(Self {
            pub_key: PublicKey::try_from(v.pub_key)?,
            data: v.data,
            timestamp: v.timestamp.to_timestamp_nanos()?,
            ts_res: v.ts_res,
            signal: v.signal,
            snr: v.snr,
            frequency: v.frequency,
            datarate: data_rate,
            signature: v.signature,
        })
    }
}

impl From<LoraWitnessReport> for LoraWitnessReportReqV1 {
    fn from(v: LoraWitnessReport) -> Self {
        Self {
            pub_key: v.pub_key.to_vec(),
            data: v.data,
            timestamp: v.timestamp.timestamp_nanos() as u64,
            ts_res: v.ts_res,
            signal: v.signal,
            snr: v.snr,
            frequency: v.frequency,
            datarate: 0,
            signature: vec![],
        }
    }
}
