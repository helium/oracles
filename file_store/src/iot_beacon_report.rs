use crate::{
    error::DecodeError,
    traits::MsgDecode,
    traits::{MsgTimestamp, TimestampDecode, TimestampEncode},
    Error, Result,
};
use chrono::{DateTime, Utc};
use helium_crypto::PublicKeyBinary;
use helium_proto::services::poc_iot::{IotBeaconIngestReportV1, IotBeaconReportReqV1};
use helium_proto::DataRate;
use serde::Serialize;

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
    type Msg = IotBeaconIngestReportV1;
}

impl TryFrom<IotBeaconReportReqV1> for IotBeaconIngestReport {
    type Error = Error;
    fn try_from(v: IotBeaconReportReqV1) -> Result<Self> {
        Ok(Self {
            received_timestamp: Utc::now(),
            report: v.try_into()?,
        })
    }
}

impl MsgTimestamp<Result<DateTime<Utc>>> for IotBeaconReportReqV1 {
    fn timestamp(&self) -> Result<DateTime<Utc>> {
        self.timestamp.to_timestamp_nanos()
    }
}

impl MsgTimestamp<u64> for IotBeaconReport {
    fn timestamp(&self) -> u64 {
        self.timestamp.encode_timestamp_nanos()
    }
}

impl MsgTimestamp<Result<DateTime<Utc>>> for IotBeaconIngestReportV1 {
    fn timestamp(&self) -> Result<DateTime<Utc>> {
        self.received_timestamp.to_timestamp_millis()
    }
}

impl MsgTimestamp<u64> for IotBeaconIngestReport {
    fn timestamp(&self) -> u64 {
        self.received_timestamp.encode_timestamp_millis()
    }
}

impl TryFrom<IotBeaconIngestReportV1> for IotBeaconIngestReport {
    type Error = Error;
    fn try_from(v: IotBeaconIngestReportV1) -> Result<Self> {
        Ok(Self {
            received_timestamp: v.timestamp()?,
            report: v
                .report
                .ok_or_else(|| Error::not_found("iot beacon ingest report"))?
                .try_into()?,
        })
    }
}

impl From<IotBeaconIngestReport> for IotBeaconReportReqV1 {
    fn from(v: IotBeaconIngestReport) -> Self {
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

impl TryFrom<IotBeaconReportReqV1> for IotBeaconReport {
    type Error = Error;
    fn try_from(v: IotBeaconReportReqV1) -> Result<Self> {
        let dr = v.datarate;
        let data_rate: DataRate = DataRate::from_i32(dr)
            .ok_or_else(|| DecodeError::unsupported_datarate("iot_beacon_report_req_v1", dr))?;
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

impl From<IotBeaconReport> for IotBeaconReportReqV1 {
    fn from(v: IotBeaconReport) -> Self {
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
