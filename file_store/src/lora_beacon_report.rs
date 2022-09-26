use crate::{datetime_from_epoch, traits::MsgDecode, Error, Result};
use chrono::{DateTime, Utc};
use helium_crypto::PublicKey;
use helium_proto::services::poc_lora::{LoraBeaconIngestReportV1, LoraBeaconReportReqV1};
use helium_proto::DataRate;
use serde::Serialize;

#[derive(Serialize)]
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

#[derive(Serialize)]
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
        //TODO: fix this to return error is not valid value
        // let data_rate: DataRate = DataRate::from_i32(v.datarate).unwrap();
        let data_rate: DataRate =
            DataRate::from_i32(v.datarate).ok_or_else(|| Error::custom("unsupported datarate"))?;
        Ok(Self {
            received_timestamp: Utc::now(),
            report: LoraBeaconReport {
                pub_key: PublicKey::try_from(v.pub_key)?,
                local_entropy: v.local_entropy,
                remote_entropy: v.remote_entropy,
                data: v.data,
                frequency: v.frequency,
                channel: v.channel,
                datarate: data_rate,
                tx_power: v.tx_power,
                timestamp: datetime_from_epoch(v.timestamp),
                signature: v.signature,
            },
        })
    }
}

impl TryFrom<LoraBeaconIngestReportV1> for LoraBeaconIngestReport {
    type Error = Error;
    fn try_from(v: LoraBeaconIngestReportV1) -> Result<Self> {
        let report = v.report.unwrap();
        //TODO: fix this to return error is not valid value
        // let data_rate: DataRate = DataRate::from_i32(v.datarate).unwrap();
        let data_rate: DataRate = DataRate::from_i32(report.datarate)
            .ok_or_else(|| Error::custom("unsupported datarate"))?;
        Ok(Self {
            received_timestamp: datetime_from_epoch(report.timestamp),
            report: LoraBeaconReport {
                pub_key: PublicKey::try_from(report.pub_key)?,
                local_entropy: report.local_entropy,
                remote_entropy: report.remote_entropy,
                data: report.data,
                frequency: report.frequency,
                channel: report.channel,
                datarate: data_rate,
                tx_power: report.tx_power,
                timestamp: datetime_from_epoch(report.timestamp),
                signature: report.signature,
            },
        })
    }
}

impl From<LoraBeaconIngestReport> for LoraBeaconReportReqV1 {
    fn from(v: LoraBeaconIngestReport) -> Self {
        Self {
            pub_key: v.report.pub_key.to_vec(),
            local_entropy: v.report.local_entropy,
            remote_entropy: v.report.remote_entropy,
            data: v.report.data,
            frequency: v.report.frequency,
            channel: v.report.channel,
            datarate: v.report.datarate as i32,
            tx_power: v.report.tx_power,
            timestamp: v.report.timestamp.timestamp() as u64,
            signature: vec![],
        }
    }
}

impl TryFrom<LoraBeaconReportReqV1> for LoraBeaconReport {
    type Error = Error;
    fn try_from(v: LoraBeaconReportReqV1) -> Result<Self> {
        //TODO: fix this to return error is not valid value
        let data_rate: DataRate = DataRate::from_i32(v.datarate).unwrap();
        // let data_rate: DataRate = DataRate::from_i32(v.datarate)
        // .unwrap_or_else(|| Error::custom(format!("unsupported datarate")));
        Ok(Self {
            pub_key: PublicKey::try_from(v.pub_key)?,
            local_entropy: v.local_entropy,
            remote_entropy: v.remote_entropy,
            data: v.data,
            frequency: v.frequency,
            channel: v.channel,
            datarate: data_rate,
            tx_power: v.tx_power,
            timestamp: datetime_from_epoch(v.timestamp),
            signature: v.signature,
        })
    }
}

impl From<LoraBeaconReport> for LoraBeaconReportReqV1 {
    fn from(v: LoraBeaconReport) -> Self {
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
            timestamp: v.timestamp.timestamp() as u64,
            signature: vec![],
        }
    }
}

// impl From<LoraBeaconIngestReportV1> for LoraBeaconIngestReport {
//     fn from(v: LoraBeaconIngestReportV1) -> Self {
//         let report = v.report.unwrap();
//         let data_rate: DataRate = DataRate::from_i32(report.datarate).unwrap();
//         Self {
//             received_timestamp: datetime_from_epoch(v.received_timestamp),
//             report: BeaconPayload {
//                 pub_key: PublicKey::try_from(report.pub_key).unwrap(),
//                 local_entropy: report.local_entropy,
//                 remote_entropy: report.remote_entropy,
//                 data: report.data,
//                 frequency: report.frequency,
//                 channel: report.channel,
//                 datarate: data_rate,
//                 tx_power: report.tx_power,
//                 timestamp: datetime_from_epoch(report.timestamp),
//                 signature: report.signature,
//             },
//         }
//     }
// }
