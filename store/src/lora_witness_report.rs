use crate::{datetime_from_epoch, traits::MsgDecode, Error, Result};
use chrono::{DateTime, Utc};
use helium_crypto::PublicKey;
use helium_proto::services::poc_lora::{LoraWitnessIngestReportV1, LoraWitnessReportReqV1};
use helium_proto::DataRate;
use serde::Serialize;

#[derive(Serialize)]
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

#[derive(Serialize)]
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
        // let data_rate: DataRate = DataRate::from_i32(v.datarate).unwrap();
        let data_rate: DataRate =
            DataRate::from_i32(v.datarate).ok_or_else(|| Error::custom("unsupported datarate"))?;
        Ok(Self {
            received_timestamp: Utc::now(),
            report: LoraWitnessReport {
                pub_key: PublicKey::try_from(v.pub_key)?,
                data: v.data,
                timestamp: datetime_from_epoch(v.timestamp),
                ts_res: v.ts_res,
                signal: v.signal,
                snr: v.snr,
                frequency: v.frequency,
                datarate: data_rate,
                signature: v.signature,
            },
        })
    }
}

impl TryFrom<LoraWitnessIngestReportV1> for LoraWitnessIngestReport {
    type Error = Error;
    fn try_from(v: LoraWitnessIngestReportV1) -> Result<Self> {
        let report = v.report.unwrap();
        //TODO: fix this to return error is not valid value
        // let data_rate: DataRate = DataRate::from_i32(v.datarate).unwrap();
        let data_rate: DataRate = DataRate::from_i32(report.datarate)
            .ok_or_else(|| Error::custom("unsupported datarate"))?;
        Ok(Self {
            received_timestamp: datetime_from_epoch(report.timestamp),
            report: LoraWitnessReport {
                pub_key: PublicKey::try_from(report.pub_key)?,
                data: report.data,
                timestamp: datetime_from_epoch(report.timestamp),
                ts_res: report.ts_res,
                signal: report.signal,
                snr: report.snr,
                frequency: report.frequency,
                datarate: data_rate,
                signature: report.signature,
            },
        })
    }
}

impl From<LoraWitnessIngestReport> for LoraWitnessReportReqV1 {
    fn from(v: LoraWitnessIngestReport) -> Self {
        Self {
            pub_key: v.report.pub_key.to_vec(),
            data: v.report.data,
            timestamp: v.report.timestamp.timestamp() as u64,
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
        let data_rate: DataRate = DataRate::from_i32(v.datarate).unwrap();
        // let data_rate: DataRate = DataRate::from_i32(v.datarate)
        //     .unwrap_or_else(|| Error::custom(format!("unsupported datarate")));
        Ok(Self {
            pub_key: PublicKey::try_from(v.pub_key)?,
            data: v.data,
            timestamp: datetime_from_epoch(v.timestamp),
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
            timestamp: v.timestamp.timestamp() as u64,
            ts_res: v.ts_res,
            signal: v.signal,
            snr: v.snr,
            frequency: v.frequency,
            datarate: 0,
            signature: vec![],
        }
    }
}

// impl From<LoraWitnessIngestReportV1> for LoraWitnessIngestReport {
//     fn from(v: LoraWitnessIngestReportV1) -> Self {
//         let ts = datetime_from_epoch(v.received_timestamp);
//         let report = v.report.unwrap();
//         let data_rate: DataRate = DataRate::from_i32(report.datarate).unwrap();
//         Self {
//             received_timestamp: ts,
//             report: WitnessPayload {
//                 pub_key: PublicKey::try_from(report.pub_key).unwrap(),
//                 data: report.data,
//                 timestamp: datetime_from_epoch(report.timestamp),
//                 ts_res: report.ts_res,
//                 signal: report.signal,
//                 snr: report.snr,
//                 frequency: report.frequency,
//                 datarate: data_rate,
//                 signature: vec![],
//             },
//         }
//     }
// }
