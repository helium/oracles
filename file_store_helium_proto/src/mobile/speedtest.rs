use chrono::{DateTime, TimeZone, Utc};
use file_store::{traits::MsgDecode, DecodeError, Error, Result};
use helium_crypto::PublicKeyBinary;
use helium_proto::services::poc_mobile::{
    Speedtest, SpeedtestAvg, SpeedtestAvgValidity, SpeedtestIngestReportV1, SpeedtestReqV1,
    SpeedtestVerificationResult, VerifiedSpeedtest as VerifiedSpeedtestProto,
};
use serde::{Deserialize, Serialize};

use crate::traits::{MsgTimestamp, TimestampDecode, TimestampEncode};

#[derive(Clone, Deserialize, Serialize, Debug)]
pub struct CellSpeedtest {
    pub pubkey: PublicKeyBinary,
    pub serial: String,
    pub timestamp: DateTime<Utc>,
    pub upload_speed: u64,
    pub download_speed: u64,
    pub latency: u32,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CellSpeedtestIngestReport {
    pub received_timestamp: DateTime<Utc>,
    pub report: CellSpeedtest,
}

impl MsgDecode for CellSpeedtest {
    type Msg = SpeedtestReqV1;
}

impl MsgDecode for CellSpeedtestIngestReport {
    type Msg = SpeedtestIngestReportV1;
}

impl MsgDecode for VerifiedSpeedtest {
    type Msg = VerifiedSpeedtestProto;
}

impl MsgTimestamp<Result<DateTime<Utc>>> for SpeedtestReqV1 {
    fn timestamp(&self) -> Result<DateTime<Utc>> {
        self.timestamp.to_timestamp()
    }
}

impl MsgTimestamp<u64> for CellSpeedtest {
    fn timestamp(&self) -> u64 {
        self.timestamp.encode_timestamp()
    }
}

impl MsgTimestamp<Result<DateTime<Utc>>> for SpeedtestIngestReportV1 {
    fn timestamp(&self) -> Result<DateTime<Utc>> {
        self.received_timestamp.to_timestamp_millis()
    }
}

impl MsgTimestamp<u64> for CellSpeedtestIngestReport {
    fn timestamp(&self) -> u64 {
        self.received_timestamp.encode_timestamp_millis()
    }
}

impl MsgTimestamp<u64> for VerifiedSpeedtest {
    fn timestamp(&self) -> u64 {
        self.timestamp.encode_timestamp_millis()
    }
}

impl MsgTimestamp<Result<DateTime<Utc>>> for VerifiedSpeedtestProto {
    fn timestamp(&self) -> Result<DateTime<Utc>> {
        self.timestamp.to_timestamp_millis()
    }
}

impl From<CellSpeedtest> for SpeedtestReqV1 {
    fn from(v: CellSpeedtest) -> Self {
        let timestamp = v.timestamp();
        SpeedtestReqV1 {
            pub_key: v.pubkey.into(),
            serial: v.serial,
            timestamp,
            upload_speed: v.upload_speed,
            download_speed: v.download_speed,
            latency: v.latency,
            signature: vec![],
        }
    }
}

impl TryFrom<SpeedtestReqV1> for CellSpeedtest {
    type Error = Error;
    fn try_from(value: SpeedtestReqV1) -> Result<Self> {
        let timestamp = value.timestamp()?;
        Ok(Self {
            pubkey: value.pub_key.into(),
            serial: value.serial,
            timestamp,
            upload_speed: value.upload_speed,
            download_speed: value.download_speed,
            latency: value.latency,
        })
    }
}

impl TryFrom<SpeedtestIngestReportV1> for CellSpeedtestIngestReport {
    type Error = Error;
    fn try_from(v: SpeedtestIngestReportV1) -> Result<Self> {
        Ok(Self {
            received_timestamp: v.timestamp()?,
            report: v
                .report
                .ok_or_else(|| Error::not_found("ingest speedtest report"))?
                .try_into()?,
        })
    }
}

impl From<CellSpeedtestIngestReport> for SpeedtestIngestReportV1 {
    fn from(v: CellSpeedtestIngestReport) -> Self {
        let received_timestamp = v.timestamp();
        let report: SpeedtestReqV1 = v.report.into();
        Self {
            received_timestamp,
            report: Some(report),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct VerifiedSpeedtest {
    pub timestamp: DateTime<Utc>,
    pub report: CellSpeedtestIngestReport,
    pub result: SpeedtestVerificationResult,
}

impl TryFrom<VerifiedSpeedtestProto> for VerifiedSpeedtest {
    type Error = Error;
    fn try_from(v: VerifiedSpeedtestProto) -> Result<Self> {
        let result = SpeedtestVerificationResult::try_from(v.result).map_err(|_| {
            DecodeError::unsupported_status_reason("verified_speedtest_proto", v.result)
        })?;
        Ok(Self {
            timestamp: v.timestamp()?,
            report: v
                .report
                .ok_or_else(|| Error::not_found("ingest verified speedtest report"))?
                .try_into()?,
            result,
        })
    }
}

impl From<VerifiedSpeedtest> for VerifiedSpeedtestProto {
    fn from(v: VerifiedSpeedtest) -> Self {
        let timestamp = v.timestamp();
        let report: SpeedtestIngestReportV1 = v.report.into();
        Self {
            timestamp,
            report: Some(report),
            result: v.result as i32,
        }
    }
}

pub mod cli {
    use super::*;

    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub struct SpeedtestAverageEntry {
        pub upload_speed_bps: u64,
        pub download_speed_bps: u64,
        pub latency_ms: u32,
        pub timestamp: DateTime<Utc>,
    }

    impl TryFrom<Speedtest> for SpeedtestAverageEntry {
        type Error = Error;

        fn try_from(v: Speedtest) -> Result<Self> {
            Ok(Self {
                upload_speed_bps: v.upload_speed_bps,
                download_speed_bps: v.download_speed_bps,
                latency_ms: v.latency_ms,
                timestamp: Utc
                    .timestamp_opt(v.timestamp as i64, 0)
                    .single()
                    .ok_or_else(|| DecodeError::invalid_timestamp(v.timestamp))?,
            })
        }
    }

    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub struct SpeedtestAverage {
        pub pub_key: PublicKeyBinary,
        pub upload_speed_avg_bps: u64,
        pub download_speed_avg_bps: u64,
        pub latency_avg_ms: u32,
        pub validity: SpeedtestAvgValidity,
        pub speedtests: Vec<SpeedtestAverageEntry>,
        pub timestamp: DateTime<Utc>,
        pub reward_multiplier: f32,
    }

    impl TryFrom<SpeedtestAvg> for SpeedtestAverage {
        type Error = Error;

        fn try_from(v: SpeedtestAvg) -> Result<Self> {
            Ok(Self {
                pub_key: v.pub_key.clone().into(),
                upload_speed_avg_bps: v.upload_speed_avg_bps,
                download_speed_avg_bps: v.download_speed_avg_bps,
                latency_avg_ms: v.latency_avg_ms,
                validity: v.validity(),
                speedtests: v
                    .speedtests
                    .into_iter()
                    .map(SpeedtestAverageEntry::try_from)
                    .collect::<Result<Vec<_>>>()?,
                timestamp: Utc
                    .timestamp_opt(v.timestamp as i64, 0)
                    .single()
                    .ok_or_else(|| DecodeError::invalid_timestamp(v.timestamp))?,
                reward_multiplier: v.reward_multiplier,
            })
        }
    }

    impl MsgDecode for SpeedtestAverage {
        type Msg = SpeedtestAvg;
    }
}

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use chrono::TimeZone;
//     use hex_literal::hex;
//     use prost::Message;

//     const PK_BYTES: [u8; 33] =
//         hex!("008f23e96ab6bbff48c8923cac831dc97111bcf33dba9f5a8539c00f9d93551af1");

//     #[test]
//     fn decode_proto_speed_test_ingest_report_to_internal_struct() {
//         let now = Utc::now().timestamp_millis();
//         let report = SpeedtestIngestReportV1 {
//             received_timestamp: now as u64,
//             report: Some(SpeedtestReqV1 {
//                 pub_key: PK_BYTES.to_vec(),
//                 serial: "serial".to_string(),
//                 timestamp: now as u64,
//                 upload_speed: 6,
//                 download_speed: 2,
//                 latency: 1,
//                 signature: vec![],
//             }),
//         };

//         let buffer = report.encode_to_vec();

//         let speedtest_report = CellSpeedtestIngestReport::decode(buffer.as_slice())
//             .expect("unable to decode in CellSpeedtestIngestReport");

//         assert_eq!(
//             speedtest_report.received_timestamp,
//             Utc.timestamp_millis_opt(now).unwrap()
//         );
//         assert_eq!(speedtest_report.report.serial, "serial");
//     }
// }
