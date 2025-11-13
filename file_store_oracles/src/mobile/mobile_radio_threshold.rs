use chrono::{DateTime, Utc};
use file_store::traits::{
    MsgDecode, TimestampDecode, TimestampDecodeError, TimestampDecodeResult, TimestampEncode,
};
use helium_crypto::PublicKeyBinary;
use helium_proto::services::poc_mobile::{
    RadioThresholdIngestReportV1, RadioThresholdReportReqV1,
    RadioThresholdReportVerificationStatus, VerifiedRadioThresholdIngestReportV1,
};
use serde::{Deserialize, Serialize};

use crate::{prost_enum, traits::MsgTimestamp};

#[derive(thiserror::Error, Debug)]
pub enum RadioThresholdError {
    #[error("invalid timestamp: {0}")]
    Timestamp(#[from] TimestampDecodeError),

    #[error("missing field: {0}")]
    MissingField(&'static str),

    #[error("unsupported verification status: {0}")]
    VerificationStatus(prost::UnknownEnumValue),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RadioThresholdReportReq {
    pub hotspot_pubkey: PublicKeyBinary,
    pub bytes_threshold: u64,
    pub subscriber_threshold: u32,
    pub threshold_timestamp: DateTime<Utc>,
    pub carrier_pub_key: PublicKeyBinary,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct VerifiedRadioThresholdIngestReport {
    pub report: RadioThresholdIngestReport,
    pub status: RadioThresholdReportVerificationStatus,
    pub timestamp: DateTime<Utc>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RadioThresholdIngestReport {
    pub received_timestamp: DateTime<Utc>,
    pub report: RadioThresholdReportReq,
}

impl MsgDecode for RadioThresholdReportReq {
    type Msg = RadioThresholdReportReqV1;
}

impl MsgDecode for RadioThresholdIngestReport {
    type Msg = RadioThresholdIngestReportV1;
}

impl MsgDecode for VerifiedRadioThresholdIngestReport {
    type Msg = VerifiedRadioThresholdIngestReportV1;
}

impl TryFrom<RadioThresholdReportReqV1> for RadioThresholdReportReq {
    type Error = RadioThresholdError;

    fn try_from(v: RadioThresholdReportReqV1) -> Result<Self, Self::Error> {
        Ok(Self {
            hotspot_pubkey: v.hotspot_pubkey.into(),
            bytes_threshold: v.bytes_threshold,
            subscriber_threshold: v.subscriber_threshold,
            threshold_timestamp: v.threshold_timestamp.to_timestamp()?,
            carrier_pub_key: v.carrier_pub_key.into(),
        })
    }
}

impl From<RadioThresholdReportReq> for RadioThresholdReportReqV1 {
    fn from(v: RadioThresholdReportReq) -> Self {
        let threshold_timestamp = v.threshold_timestamp.timestamp() as u64;
        Self {
            cbsd_id: String::default(),
            hotspot_pubkey: v.hotspot_pubkey.into(),
            bytes_threshold: v.bytes_threshold,
            subscriber_threshold: v.subscriber_threshold,
            threshold_timestamp,
            carrier_pub_key: v.carrier_pub_key.into(),
            signature: vec![],
        }
    }
}

impl MsgTimestamp<TimestampDecodeResult> for RadioThresholdReportReqV1 {
    fn timestamp(&self) -> TimestampDecodeResult {
        self.threshold_timestamp.to_timestamp()
    }
}

impl MsgTimestamp<u64> for RadioThresholdReportReq {
    fn timestamp(&self) -> u64 {
        self.threshold_timestamp.encode_timestamp()
    }
}

impl MsgTimestamp<TimestampDecodeResult> for RadioThresholdIngestReportV1 {
    fn timestamp(&self) -> TimestampDecodeResult {
        self.received_timestamp.to_timestamp_millis()
    }
}

impl MsgTimestamp<u64> for RadioThresholdIngestReport {
    fn timestamp(&self) -> u64 {
        self.received_timestamp.encode_timestamp_millis()
    }
}

impl MsgTimestamp<TimestampDecodeResult> for VerifiedRadioThresholdIngestReportV1 {
    fn timestamp(&self) -> TimestampDecodeResult {
        self.timestamp.to_timestamp_millis()
    }
}

impl MsgTimestamp<u64> for VerifiedRadioThresholdIngestReport {
    fn timestamp(&self) -> u64 {
        self.timestamp.encode_timestamp_millis()
    }
}

impl TryFrom<RadioThresholdIngestReportV1> for RadioThresholdIngestReport {
    type Error = RadioThresholdError;

    fn try_from(v: RadioThresholdIngestReportV1) -> Result<Self, Self::Error> {
        Ok(Self {
            received_timestamp: v.timestamp()?,
            report: v
                .report
                .ok_or(RadioThresholdError::MissingField(
                    "radio_threshold_ingest_report.report",
                ))?
                .try_into()?,
        })
    }
}

impl From<RadioThresholdIngestReport> for RadioThresholdIngestReportV1 {
    fn from(v: RadioThresholdIngestReport) -> Self {
        let received_timestamp = v.timestamp();
        let report: RadioThresholdReportReqV1 = v.report.into();
        Self {
            received_timestamp,
            report: Some(report),
        }
    }
}

impl TryFrom<VerifiedRadioThresholdIngestReportV1> for VerifiedRadioThresholdIngestReport {
    type Error = RadioThresholdError;

    fn try_from(v: VerifiedRadioThresholdIngestReportV1) -> Result<Self, Self::Error> {
        Ok(Self {
            report: v
                .report
                .ok_or(RadioThresholdError::MissingField(
                    "verified_radio_threshold_ingest_report.report",
                ))?
                .try_into()?,
            status: prost_enum(v.status, RadioThresholdError::VerificationStatus)?,
            timestamp: v.timestamp.to_timestamp()?,
        })
    }
}

impl From<VerifiedRadioThresholdIngestReport> for VerifiedRadioThresholdIngestReportV1 {
    fn from(v: VerifiedRadioThresholdIngestReport) -> Self {
        let timestamp = v.timestamp();
        let report: RadioThresholdIngestReportV1 = v.report.into();
        Self {
            report: Some(report),
            status: v.status as i32,
            timestamp,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use chrono::NaiveDateTime;

    use super::*;

    #[test]
    fn radio_threshold_from_proto_wifi() -> anyhow::Result<()> {
        let pubkey = PublicKeyBinary::from_str("1trSusedShTqrkW7HUxv9QtrjadcnnQEmTJFTdBLDkERUV4bb3rXjzeL7QgGS6rFnsqkHwsUVTxodx2ZtKQ4KehaVzfji6jjTKH85JjmdQUAbqakURZJrdDjCnTHkaVae2mh5asCyQnDXvFpty4eKbaupQKgFuZmzVrowuAMjV1T31yZisa5i1eux2RTuMsfyHu6emk87X3BAcHwTd6vKok1SkBGQmPUo7ThJE7qSD5bixMuKXyowzCEeLkYkrhQr1yCsBwmBmnxT5ZydsTkJQdhKvtnyVxh1kSJi59MqAbD6N4DfGzSAqBSNQZSUXKrXoHDuYZ1wL7A2MLizXcEUGqWFdKfBaJ5ekKthRZjLGpWKP")?;

        let carrier_pubkey =
            PublicKeyBinary::from_str("14ihsKqVhXqfzET1dkLZGNQWrB9ZeGnqJtdMGajFjPmwKsKEEAC")?;

        let proto = RadioThresholdIngestReportV1 {
            received_timestamp: 1712624400000,
            report: Some(RadioThresholdReportReqV1 {
                cbsd_id: "".to_string(),
                hotspot_pubkey: pubkey.as_ref().into(),
                bytes_threshold: 1000,
                subscriber_threshold: 3,
                threshold_timestamp: 1712624400,
                carrier_pub_key: carrier_pubkey.as_ref().into(),
                signature: vec![],
            }),
        };

        let report = RadioThresholdIngestReport::try_from(proto)?;
        assert_eq!(parse_dt("2024-04-09 01:00:00"), report.received_timestamp);
        assert_eq!(pubkey, report.report.hotspot_pubkey);
        assert_eq!(1000, report.report.bytes_threshold);
        assert_eq!(3, report.report.subscriber_threshold);
        assert_eq!(
            parse_dt("2024-04-09 01:00:00"),
            report.report.threshold_timestamp
        );
        assert_eq!(carrier_pubkey, report.report.carrier_pub_key);

        Ok(())
    }

    #[test]
    fn radio_threshold_from_proto_cbrs() -> anyhow::Result<()> {
        // UPD: Since cbrs is not supported anymore this test
        // assures that cbsd_id field just is ignored (try_from not fails)
        let pubkey =
            PublicKeyBinary::from_str("112HqsSX9Ft4ehxQCAcdb4cDSYX2ntsBZ7rtooioz3d3VXcF7MRr")?;

        let carrier_pubkey =
            PublicKeyBinary::from_str("14ihsKqVhXqfzET1dkLZGNQWrB9ZeGnqJtdMGajFjPmwKsKEEAC")?;

        let proto = RadioThresholdIngestReportV1 {
            received_timestamp: 1712624400000,
            report: Some(RadioThresholdReportReqV1 {
                cbsd_id: "P27-SCE4255W2112CW5003971".to_string(),
                hotspot_pubkey: pubkey.as_ref().into(),
                bytes_threshold: 1000,
                subscriber_threshold: 3,
                threshold_timestamp: 1712624400,
                carrier_pub_key: carrier_pubkey.as_ref().into(),
                signature: vec![],
            }),
        };

        let report = RadioThresholdIngestReport::try_from(proto)?;
        assert_eq!(parse_dt("2024-04-09 01:00:00"), report.received_timestamp);
        assert_eq!(pubkey, report.report.hotspot_pubkey);
        assert_eq!(1000, report.report.bytes_threshold);
        assert_eq!(3, report.report.subscriber_threshold);
        assert_eq!(
            parse_dt("2024-04-09 01:00:00"),
            report.report.threshold_timestamp
        );
        assert_eq!(carrier_pubkey, report.report.carrier_pub_key);

        Ok(())
    }

    fn parse_dt(dt: &str) -> DateTime<Utc> {
        NaiveDateTime::parse_from_str(dt, "%Y-%m-%d %H:%M:%S")
            .expect("unable_to_parse")
            .and_utc()
    }
}
