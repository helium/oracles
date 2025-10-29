use chrono::{DateTime, Utc};
use file_store::traits::{
    MsgDecode, TimestampDecode, TimestampDecodeError, TimestampDecodeResult, TimestampEncode,
};
use helium_crypto::PublicKeyBinary;
use helium_proto::services::poc_mobile::{
    SubscriberLocationIngestReportV1, SubscriberLocationReqV1, SubscriberReportVerificationStatus,
    VerifiedSubscriberLocationIngestReportV1,
};
use serde::{Deserialize, Serialize};

use crate::{prost_enum, traits::MsgTimestamp};

#[derive(thiserror::Error, Debug)]
pub enum SubscriberLocationError {
    #[error("invalid timestamp: {0}")]
    Timestamp(#[from] TimestampDecodeError),

    #[error("missing field: {0}")]
    MissingField(&'static str),

    #[error("unsupported status reason: {0}")]
    StatusReason(prost::UnknownEnumValue),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SubscriberLocationReq {
    pub subscriber_id: Vec<u8>,
    pub timestamp: DateTime<Utc>,
    pub carrier_pub_key: PublicKeyBinary,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SubscriberLocationIngestReport {
    pub received_timestamp: DateTime<Utc>,
    pub report: SubscriberLocationReq,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct VerifiedSubscriberLocationIngestReport {
    pub report: SubscriberLocationIngestReport,
    pub status: SubscriberReportVerificationStatus,
    pub timestamp: DateTime<Utc>,
}

impl MsgDecode for SubscriberLocationReq {
    type Msg = SubscriberLocationReqV1;
}

impl MsgDecode for SubscriberLocationIngestReport {
    type Msg = SubscriberLocationIngestReportV1;
}

impl MsgDecode for VerifiedSubscriberLocationIngestReport {
    type Msg = VerifiedSubscriberLocationIngestReportV1;
}

impl TryFrom<SubscriberLocationReqV1> for SubscriberLocationReq {
    type Error = SubscriberLocationError;

    fn try_from(v: SubscriberLocationReqV1) -> Result<Self, Self::Error> {
        Ok(Self {
            subscriber_id: v.subscriber_id,
            timestamp: v.timestamp.to_timestamp()?,
            carrier_pub_key: v.carrier_pub_key.into(),
        })
    }
}

impl From<SubscriberLocationReq> for SubscriberLocationReqV1 {
    fn from(v: SubscriberLocationReq) -> Self {
        let timestamp = v.timestamp();
        Self {
            subscriber_id: v.subscriber_id,
            timestamp,
            carrier_pub_key: v.carrier_pub_key.into(),
            signature: vec![],
        }
    }
}

impl MsgTimestamp<TimestampDecodeResult> for SubscriberLocationReqV1 {
    fn timestamp(&self) -> TimestampDecodeResult {
        self.timestamp.to_timestamp()
    }
}

impl MsgTimestamp<u64> for SubscriberLocationReq {
    fn timestamp(&self) -> u64 {
        self.timestamp.encode_timestamp()
    }
}

impl MsgTimestamp<TimestampDecodeResult> for SubscriberLocationIngestReportV1 {
    fn timestamp(&self) -> TimestampDecodeResult {
        self.received_timestamp.to_timestamp_millis()
    }
}

impl MsgTimestamp<u64> for SubscriberLocationIngestReport {
    fn timestamp(&self) -> u64 {
        self.received_timestamp.encode_timestamp_millis()
    }
}

impl MsgTimestamp<TimestampDecodeResult> for VerifiedSubscriberLocationIngestReportV1 {
    fn timestamp(&self) -> TimestampDecodeResult {
        self.timestamp.to_timestamp_millis()
    }
}

impl MsgTimestamp<u64> for VerifiedSubscriberLocationIngestReport {
    fn timestamp(&self) -> u64 {
        self.timestamp.encode_timestamp_millis()
    }
}

impl TryFrom<SubscriberLocationIngestReportV1> for SubscriberLocationIngestReport {
    type Error = SubscriberLocationError;

    fn try_from(v: SubscriberLocationIngestReportV1) -> Result<Self, Self::Error> {
        Ok(Self {
            received_timestamp: v.timestamp()?,
            report: v
                .report
                .ok_or(SubscriberLocationError::MissingField(
                    "subscriber_location_ingest_report.report",
                ))?
                .try_into()?,
        })
    }
}

impl From<SubscriberLocationIngestReport> for SubscriberLocationIngestReportV1 {
    fn from(v: SubscriberLocationIngestReport) -> Self {
        let received_timestamp = v.timestamp();
        let report: SubscriberLocationReqV1 = v.report.into();
        Self {
            received_timestamp,
            report: Some(report),
        }
    }
}

impl TryFrom<VerifiedSubscriberLocationIngestReportV1> for VerifiedSubscriberLocationIngestReport {
    type Error = SubscriberLocationError;

    fn try_from(v: VerifiedSubscriberLocationIngestReportV1) -> Result<Self, Self::Error> {
        Ok(Self {
            report: v
                .report
                .ok_or(SubscriberLocationError::MissingField(
                    "verified_subscriber_location_ingest_report.report",
                ))?
                .try_into()?,
            status: prost_enum(v.status, SubscriberLocationError::StatusReason)?,
            timestamp: v.timestamp.to_timestamp()?,
        })
    }
}

impl From<VerifiedSubscriberLocationIngestReport> for VerifiedSubscriberLocationIngestReportV1 {
    fn from(v: VerifiedSubscriberLocationIngestReport) -> Self {
        let timestamp = v.timestamp();
        let report: SubscriberLocationIngestReportV1 = v.report.into();
        Self {
            report: Some(report),
            status: v.status as i32,
            timestamp,
        }
    }
}
