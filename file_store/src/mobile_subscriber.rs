use crate::{
    error::DecodeError,
    traits::{MsgDecode, MsgTimestamp, TimestampDecode, TimestampEncode},
    Error, Result,
};
use chrono::{DateTime, Utc};
use helium_crypto::PublicKeyBinary;
use helium_proto::services::poc_mobile::{
    SubscriberLocationIngestReportV1, SubscriberLocationReqV1, SubscriberReportVerificationStatus,
    VerifiedSubscriberLocationIngestReportV1,
};
use serde::{Deserialize, Serialize};

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
    type Error = Error;
    fn try_from(v: SubscriberLocationReqV1) -> Result<Self> {
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

impl MsgTimestamp<Result<DateTime<Utc>>> for SubscriberLocationReqV1 {
    fn timestamp(&self) -> Result<DateTime<Utc>> {
        self.timestamp.to_timestamp()
    }
}

impl MsgTimestamp<u64> for SubscriberLocationReq {
    fn timestamp(&self) -> u64 {
        self.timestamp.encode_timestamp()
    }
}

impl MsgTimestamp<Result<DateTime<Utc>>> for SubscriberLocationIngestReportV1 {
    fn timestamp(&self) -> Result<DateTime<Utc>> {
        self.received_timestamp.to_timestamp_millis()
    }
}

impl MsgTimestamp<u64> for SubscriberLocationIngestReport {
    fn timestamp(&self) -> u64 {
        self.received_timestamp.encode_timestamp_millis()
    }
}

impl MsgTimestamp<Result<DateTime<Utc>>> for VerifiedSubscriberLocationIngestReportV1 {
    fn timestamp(&self) -> Result<DateTime<Utc>> {
        self.timestamp.to_timestamp_millis()
    }
}

impl MsgTimestamp<u64> for VerifiedSubscriberLocationIngestReport {
    fn timestamp(&self) -> u64 {
        self.timestamp.encode_timestamp_millis()
    }
}

impl TryFrom<SubscriberLocationIngestReportV1> for SubscriberLocationIngestReport {
    type Error = Error;
    fn try_from(v: SubscriberLocationIngestReportV1) -> Result<Self> {
        Ok(Self {
            received_timestamp: v.timestamp()?,
            report: v
                .report
                .ok_or_else(|| Error::not_found("ingest subscriber location report"))?
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
    type Error = Error;
    fn try_from(v: VerifiedSubscriberLocationIngestReportV1) -> Result<Self> {
        let status = SubscriberReportVerificationStatus::from_i32(v.status).ok_or_else(|| {
            DecodeError::unsupported_status_reason(
                "verified_subscriber_location_ingest_report_v1",
                v.status,
            )
        })?;
        Ok(Self {
            report: v
                .report
                .ok_or_else(|| Error::not_found("ingest subscriber location ingest report"))?
                .try_into()?,
            status,
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
