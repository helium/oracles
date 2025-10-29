use chrono::{DateTime, Utc};
use file_store::traits::{
    MsgDecode, TimestampDecode, TimestampDecodeError, TimestampDecodeResult, TimestampEncode,
};
use helium_crypto::PublicKeyBinary;
use helium_proto::services::poc_mobile::{
    InvalidatedRadioThresholdIngestReportV1, InvalidatedRadioThresholdReportReqV1,
    InvalidatedRadioThresholdReportVerificationStatus, InvalidatedThresholdReason,
    VerifiedInvalidatedRadioThresholdIngestReportV1,
};
use serde::{Deserialize, Serialize};

use crate::{prost_enum, traits::MsgTimestamp};

#[derive(thiserror::Error, Debug)]
pub enum MobileBanInvalidatedThresholdError {
    #[error("invalid timestamp: {0}")]
    Timestamp(#[from] TimestampDecodeError),

    #[error("missing field: {0}")]
    MissingField(&'static str),

    #[error("unsupported reason: {0}")]
    Reason(prost::UnknownEnumValue),

    #[error("unsupported status: {0}")]
    Status(prost::UnknownEnumValue),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct InvalidatedRadioThresholdReportReq {
    pub hotspot_pubkey: PublicKeyBinary,
    pub reason: InvalidatedThresholdReason,
    pub timestamp: DateTime<Utc>,
    pub carrier_pub_key: PublicKeyBinary,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct VerifiedInvalidatedRadioThresholdIngestReport {
    pub report: InvalidatedRadioThresholdIngestReport,
    pub status: InvalidatedRadioThresholdReportVerificationStatus,
    pub timestamp: DateTime<Utc>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct InvalidatedRadioThresholdIngestReport {
    pub received_timestamp: DateTime<Utc>,
    pub report: InvalidatedRadioThresholdReportReq,
}

impl MsgDecode for InvalidatedRadioThresholdReportReq {
    type Msg = InvalidatedRadioThresholdReportReqV1;
}

impl MsgDecode for InvalidatedRadioThresholdIngestReport {
    type Msg = InvalidatedRadioThresholdIngestReportV1;
}

impl MsgDecode for VerifiedInvalidatedRadioThresholdIngestReport {
    type Msg = VerifiedInvalidatedRadioThresholdIngestReportV1;
}

impl TryFrom<InvalidatedRadioThresholdReportReqV1> for InvalidatedRadioThresholdReportReq {
    type Error = MobileBanInvalidatedThresholdError;

    fn try_from(v: InvalidatedRadioThresholdReportReqV1) -> Result<Self, Self::Error> {
        Ok(Self {
            reason: prost_enum(v.reason, MobileBanInvalidatedThresholdError::Reason)?,
            timestamp: v.timestamp()?,
            hotspot_pubkey: v.hotspot_pubkey.into(),
            carrier_pub_key: v.carrier_pub_key.into(),
        })
    }
}

impl From<InvalidatedRadioThresholdReportReq> for InvalidatedRadioThresholdReportReqV1 {
    fn from(v: InvalidatedRadioThresholdReportReq) -> Self {
        Self {
            cbsd_id: String::default(),
            timestamp: v.timestamp(),
            hotspot_pubkey: v.hotspot_pubkey.into(),
            reason: v.reason as i32,
            carrier_pub_key: v.carrier_pub_key.into(),
            signature: vec![],
        }
    }
}

impl MsgTimestamp<TimestampDecodeResult> for InvalidatedRadioThresholdReportReqV1 {
    fn timestamp(&self) -> TimestampDecodeResult {
        self.timestamp.to_timestamp()
    }
}

impl MsgTimestamp<u64> for InvalidatedRadioThresholdReportReq {
    fn timestamp(&self) -> u64 {
        self.timestamp.encode_timestamp()
    }
}

impl MsgTimestamp<TimestampDecodeResult> for InvalidatedRadioThresholdIngestReportV1 {
    fn timestamp(&self) -> TimestampDecodeResult {
        self.received_timestamp.to_timestamp_millis()
    }
}

impl MsgTimestamp<u64> for InvalidatedRadioThresholdIngestReport {
    fn timestamp(&self) -> u64 {
        self.received_timestamp.encode_timestamp_millis()
    }
}

impl MsgTimestamp<TimestampDecodeResult> for VerifiedInvalidatedRadioThresholdIngestReportV1 {
    fn timestamp(&self) -> TimestampDecodeResult {
        self.timestamp.to_timestamp_millis()
    }
}

impl MsgTimestamp<u64> for VerifiedInvalidatedRadioThresholdIngestReport {
    fn timestamp(&self) -> u64 {
        self.timestamp.encode_timestamp_millis()
    }
}

impl TryFrom<InvalidatedRadioThresholdIngestReportV1> for InvalidatedRadioThresholdIngestReport {
    type Error = MobileBanInvalidatedThresholdError;

    fn try_from(v: InvalidatedRadioThresholdIngestReportV1) -> Result<Self, Self::Error> {
        Ok(Self {
            received_timestamp: v.timestamp()?,
            report: v
                .report
                .ok_or(MobileBanInvalidatedThresholdError::MissingField(
                    "invalidated_radio_threshold_ingest_report.report",
                ))?
                .try_into()?,
        })
    }
}

impl From<InvalidatedRadioThresholdIngestReport> for InvalidatedRadioThresholdIngestReportV1 {
    fn from(v: InvalidatedRadioThresholdIngestReport) -> Self {
        let received_timestamp = v.timestamp();
        let report: InvalidatedRadioThresholdReportReqV1 = v.report.into();
        Self {
            received_timestamp,
            report: Some(report),
        }
    }
}

impl TryFrom<VerifiedInvalidatedRadioThresholdIngestReportV1>
    for VerifiedInvalidatedRadioThresholdIngestReport
{
    type Error = MobileBanInvalidatedThresholdError;

    fn try_from(v: VerifiedInvalidatedRadioThresholdIngestReportV1) -> Result<Self, Self::Error> {
        Ok(Self {
            status: prost_enum(v.status, MobileBanInvalidatedThresholdError::Status)?,
            timestamp: v.timestamp()?,
            report: v
                .report
                .ok_or(MobileBanInvalidatedThresholdError::MissingField(
                    "verified_invalidated_radio_threshold_ingest_report.report",
                ))?
                .try_into()?,
        })
    }
}

impl From<VerifiedInvalidatedRadioThresholdIngestReport>
    for VerifiedInvalidatedRadioThresholdIngestReportV1
{
    fn from(v: VerifiedInvalidatedRadioThresholdIngestReport) -> Self {
        let timestamp = v.timestamp();
        let report: InvalidatedRadioThresholdIngestReportV1 = v.report.into();
        Self {
            report: Some(report),
            status: v.status as i32,
            timestamp,
        }
    }
}
