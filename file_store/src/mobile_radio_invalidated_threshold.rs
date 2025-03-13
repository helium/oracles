use crate::{
    error::DecodeError,
    traits::{MsgDecode, MsgTimestamp, TimestampDecode, TimestampEncode},
    Error, Result,
};
use chrono::{DateTime, Utc};
use helium_crypto::PublicKeyBinary;
use helium_proto::services::poc_mobile::{
    InvalidatedRadioThresholdIngestReportV1, InvalidatedRadioThresholdReportReqV1,
    InvalidatedRadioThresholdReportVerificationStatus, InvalidatedThresholdReason,
    VerifiedInvalidatedRadioThresholdIngestReportV1,
};
use serde::{Deserialize, Serialize};

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
    type Error = Error;
    fn try_from(v: InvalidatedRadioThresholdReportReqV1) -> Result<Self> {
        let reason = InvalidatedThresholdReason::try_from(v.reason).map_err(|_| {
            DecodeError::unsupported_invalidated_reason(
                "invalidated_radio_threshold_report_req_v1",
                v.reason,
            )
        })?;
        Ok(Self {
            hotspot_pubkey: v.hotspot_pubkey.into(),
            reason,
            timestamp: v.timestamp.to_timestamp()?,
            carrier_pub_key: v.carrier_pub_key.into(),
        })
    }
}

impl From<InvalidatedRadioThresholdReportReq> for InvalidatedRadioThresholdReportReqV1 {
    fn from(v: InvalidatedRadioThresholdReportReq) -> Self {
        let timestamp = v.timestamp.timestamp() as u64;
        Self {
            cbsd_id: String::default(),
            hotspot_pubkey: v.hotspot_pubkey.into(),
            reason: v.reason as i32,
            timestamp,
            carrier_pub_key: v.carrier_pub_key.into(),
            signature: vec![],
        }
    }
}

impl MsgTimestamp<Result<DateTime<Utc>>> for InvalidatedRadioThresholdReportReqV1 {
    fn timestamp(&self) -> Result<DateTime<Utc>> {
        self.timestamp.to_timestamp()
    }
}

impl MsgTimestamp<u64> for InvalidatedRadioThresholdReportReq {
    fn timestamp(&self) -> u64 {
        self.timestamp.encode_timestamp()
    }
}

impl MsgTimestamp<Result<DateTime<Utc>>> for InvalidatedRadioThresholdIngestReportV1 {
    fn timestamp(&self) -> Result<DateTime<Utc>> {
        self.received_timestamp.to_timestamp_millis()
    }
}

impl MsgTimestamp<u64> for InvalidatedRadioThresholdIngestReport {
    fn timestamp(&self) -> u64 {
        self.received_timestamp.encode_timestamp_millis()
    }
}

impl MsgTimestamp<Result<DateTime<Utc>>> for VerifiedInvalidatedRadioThresholdIngestReportV1 {
    fn timestamp(&self) -> Result<DateTime<Utc>> {
        self.timestamp.to_timestamp_millis()
    }
}

impl MsgTimestamp<u64> for VerifiedInvalidatedRadioThresholdIngestReport {
    fn timestamp(&self) -> u64 {
        self.timestamp.encode_timestamp_millis()
    }
}

impl TryFrom<InvalidatedRadioThresholdIngestReportV1> for InvalidatedRadioThresholdIngestReport {
    type Error = Error;
    fn try_from(v: InvalidatedRadioThresholdIngestReportV1) -> Result<Self> {
        Ok(Self {
            received_timestamp: v.timestamp()?,
            report: v
                .report
                .ok_or_else(|| {
                    Error::not_found("ingest invalidated radio threshold ingest report")
                })?
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
    type Error = Error;
    fn try_from(v: VerifiedInvalidatedRadioThresholdIngestReportV1) -> Result<Self> {
        let status = InvalidatedRadioThresholdReportVerificationStatus::try_from(v.status)
            .map_err(|_| {
                DecodeError::unsupported_status_reason(
                    "verified_invalidated_radio_threshold_ingest_report_v1",
                    v.status,
                )
            })?;
        let report = v
            .report
            .ok_or_else(|| Error::not_found("ingest invalidated radio threshold ingest report"))?
            .try_into()?;
        Ok(Self {
            report,
            status,
            timestamp: v.timestamp.to_timestamp()?,
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
