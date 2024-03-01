use crate::{
    error::DecodeError,
    traits::{MsgDecode, MsgTimestamp, TimestampDecode, TimestampEncode},
    Error, Result,
};
use chrono::{DateTime, Utc};
use helium_crypto::PublicKeyBinary;
use helium_proto::services::poc_mobile::{
    RadioThresholdIngestReportV1, RadioThresholdReportReqV1,
    RadioThresholdReportVerificationStatus, VerifiedRadioThresholdIngestReportV1,
};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RadioThresholdReportReq {
    pub hotspot_pubkey: PublicKeyBinary,
    pub cbsd_id: Option<String>,
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
    type Error = Error;
    fn try_from(v: RadioThresholdReportReqV1) -> Result<Self> {
        Ok(Self {
            cbsd_id: v.cbsd_id.parse().ok(),
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
            cbsd_id: v.cbsd_id.unwrap_or_default(),
            hotspot_pubkey: v.hotspot_pubkey.into(),
            bytes_threshold: v.bytes_threshold,
            subscriber_threshold: v.subscriber_threshold,
            threshold_timestamp,
            carrier_pub_key: v.carrier_pub_key.into(),
            signature: vec![],
        }
    }
}

impl MsgTimestamp<Result<DateTime<Utc>>> for RadioThresholdReportReqV1 {
    fn timestamp(&self) -> Result<DateTime<Utc>> {
        self.threshold_timestamp.to_timestamp()
    }
}

impl MsgTimestamp<u64> for RadioThresholdReportReq {
    fn timestamp(&self) -> u64 {
        self.threshold_timestamp.encode_timestamp()
    }
}

impl MsgTimestamp<Result<DateTime<Utc>>> for RadioThresholdIngestReportV1 {
    fn timestamp(&self) -> Result<DateTime<Utc>> {
        self.received_timestamp.to_timestamp_millis()
    }
}

impl MsgTimestamp<u64> for RadioThresholdIngestReport {
    fn timestamp(&self) -> u64 {
        self.received_timestamp.encode_timestamp_millis()
    }
}

impl MsgTimestamp<Result<DateTime<Utc>>> for VerifiedRadioThresholdIngestReportV1 {
    fn timestamp(&self) -> Result<DateTime<Utc>> {
        self.timestamp.to_timestamp_millis()
    }
}

impl MsgTimestamp<u64> for VerifiedRadioThresholdIngestReport {
    fn timestamp(&self) -> u64 {
        self.timestamp.encode_timestamp_millis()
    }
}

impl TryFrom<RadioThresholdIngestReportV1> for RadioThresholdIngestReport {
    type Error = Error;
    fn try_from(v: RadioThresholdIngestReportV1) -> Result<Self> {
        Ok(Self {
            received_timestamp: v.timestamp()?,
            report: v
                .report
                .ok_or_else(|| Error::not_found("ingest radio threshold ingest report"))?
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
    type Error = Error;
    fn try_from(v: VerifiedRadioThresholdIngestReportV1) -> Result<Self> {
        let status =
            RadioThresholdReportVerificationStatus::from_i32(v.status).ok_or_else(|| {
                DecodeError::unsupported_status_reason(
                    "verified_radio_threshold_ingest_report_v1",
                    v.status,
                )
            })?;
        Ok(Self {
            report: v
                .report
                .ok_or_else(|| Error::not_found("ingest radio threshold ingest report"))?
                .try_into()?,
            status,
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
