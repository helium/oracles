use chrono::{DateTime, Utc};
use file_store::traits::{
    MsgDecode, TimestampDecode, TimestampDecodeError, TimestampDecodeResult, TimestampEncode,
};
use helium_proto::services::poc_mobile::{
    SubscriberVerifiedMappingEventIngestReportV1, SubscriberVerifiedMappingEventVerificationStatus,
    VerifiedSubscriberVerifiedMappingEventIngestReportV1,
};
use serde::{Deserialize, Serialize};

use crate::{
    subscriber_verified_mapping_event_ingest_report::{
        SubscriberIngestReportError, SubscriberVerifiedMappingEventIngestReport,
    },
    traits::MsgTimestamp,
};

#[derive(thiserror::Error, Debug)]
pub enum VerifiedSubscriberMappingError {
    #[error("invalid timestamp: {0}")]
    Timestamp(#[from] TimestampDecodeError),

    #[error("missing field: {0}")]
    MissingField(&'static str),

    #[error("subscriber ingest report: {0}")]
    SubscriberIngestReport(#[from] SubscriberIngestReportError),
}

#[derive(Clone, Deserialize, Serialize, Debug, PartialEq)]
pub struct VerifiedSubscriberVerifiedMappingEventIngestReport {
    pub report: SubscriberVerifiedMappingEventIngestReport,
    pub status: SubscriberVerifiedMappingEventVerificationStatus,
    pub timestamp: DateTime<Utc>,
}

impl MsgDecode for VerifiedSubscriberVerifiedMappingEventIngestReport {
    type Msg = VerifiedSubscriberVerifiedMappingEventIngestReportV1;
}

impl MsgTimestamp<TimestampDecodeResult> for VerifiedSubscriberVerifiedMappingEventIngestReportV1 {
    fn timestamp(&self) -> TimestampDecodeResult {
        self.timestamp.to_timestamp_millis()
    }
}

impl MsgTimestamp<u64> for VerifiedSubscriberVerifiedMappingEventIngestReport {
    fn timestamp(&self) -> u64 {
        self.timestamp.encode_timestamp_millis()
    }
}

impl From<VerifiedSubscriberVerifiedMappingEventIngestReport>
    for VerifiedSubscriberVerifiedMappingEventIngestReportV1
{
    fn from(v: VerifiedSubscriberVerifiedMappingEventIngestReport) -> Self {
        let timestamp = v.timestamp();
        let report: SubscriberVerifiedMappingEventIngestReportV1 = v.report.into();
        Self {
            report: Some(report),
            status: v.status as i32,
            timestamp,
        }
    }
}

impl TryFrom<VerifiedSubscriberVerifiedMappingEventIngestReportV1>
    for VerifiedSubscriberVerifiedMappingEventIngestReport
{
    type Error = VerifiedSubscriberMappingError;

    fn try_from(
        v: VerifiedSubscriberVerifiedMappingEventIngestReportV1,
    ) -> Result<Self, Self::Error> {
        Ok(Self {
            status: v.status(),
            timestamp: v.timestamp()?,
            report: v
                .report
                .ok_or(VerifiedSubscriberMappingError::MissingField(
                    "verified_subscriber_verified_mapping_event_ingest_report.report",
                ))?
                .try_into()?,
        })
    }
}
