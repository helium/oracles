use chrono::{DateTime, Utc};
use file_store::{
    traits::{MsgDecode, TimestampDecode, TimestampEncode},
    Error, Result,
};
use helium_proto::services::poc_mobile::{
    SubscriberVerifiedMappingEventIngestReportV1, SubscriberVerifiedMappingEventVerificationStatus,
    VerifiedSubscriberVerifiedMappingEventIngestReportV1,
};
use serde::{Deserialize, Serialize};

use crate::{
    subscriber_verified_mapping_event_ingest_report::SubscriberVerifiedMappingEventIngestReport,
    traits::MsgTimestamp,
};

#[derive(Clone, Deserialize, Serialize, Debug, PartialEq)]
pub struct VerifiedSubscriberVerifiedMappingEventIngestReport {
    pub report: SubscriberVerifiedMappingEventIngestReport,
    pub status: SubscriberVerifiedMappingEventVerificationStatus,
    pub timestamp: DateTime<Utc>,
}

impl MsgDecode for VerifiedSubscriberVerifiedMappingEventIngestReport {
    type Msg = VerifiedSubscriberVerifiedMappingEventIngestReportV1;
}

impl MsgTimestamp<Result<DateTime<Utc>>> for VerifiedSubscriberVerifiedMappingEventIngestReportV1 {
    fn timestamp(&self) -> Result<DateTime<Utc>> {
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
    type Error = Error;
    fn try_from(v: VerifiedSubscriberVerifiedMappingEventIngestReportV1) -> Result<Self> {
        Ok(Self {
            report: v
                .clone()
                .report
                .ok_or_else(|| {
                    Error::not_found(
                        "ingest VerifiedSubscriberVerifiedMappingEventIngestReport report",
                    )
                })?
                .try_into()?,
            status: v.status(),
            timestamp: v.timestamp()?,
        })
    }
}
