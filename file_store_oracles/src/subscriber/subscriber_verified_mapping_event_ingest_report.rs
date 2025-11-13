use chrono::{DateTime, Utc};
use file_store::traits::{
    MsgDecode, TimestampDecode, TimestampDecodeError, TimestampDecodeResult, TimestampEncode,
};
use helium_proto::services::poc_mobile::{
    SubscriberVerifiedMappingEventIngestReportV1, SubscriberVerifiedMappingEventReqV1,
};
use serde::{Deserialize, Serialize};

use crate::{
    subscriber_verified_mapping_event::{SubscriberMappingError, SubscriberVerifiedMappingEvent},
    traits::MsgTimestamp,
};

#[derive(thiserror::Error, Debug)]
pub enum SubscriberIngestReportError {
    #[error("invalid timestamp: {0}")]
    Timestamp(#[from] TimestampDecodeError),

    #[error("missing field: {0}")]
    MissingField(&'static str),

    #[error("subscriber location: {0}")]
    SubscriberLocation(#[from] SubscriberMappingError),
}

#[derive(Clone, Deserialize, Serialize, Debug, PartialEq)]
pub struct SubscriberVerifiedMappingEventIngestReport {
    pub received_timestamp: DateTime<Utc>,
    pub report: SubscriberVerifiedMappingEvent,
}

impl MsgDecode for SubscriberVerifiedMappingEventIngestReport {
    type Msg = SubscriberVerifiedMappingEventIngestReportV1;
}

impl MsgTimestamp<TimestampDecodeResult> for SubscriberVerifiedMappingEventIngestReportV1 {
    fn timestamp(&self) -> TimestampDecodeResult {
        self.received_timestamp.to_timestamp_millis()
    }
}

impl MsgTimestamp<u64> for SubscriberVerifiedMappingEventIngestReport {
    fn timestamp(&self) -> u64 {
        self.received_timestamp.encode_timestamp_millis()
    }
}

impl From<SubscriberVerifiedMappingEventIngestReport>
    for SubscriberVerifiedMappingEventIngestReportV1
{
    fn from(v: SubscriberVerifiedMappingEventIngestReport) -> Self {
        let received_timestamp = v.timestamp();
        let report: SubscriberVerifiedMappingEventReqV1 = v.report.into();
        Self {
            received_timestamp,
            report: Some(report),
        }
    }
}

impl TryFrom<SubscriberVerifiedMappingEventIngestReportV1>
    for SubscriberVerifiedMappingEventIngestReport
{
    type Error = SubscriberIngestReportError;

    fn try_from(v: SubscriberVerifiedMappingEventIngestReportV1) -> Result<Self, Self::Error> {
        Ok(Self {
            received_timestamp: v.timestamp()?,
            report: v
                .report
                .ok_or(SubscriberIngestReportError::MissingField(
                    "subscriber_verified_mapping_event_ingest_report.report",
                ))?
                .try_into()?,
        })
    }
}
