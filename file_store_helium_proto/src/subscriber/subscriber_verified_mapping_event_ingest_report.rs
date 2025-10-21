use chrono::{DateTime, Utc};
use file_store::{
    traits::{MsgDecode, TimestampDecode, TimestampEncode},
    Error, Result,
};
use helium_proto::services::poc_mobile::{
    SubscriberVerifiedMappingEventIngestReportV1, SubscriberVerifiedMappingEventReqV1,
};
use serde::{Deserialize, Serialize};

use crate::{
    subscriber_verified_mapping_event::SubscriberVerifiedMappingEvent, traits::MsgTimestamp,
};

#[derive(Clone, Deserialize, Serialize, Debug, PartialEq)]
pub struct SubscriberVerifiedMappingEventIngestReport {
    pub received_timestamp: DateTime<Utc>,
    pub report: SubscriberVerifiedMappingEvent,
}

impl MsgDecode for SubscriberVerifiedMappingEventIngestReport {
    type Msg = SubscriberVerifiedMappingEventIngestReportV1;
}

impl MsgTimestamp<Result<DateTime<Utc>>> for SubscriberVerifiedMappingEventIngestReportV1 {
    fn timestamp(&self) -> Result<DateTime<Utc>> {
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
    type Error = Error;
    fn try_from(v: SubscriberVerifiedMappingEventIngestReportV1) -> Result<Self> {
        Ok(Self {
            received_timestamp: v.timestamp()?,
            report: v
                .report
                .ok_or_else(|| {
                    Error::not_found("ingest SubscriberVerifiedMappingEventIngestReport report")
                })?
                .try_into()?,
        })
    }
}
