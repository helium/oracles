use crate::{
    traits::{MsgDecode, MsgTimestamp, TimestampDecode, TimestampEncode},
    Error, Result,
};
use chrono::{DateTime, Utc};
use helium_proto::services::poc_mobile::{
    VerifiedSubscriberMappingEventIngestReportV1, VerifiedSubscriberMappingEventReqV1,
};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct VerifiedSubscriberMappingEvent {
    pub subscriber_id: String,
    pub total_reward_points: u64,
    pub timestamp: DateTime<Utc>,
}
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct VerifiedSubscriberMappingEventIngestReport {
    pub received_timestamp: DateTime<Utc>,
    pub report: VerifiedSubscriberMappingEvent,
}

impl MsgDecode for VerifiedSubscriberMappingEvent {
    type Msg = VerifiedSubscriberMappingEventReqV1;
}

impl MsgDecode for VerifiedSubscriberMappingEventIngestReport {
    type Msg = VerifiedSubscriberMappingEventIngestReportV1;
}

impl TryFrom<VerifiedSubscriberMappingEventReqV1> for VerifiedSubscriberMappingEvent {
    type Error = Error;
    fn try_from(v: VerifiedSubscriberMappingEventReqV1) -> Result<Self> {
        Ok(Self {
            subscriber_id: v.subscriber_id,
            total_reward_points: v.total_reward_points,
            timestamp: v.timestamp.to_timestamp()?,
        })
    }
}

impl From<VerifiedSubscriberMappingEvent> for VerifiedSubscriberMappingEventReqV1 {
    fn from(v: VerifiedSubscriberMappingEvent) -> Self {
        let timestamp = v.timestamp();
        Self {
            subscriber_id: v.subscriber_id,
            total_reward_points: v.total_reward_points,
            timestamp,
        }
    }
}

impl MsgTimestamp<Result<DateTime<Utc>>> for VerifiedSubscriberMappingEventReqV1 {
    fn timestamp(&self) -> Result<DateTime<Utc>> {
        self.timestamp.to_timestamp()
    }
}

impl MsgTimestamp<u64> for VerifiedSubscriberMappingEvent {
    fn timestamp(&self) -> u64 {
        self.timestamp.encode_timestamp()
    }
}

impl MsgTimestamp<Result<DateTime<Utc>>> for VerifiedSubscriberMappingEventIngestReportV1 {
    fn timestamp(&self) -> Result<DateTime<Utc>> {
        self.received_timestamp.to_timestamp_millis()
    }
}

impl MsgTimestamp<u64> for VerifiedSubscriberMappingEventIngestReport {
    fn timestamp(&self) -> u64 {
        self.received_timestamp.encode_timestamp_millis()
    }
}

impl TryFrom<VerifiedSubscriberMappingEventIngestReportV1>
    for VerifiedSubscriberMappingEventIngestReport
{
    type Error = Error;
    fn try_from(v: VerifiedSubscriberMappingEventIngestReportV1) -> Result<Self> {
        Ok(Self {
            received_timestamp: v.timestamp()?,
            report: v
                .report
                .ok_or_else(|| Error::not_found("verified subscriber mapping event ingest report"))?
                .try_into()?,
        })
    }
}

impl From<VerifiedSubscriberMappingEventIngestReport>
    for VerifiedSubscriberMappingEventIngestReportV1
{
    fn from(v: VerifiedSubscriberMappingEventIngestReport) -> Self {
        let received_timestamp = v.timestamp();
        let report: VerifiedSubscriberMappingEventReqV1 = v.report.into();
        Self {
            received_timestamp,
            report: Some(report),
        }
    }
}

#[cfg(test)]
mod tests {
    use chrono::NaiveDateTime;

    use super::*;

    #[test]
    fn from_proto() -> anyhow::Result<()> {
        let proto = VerifiedSubscriberMappingEventIngestReportV1 {
            received_timestamp: 1712624400000,
            report: Some(VerifiedSubscriberMappingEventReqV1 {
                subscriber_id: "sub1".to_string(),
                total_reward_points: 1000,
                timestamp: 1712624400,
            }),
        };

        let report = VerifiedSubscriberMappingEventIngestReport::try_from(proto)?;
        assert_eq!(parse_dt("2024-04-09 01:00:00"), report.received_timestamp);
        assert_eq!("sub1", report.report.subscriber_id);
        assert_eq!(1000, report.report.total_reward_points);
        assert_eq!(parse_dt("2024-04-09 01:00:00"), report.report.timestamp);

        Ok(())
    }

    fn parse_dt(dt: &str) -> DateTime<Utc> {
        NaiveDateTime::parse_from_str(dt, "%Y-%m-%d %H:%M:%S")
            .expect("unable_to_parse")
            .and_utc()
    }
}
