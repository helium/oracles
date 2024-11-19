use crate::{
    radio_location_estimates::RadioLocationEstimatesReq,
    traits::{MsgDecode, MsgTimestamp, TimestampDecode, TimestampEncode},
    Error, Result,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

pub mod proto {
    pub use helium_proto::services::poc_mobile::{
        RadioLocationEstimatesIngestReportV1, RadioLocationEstimatesReqV1,
    };
}

#[derive(Clone, Deserialize, Serialize, Debug, PartialEq)]
pub struct RadioLocationEstimatesIngestReport {
    pub received_timestamp: DateTime<Utc>,
    pub report: RadioLocationEstimatesReq,
}

impl MsgDecode for RadioLocationEstimatesIngestReport {
    type Msg = proto::RadioLocationEstimatesIngestReportV1;
}

impl MsgTimestamp<Result<DateTime<Utc>>> for proto::RadioLocationEstimatesIngestReportV1 {
    fn timestamp(&self) -> Result<DateTime<Utc>> {
        self.received_timestamp.to_timestamp()
    }
}

impl MsgTimestamp<u64> for RadioLocationEstimatesIngestReport {
    fn timestamp(&self) -> u64 {
        self.received_timestamp.encode_timestamp()
    }
}

impl From<RadioLocationEstimatesIngestReport> for proto::RadioLocationEstimatesIngestReportV1 {
    fn from(v: RadioLocationEstimatesIngestReport) -> Self {
        let received_timestamp = v.timestamp();
        let report: proto::RadioLocationEstimatesReqV1 = v.report.into();
        Self {
            received_timestamp,
            report: Some(report),
        }
    }
}

impl TryFrom<proto::RadioLocationEstimatesIngestReportV1> for RadioLocationEstimatesIngestReport {
    type Error = Error;
    fn try_from(v: proto::RadioLocationEstimatesIngestReportV1) -> Result<Self> {
        Ok(Self {
            received_timestamp: v.timestamp()?,
            report: v
                .report
                .ok_or_else(|| {
                    Error::not_found("ingest RadioLocationEstimatesIngestReport report")
                })?
                .try_into()?,
        })
    }
}
