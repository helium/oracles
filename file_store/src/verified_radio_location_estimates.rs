use crate::{
    radio_location_estimates_ingest_report::RadioLocationEstimatesIngestReport,
    traits::{MsgDecode, MsgTimestamp, TimestampDecode, TimestampEncode},
    Error, Result,
};
use chrono::{DateTime, Utc};
use helium_proto::services::poc_mobile::{
    RadioLocationEstimatesIngestReportV1, RadioLocationEstimatesVerificationStatus,
    VerifiedRadioLocationEstimatesReportV1,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Deserialize, Serialize, Debug, PartialEq)]
pub struct VerifiedRadioLocationEstimatesReport {
    pub report: RadioLocationEstimatesIngestReport,
    pub status: RadioLocationEstimatesVerificationStatus,
    pub timestamp: DateTime<Utc>,
}

impl MsgDecode for VerifiedRadioLocationEstimatesReport {
    type Msg = VerifiedRadioLocationEstimatesReportV1;
}

impl MsgTimestamp<Result<DateTime<Utc>>> for VerifiedRadioLocationEstimatesReportV1 {
    fn timestamp(&self) -> Result<DateTime<Utc>> {
        self.timestamp.to_timestamp()
    }
}

impl MsgTimestamp<u64> for VerifiedRadioLocationEstimatesReport {
    fn timestamp(&self) -> u64 {
        self.timestamp.encode_timestamp()
    }
}

impl From<VerifiedRadioLocationEstimatesReport> for VerifiedRadioLocationEstimatesReportV1 {
    fn from(v: VerifiedRadioLocationEstimatesReport) -> Self {
        let timestamp = v.timestamp();
        let report: RadioLocationEstimatesIngestReportV1 = v.report.into();
        Self {
            report: Some(report),
            status: v.status as i32,
            timestamp,
        }
    }
}

impl TryFrom<VerifiedRadioLocationEstimatesReportV1> for VerifiedRadioLocationEstimatesReport {
    type Error = Error;
    fn try_from(v: VerifiedRadioLocationEstimatesReportV1) -> Result<Self> {
        Ok(Self {
            report: v
                .clone()
                .report
                .ok_or_else(|| {
                    Error::not_found("ingest VerifiedRadioLocationEstimatesReport report")
                })?
                .try_into()?,
            status: v.status.try_into()?,
            timestamp: v.timestamp()?,
        })
    }
}
