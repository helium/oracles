use chrono::{DateTime, Utc};
use helium_crypto::PublicKeyBinary;
use serde::{Deserialize, Serialize};

use file_store::{
    traits::{MsgDecode, TimestampDecode},
    Error,
};

pub mod proto {
    pub use helium_proto::services::poc_mobile::{
        UniqueConnectionsIngestReportV1, UniqueConnectionsReqV1,
        VerifiedUniqueConnectionsIngestReportStatus, VerifiedUniqueConnectionsIngestReportV1,
    };
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct UniqueConnectionsIngestReport {
    pub received_timestamp: DateTime<Utc>,
    pub report: UniqueConnectionReq,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VerifiedUniqueConnectionsIngestReport {
    pub timestamp: DateTime<Utc>,
    pub report: UniqueConnectionsIngestReport,
    pub status: proto::VerifiedUniqueConnectionsIngestReportStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct UniqueConnectionReq {
    pub pubkey: PublicKeyBinary,
    pub start_timestamp: DateTime<Utc>,
    pub end_timestamp: DateTime<Utc>,
    pub unique_connections: u64,
    pub timestamp: DateTime<Utc>,
    pub carrier_key: PublicKeyBinary,
    pub signature: Vec<u8>,
}

impl MsgDecode for UniqueConnectionsIngestReport {
    type Msg = proto::UniqueConnectionsIngestReportV1;
}

impl TryFrom<proto::UniqueConnectionsIngestReportV1> for UniqueConnectionsIngestReport {
    type Error = Error;

    fn try_from(value: proto::UniqueConnectionsIngestReportV1) -> Result<Self, Self::Error> {
        Ok(Self {
            received_timestamp: value.received_timestamp.to_timestamp_millis()?,
            report: value
                .report
                .ok_or_else(|| Error::not_found("ingest unique connections"))?
                .try_into()?,
        })
    }
}

impl From<UniqueConnectionsIngestReport> for proto::UniqueConnectionsIngestReportV1 {
    fn from(value: UniqueConnectionsIngestReport) -> Self {
        Self {
            received_timestamp: value.received_timestamp.timestamp_millis() as u64,
            report: Some(value.report.into()),
        }
    }
}

impl TryFrom<proto::UniqueConnectionsReqV1> for UniqueConnectionReq {
    type Error = Error;

    fn try_from(value: proto::UniqueConnectionsReqV1) -> Result<Self, Self::Error> {
        Ok(Self {
            pubkey: value.pubkey.into(),
            start_timestamp: value.start_timestamp.to_timestamp_millis()?,
            end_timestamp: value.end_timestamp.to_timestamp_millis()?,
            unique_connections: value.unique_connections,
            timestamp: value.timestamp.to_timestamp_millis()?,
            carrier_key: value.carrier_key.into(),
            signature: value.signature,
        })
    }
}

impl From<UniqueConnectionReq> for proto::UniqueConnectionsReqV1 {
    fn from(value: UniqueConnectionReq) -> Self {
        Self {
            pubkey: value.pubkey.into(),
            start_timestamp: value.start_timestamp.timestamp_millis() as u64,
            end_timestamp: value.end_timestamp.timestamp_millis() as u64,
            unique_connections: value.unique_connections,
            timestamp: value.timestamp.timestamp_millis() as u64,
            carrier_key: value.carrier_key.into(),
            signature: value.signature,
        }
    }
}

impl From<VerifiedUniqueConnectionsIngestReport>
    for proto::VerifiedUniqueConnectionsIngestReportV1
{
    fn from(value: VerifiedUniqueConnectionsIngestReport) -> Self {
        Self {
            timestamp: value.timestamp.timestamp_millis() as u64,
            report: Some(value.report.into()),
            status: value.status.into(),
        }
    }
}

#[cfg(test)]
mod tests {

    use file_store::traits::TimestampDecode;

    use super::*;

    #[test]
    fn ingest_report_timestamp_millis() {
        // Ensure timestamp are encode in milliseconds by taking a round trip through proto
        let now = Utc::now();
        // Going to millis and back is necessary to remove nanosecond precision from the used Utc::now().
        let millis = now.timestamp_millis() as u64;
        let timestamp = millis.to_timestamp_millis().unwrap();

        let pubkey = PublicKeyBinary::from(vec![1]);

        let report = UniqueConnectionsIngestReport {
            received_timestamp: timestamp,
            report: UniqueConnectionReq {
                pubkey: pubkey.clone(),
                start_timestamp: timestamp,
                end_timestamp: timestamp,
                unique_connections: 42,
                timestamp,
                carrier_key: pubkey,
                signature: vec![],
            },
        };

        let proto = proto::UniqueConnectionsIngestReportV1::from(report.clone());
        let report_back = UniqueConnectionsIngestReport::try_from(proto).unwrap();

        assert_eq!(report, report_back);
    }
}
