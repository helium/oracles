use crate::{
    traits::{MsgDecode, MsgTimestamp, TimestampDecode},
    Error, Result,
};
use chrono::{DateTime, Utc};
use helium_crypto::PublicKeyBinary;
use helium_proto::services::poc_mobile::{
    SubscriberLocationIngestReportV1, SubscriberLocationReqV1,
};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SubscriberLocationReq {
    pub subscriber_id: Vec<u8>,
    pub timestamp: DateTime<Utc>,
    pub pubkey: PublicKeyBinary,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SubscriberLocationIngestReport {
    pub received_timestamp: DateTime<Utc>,
    pub report: SubscriberLocationReq,
}

impl MsgDecode for SubscriberLocationReq {
    type Msg = SubscriberLocationReqV1;
}

impl MsgDecode for SubscriberLocationIngestReport {
    type Msg = SubscriberLocationIngestReportV1;
}

impl TryFrom<SubscriberLocationReqV1> for SubscriberLocationReq {
    type Error = Error;
    fn try_from(v: SubscriberLocationReqV1) -> Result<Self> {
        Ok(Self {
            subscriber_id: v.subscriber_id,
            timestamp: v.timestamp.to_timestamp()?,
            pubkey: v.carrier_pub_key.into(),
        })
    }
}

impl MsgTimestamp<Result<DateTime<Utc>>> for SubscriberLocationReqV1 {
    fn timestamp(&self) -> Result<DateTime<Utc>> {
        self.timestamp.to_timestamp()
    }
}

impl TryFrom<SubscriberLocationIngestReportV1> for SubscriberLocationIngestReport {
    type Error = Error;
    fn try_from(v: SubscriberLocationIngestReportV1) -> Result<Self> {
        Ok(Self {
            received_timestamp: v.timestamp()?,
            report: v
                .report
                .ok_or_else(|| Error::not_found("ingest subscriber location report"))?
                .try_into()?,
        })
    }
}

impl MsgTimestamp<Result<DateTime<Utc>>> for SubscriberLocationIngestReportV1 {
    fn timestamp(&self) -> Result<DateTime<Utc>> {
        self.received_timestamp.to_timestamp_millis()
    }
}
