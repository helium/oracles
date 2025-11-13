use chrono::{DateTime, Utc};
use file_store::traits::{
    MsgDecode, TimestampDecode, TimestampDecodeError, TimestampDecodeResult, TimestampEncode,
};
use helium_crypto::PublicKeyBinary;
use helium_proto::services::poc_mobile::SubscriberVerifiedMappingEventReqV1;
use serde::{Deserialize, Serialize};

use crate::traits::MsgTimestamp;

#[derive(thiserror::Error, Debug)]
pub enum SubscriberMappingError {
    #[error("invalid timestamp: {0}")]
    Timestamp(#[from] TimestampDecodeError),
}

#[derive(Clone, Deserialize, Serialize, Debug, PartialEq)]
pub struct SubscriberVerifiedMappingEvent {
    pub subscriber_id: Vec<u8>,
    pub total_reward_points: u64,
    pub timestamp: DateTime<Utc>,
    pub carrier_mapping_key: PublicKeyBinary,
}

impl MsgDecode for SubscriberVerifiedMappingEvent {
    type Msg = SubscriberVerifiedMappingEventReqV1;
}

impl MsgTimestamp<TimestampDecodeResult> for SubscriberVerifiedMappingEventReqV1 {
    fn timestamp(&self) -> TimestampDecodeResult {
        self.timestamp.to_timestamp()
    }
}

impl MsgTimestamp<u64> for SubscriberVerifiedMappingEvent {
    fn timestamp(&self) -> u64 {
        self.timestamp.encode_timestamp()
    }
}

impl From<SubscriberVerifiedMappingEvent> for SubscriberVerifiedMappingEventReqV1 {
    fn from(v: SubscriberVerifiedMappingEvent) -> Self {
        let timestamp = v.timestamp();
        SubscriberVerifiedMappingEventReqV1 {
            subscriber_id: v.subscriber_id,
            total_reward_points: v.total_reward_points,
            timestamp,
            carrier_mapping_key: v.carrier_mapping_key.into(),
            signature: vec![],
        }
    }
}

impl TryFrom<SubscriberVerifiedMappingEventReqV1> for SubscriberVerifiedMappingEvent {
    type Error = SubscriberMappingError;

    fn try_from(v: SubscriberVerifiedMappingEventReqV1) -> Result<Self, Self::Error> {
        let timestamp = v.timestamp()?;
        Ok(Self {
            subscriber_id: v.subscriber_id,
            total_reward_points: v.total_reward_points,
            timestamp,
            carrier_mapping_key: v.carrier_mapping_key.into(),
        })
    }
}
