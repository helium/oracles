use crate::{
    traits::{MsgDecode, MsgTimestamp, TimestampDecode, TimestampEncode},
    Error, Result,
};
use chrono::{DateTime, Utc};
use helium_crypto::PublicKeyBinary;
use helium_proto::services::poc_mobile::VerifiedSubscriberMappingEventReqV1;
use serde::{Deserialize, Serialize};
use sqlx::Row;

#[derive(Clone, Deserialize, Serialize, Debug, PartialEq)]
pub struct VerifiedSubscriberMappingEvent {
    pub subscriber_id: Vec<u8>,
    pub total_reward_points: u64,
    pub timestamp: DateTime<Utc>,
    pub carrier_mapping_key: PublicKeyBinary,
}

impl MsgDecode for VerifiedSubscriberMappingEvent {
    type Msg = VerifiedSubscriberMappingEventReqV1;
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

impl From<VerifiedSubscriberMappingEvent> for VerifiedSubscriberMappingEventReqV1 {
    fn from(v: VerifiedSubscriberMappingEvent) -> Self {
        let timestamp = v.timestamp();
        VerifiedSubscriberMappingEventReqV1 {
            subscriber_id: v.subscriber_id,
            total_reward_points: v.total_reward_points,
            timestamp,
            carrier_mapping_key: v.carrier_mapping_key.into(),
            signature: vec![],
        }
    }
}

impl TryFrom<VerifiedSubscriberMappingEventReqV1> for VerifiedSubscriberMappingEvent {
    type Error = Error;
    fn try_from(v: VerifiedSubscriberMappingEventReqV1) -> Result<Self> {
        let timestamp = v.timestamp()?;
        Ok(Self {
            subscriber_id: v.subscriber_id,
            total_reward_points: v.total_reward_points,
            timestamp,
            carrier_mapping_key: v.carrier_mapping_key.into(),
        })
    }
}

impl sqlx::FromRow<'_, sqlx::postgres::PgRow> for VerifiedSubscriberMappingEvent {
    fn from_row(row: &sqlx::postgres::PgRow) -> sqlx::Result<Self> {
        Ok(Self {
            subscriber_id: row.get::<Vec<u8>, &str>("subscriber_id"),
            total_reward_points: row.get::<i32, &str>("total_reward_points") as u64,
            timestamp: row.get::<DateTime<Utc>, &str>("timestamp"),
            carrier_mapping_key: vec![].into(),
        })
    }
}
