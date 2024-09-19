use crate::{
    traits::{MsgDecode, TimestampDecode, TimestampEncode},
    Error, Result,
};
use chrono::{DateTime, Utc};
use helium_crypto::PublicKeyBinary;
use helium_proto::services::poc_mobile::{
    self as proto, PromotionRewardIngestReportV1, PromotionRewardReqV1,
};

#[derive(Debug, Clone, PartialEq, Hash)]
pub enum Entity {
    SubscriberId(Vec<u8>),
    GatewayKey(PublicKeyBinary),
}

impl From<proto::promotion_reward_req_v1::Entity> for Entity {
    fn from(entity: proto::promotion_reward_req_v1::Entity) -> Self {
        match entity {
            proto::promotion_reward_req_v1::Entity::SubscriberId(v) => Entity::SubscriberId(v),
            proto::promotion_reward_req_v1::Entity::GatewayKey(k) => Entity::GatewayKey(k.into()),
        }
    }
}

impl From<Entity> for proto::promotion_reward_req_v1::Entity {
    fn from(entity: Entity) -> Self {
        match entity {
            Entity::SubscriberId(v) => proto::promotion_reward_req_v1::Entity::SubscriberId(v),
            Entity::GatewayKey(k) => proto::promotion_reward_req_v1::Entity::GatewayKey(k.into()),
        }
    }
}

impl From<Entity> for proto::promotion_reward::Entity {
    fn from(entity: Entity) -> Self {
        match entity {
            Entity::SubscriberId(v) => proto::promotion_reward::Entity::SubscriberId(v),
            Entity::GatewayKey(k) => proto::promotion_reward::Entity::GatewayKey(k.into()),
        }
    }
}

#[derive(Clone)]
pub struct PromotionReward {
    pub entity: Entity,
    pub shares: u64,
    pub timestamp: DateTime<Utc>,
    pub received_timestamp: DateTime<Utc>,
    pub carrier_pub_key: PublicKeyBinary,
    pub signature: Vec<u8>,
}

impl MsgDecode for PromotionReward {
    type Msg = PromotionRewardIngestReportV1;
}

impl TryFrom<PromotionRewardIngestReportV1> for PromotionReward {
    type Error = Error;

    fn try_from(v: PromotionRewardIngestReportV1) -> Result<Self> {
        let received_timestamp = v.received_timestamp.to_timestamp_millis()?;
        let Some(v) = v.report else {
            return Err(Error::NotFound("report".to_string()));
        };
        Ok(Self {
            entity: if let Some(entity) = v.entity {
                entity.into()
            } else {
                return Err(Error::NotFound("entity".to_string()));
            },
            shares: v.shares,
            timestamp: v.timestamp.to_timestamp()?,
            received_timestamp,
            carrier_pub_key: v.carrier_pub_key.into(),
            signature: v.signature,
        })
    }
}

impl From<PromotionReward> for PromotionRewardReqV1 {
    fn from(v: PromotionReward) -> Self {
        Self {
            entity: Some(v.entity.into()),
            shares: v.shares,
            timestamp: v.timestamp.encode_timestamp(),
            carrier_pub_key: v.carrier_pub_key.into(),
            signature: v.signature,
        }
    }
}
