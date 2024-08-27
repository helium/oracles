use crate::{
    error::DecodeError,
    traits::{MsgDecode, TimestampDecode, TimestampEncode},
    Error, Result,
};
use chrono::{DateTime, Utc};
use helium_crypto::PublicKeyBinary;
use helium_proto::services::poc_mobile::{
    self as proto, PromotionRewardIngestReportV1, PromotionRewardReqV1,
};
use uuid::Uuid;

#[derive(Clone, PartialEq, Hash)]
pub enum Entity {
    SubscriberId(Uuid),
    GatewayKey(PublicKeyBinary),
}

impl TryFrom<proto::promotion_reward_req_v1::Entity> for Entity {
    type Error = Error;

    fn try_from(entity: proto::promotion_reward_req_v1::Entity) -> Result<Self> {
        match entity {
            proto::promotion_reward_req_v1::Entity::SubscriberId(v) => Ok(Entity::SubscriberId(
                Uuid::from_slice(v.as_slice()).map_err(DecodeError::from)?,
            )),
            proto::promotion_reward_req_v1::Entity::GatewayKey(k) => {
                Ok(Entity::GatewayKey(k.into()))
            }
        }
    }
}

impl From<Entity> for proto::promotion_reward_req_v1::Entity {
    fn from(entity: Entity) -> Self {
        match entity {
            Entity::SubscriberId(v) => {
                proto::promotion_reward_req_v1::Entity::SubscriberId(Vec::from(v.into_bytes()))
            }
            Entity::GatewayKey(k) => proto::promotion_reward_req_v1::Entity::GatewayKey(k.into()),
        }
    }
}

impl From<Entity> for proto::promotion_reward::Entity {
    fn from(entity: Entity) -> Self {
        match entity {
            Entity::SubscriberId(v) => {
                proto::promotion_reward::Entity::SubscriberId(Vec::from(v.into_bytes()))
            }
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
                entity.try_into()?
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
