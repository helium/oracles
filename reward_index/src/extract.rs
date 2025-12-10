use crate::{
    indexer::{RewardKey, RewardType},
    settings,
};

use anyhow::Result;
use helium_crypto::PublicKeyBinary;
pub mod proto {
    pub use helium_proto::{
        services::{
            poc_lora::{iot_reward_share::Reward as IotReward, IotRewardShare},
            poc_mobile::{mobile_reward_share::Reward as MobileReward, MobileRewardShare},
        },
        IotRewardToken, MobileRewardToken, ServiceProvider,
    };
}

#[derive(thiserror::Error, Debug)]
pub enum ExtractError {
    #[error("invalid {0} reward share")]
    InvalidRewardShare(settings::Mode),
    #[error("unsupported reward type: {0}")]
    UnsupportedType(&'static str),
    #[error("failed to decode service provider id: {0}")]
    ServiceProviderDecode(i32),
}

pub fn mobile_reward(
    share: proto::MobileRewardShare,
    unallocated_reward_key: &str,
) -> Result<(RewardKey, u64), ExtractError> {
    let Some(reward) = share.reward else {
        return Err(ExtractError::InvalidRewardShare(settings::Mode::Mobile));
    };

    use proto::{MobileReward, ServiceProvider};

    match reward {
        MobileReward::RadioReward(_r) => {
            // RadioReward has been replaced by RadioRewardV2
            Err(ExtractError::UnsupportedType("radio_reward_v1"))
        }
        MobileReward::RadioRewardV2(r) => Ok((
            RewardKey {
                key: PublicKeyBinary::from(r.hotspot_key).to_string(),
                reward_type: RewardType::MobileGateway,
            },
            r.base_poc_reward + r.boosted_poc_reward,
        )),
        MobileReward::GatewayReward(r) => Ok((
            RewardKey {
                key: PublicKeyBinary::from(r.hotspot_key).to_string(),
                reward_type: RewardType::MobileGateway,
            },
            r.dc_transfer_reward,
        )),
        MobileReward::SubscriberReward(r) => {
            let key = if r.reward_override_entity_key.trim().is_empty() {
                bs58::encode(&r.subscriber_id).into_string()
            } else {
                r.reward_override_entity_key
            };

            Ok((
                RewardKey {
                    key,
                    reward_type: RewardType::MobileSubscriber,
                },
                r.discovery_location_amount + r.verification_mapping_amount,
            ))
        }
        MobileReward::ServiceProviderReward(r) => {
            let sp = ServiceProvider::try_from(r.service_provider_id)
                .map_err(|_| ExtractError::ServiceProviderDecode(r.service_provider_id))?;

            #[allow(unreachable_patterns)]
            let sp_key = match sp {
                ServiceProvider::HeliumMobile => r.service_provider_reward_type,
                _ => sp.to_string(),
            };

            Ok((
                RewardKey {
                    key: sp_key,
                    reward_type: RewardType::MobileServiceProvider,
                },
                r.amount,
            ))
        }
        MobileReward::UnallocatedReward(r) => Ok((
            RewardKey {
                key: unallocated_reward_key.to_string(),
                reward_type: RewardType::MobileUnallocated,
            },
            r.amount,
        )),
        MobileReward::PromotionReward(promotion) => Ok((
            RewardKey {
                key: promotion.entity,
                reward_type: RewardType::MobilePromotion,
            },
            promotion.service_provider_amount + promotion.matched_amount,
        )),
    }
}

pub fn iot_reward(
    share: proto::IotRewardShare,
    op_fund_key: &str,
    unallocated_reward_key: &str,
) -> Result<(RewardKey, u64), ExtractError> {
    let Some(reward) = share.reward else {
        return Err(ExtractError::InvalidRewardShare(settings::Mode::Iot));
    };

    use proto::IotReward;

    match reward {
        IotReward::GatewayReward(r) => Ok((
            RewardKey {
                key: PublicKeyBinary::from(r.hotspot_key).to_string(),
                reward_type: RewardType::IotGateway,
            },
            r.witness_amount + r.beacon_amount + r.dc_transfer_amount,
        )),
        IotReward::OperationalReward(r) => Ok((
            RewardKey {
                key: op_fund_key.to_string(),
                reward_type: RewardType::IotOperational,
            },
            r.amount,
        )),
        IotReward::UnallocatedReward(r) => Ok((
            RewardKey {
                key: unallocated_reward_key.to_string(),
                reward_type: RewardType::IotUnallocated,
            },
            r.amount,
        )),
    }
}
#[cfg(test)]
mod tests {

    use crate::indexer::RewardType;

    use super::*;

    use chrono::Utc;
    use helium_proto::services::poc_lora::GatewayReward as IotGatewayReward;
    use helium_proto::services::poc_mobile::GatewayReward as MobileGatewayReward;

    #[test]
    fn test_extract_mobile_reward() -> anyhow::Result<()> {
        let reward = proto::MobileRewardShare {
            start_period: Utc::now().timestamp_millis() as u64,
            end_period: Utc::now().timestamp_millis() as u64,
            reward: Some(proto::MobileReward::GatewayReward(MobileGatewayReward {
                hotspot_key: vec![1],
                dc_transfer_reward: 1,
                rewardable_bytes: 2,
                price: 3,
            })),
        };

        let (reward_key, amount) = mobile_reward(reward, "unallocated-key")?;
        assert_eq!(reward_key.key, PublicKeyBinary::from(vec![1]).to_string());
        assert_eq!(reward_key.reward_type, RewardType::MobileGateway);
        assert_eq!(amount, 1, "only dc_transfer_reward");

        Ok(())
    }

    #[test]
    fn test_extract_iot_reward() -> anyhow::Result<()> {
        let reward = proto::IotRewardShare {
            start_period: Utc::now().timestamp_millis() as u64,
            end_period: Utc::now().timestamp_millis() as u64,
            reward: Some(proto::IotReward::GatewayReward(IotGatewayReward {
                hotspot_key: vec![1],
                beacon_amount: 1,
                witness_amount: 2,
                dc_transfer_amount: 3,
            })),
        };

        let (reward_key, amount) = iot_reward(reward, "op-fund-key", "unallocated-key")?;
        assert_eq!(reward_key.key, PublicKeyBinary::from(vec![1]).to_string());
        assert_eq!(reward_key.reward_type, RewardType::IotGateway);
        assert_eq!(amount, 6, "all reward added together");

        Ok(())
    }
}
