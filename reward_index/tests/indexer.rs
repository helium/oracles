use chrono::Utc;
use file_store::traits::MsgBytes;
use helium_crypto::PublicKeyBinary;
use helium_proto::services::{
    poc_lora::{iot_reward_share, GatewayReward as IotGatewayReward, IotRewardShare},
    poc_mobile::{mobile_reward_share, GatewayReward as MobileGatewayReward, MobileRewardShare},
};
use reward_index::{
    indexer::{extract_iot_reward, extract_mobile_reward, extract_reward_share, RewardType},
    settings,
};

#[test]
fn test_extract_mobile_reward() -> anyhow::Result<()> {
    let mobile_reward = MobileRewardShare {
        start_period: Utc::now().timestamp_millis() as u64,
        end_period: Utc::now().timestamp_millis() as u64,
        reward: Some(mobile_reward_share::Reward::GatewayReward(
            MobileGatewayReward {
                hotspot_key: vec![1],
                dc_transfer_reward: 1,
                rewardable_bytes: 2,
                price: 3,
            },
        )),
    };

    let (reward_key, amount) =
        extract_mobile_reward(mobile_reward, "unallocated-key")?.expect("valid reward share");
    assert_eq!(reward_key.key, PublicKeyBinary::from(vec![1]).to_string());
    assert_eq!(reward_key.reward_type, RewardType::MobileGateway);
    assert_eq!(amount, 1, "only dc_transfer_reward");

    Ok(())
}

#[test]
fn test_extract_iot_reward() -> anyhow::Result<()> {
    let iot_reward = IotRewardShare {
        start_period: Utc::now().timestamp_millis() as u64,
        end_period: Utc::now().timestamp_millis() as u64,
        reward: Some(iot_reward_share::Reward::GatewayReward(IotGatewayReward {
            hotspot_key: vec![1],
            beacon_amount: 1,
            witness_amount: 2,
            dc_transfer_amount: 3,
        })),
    };

    let (reward_key, amount) = extract_iot_reward(iot_reward, "op-fund-key", "unallocated-key")?
        .expect("valid reward share");
    assert_eq!(reward_key.key, PublicKeyBinary::from(vec![1]).to_string());
    assert_eq!(reward_key.reward_type, RewardType::IotGateway);
    assert_eq!(amount, 6, "all reward added together");

    Ok(())
}

#[test]
fn test_extract_reward_share() -> anyhow::Result<()> {
    let mobile_reward = MobileRewardShare {
        start_period: Utc::now().timestamp_millis() as u64,
        end_period: Utc::now().timestamp_millis() as u64,
        reward: Some(mobile_reward_share::Reward::GatewayReward(
            MobileGatewayReward {
                hotspot_key: vec![1],
                dc_transfer_reward: 1,
                rewardable_bytes: 2,
                price: 3,
            },
        )),
    };
    let iot_reward = IotRewardShare {
        start_period: Utc::now().timestamp_millis() as u64,
        end_period: Utc::now().timestamp_millis() as u64,
        reward: Some(iot_reward_share::Reward::GatewayReward(IotGatewayReward {
            hotspot_key: vec![1],
            beacon_amount: 1,
            witness_amount: 2,
            dc_transfer_amount: 3,
        })),
    };

    let key = PublicKeyBinary::from(vec![1]).to_string();

    let (mobile_reward_key, mobile_amount) =
        extract_reward_share(settings::Mode::Mobile, &mobile_reward.as_bytes(), "", "")?
            .expect("valid mobile share");

    assert_eq!(mobile_reward_key.key, key);
    assert_eq!(mobile_reward_key.reward_type, RewardType::MobileGateway);
    assert_eq!(mobile_amount, 1, "only dc_transfer_reward");

    let (iot_reward_key, iot_amount) =
        extract_reward_share(settings::Mode::Iot, &iot_reward.as_bytes(), "", "")?
            .expect("valid iot share");

    assert_eq!(iot_reward_key.key, key);
    assert_eq!(iot_reward_key.reward_type, RewardType::IotGateway);
    assert_eq!(iot_amount, 6, "all reward added together");

    Ok(())
}
