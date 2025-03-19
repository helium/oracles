use chrono::{DateTime, Duration, Utc};
use file_store::Stream;
use helium_crypto::PublicKeyBinary;
use helium_proto::{
    services::poc_mobile::{
        mobile_reward_share, GatewayReward, MobileRewardShare, PromotionReward, RadioRewardV2,
        ServiceProviderReward, SubscriberReward, UnallocatedReward, UnallocatedRewardType,
    },
    ServiceProvider,
};
use prost::bytes::BytesMut;
use reward_index::indexer::{handle_mobile_rewards, RewardType};
use sqlx::PgPool;

use crate::common::{get_reward, mobile_rewards_stream, nanos_trunc};

async fn process_rewards(
    pool: &PgPool,
    rewards: Stream<BytesMut>,
    manifest_time: DateTime<Utc>,
) -> anyhow::Result<()> {
    let mut txn = pool.begin().await?;

    handle_mobile_rewards(&mut txn, rewards, &manifest_time, "unallocated-key").await?;

    txn.commit().await?;

    Ok(())
}

#[sqlx::test]
async fn accumulates_rewards(pool: PgPool) -> anyhow::Result<()> {
    fn make_gateway_reward(hotspot_key: Vec<u8>, dc_transfer_reward: u64) -> MobileRewardShare {
        MobileRewardShare {
            start_period: Utc::now().timestamp_millis() as u64,
            end_period: Utc::now().timestamp_millis() as u64,
            reward: Some(mobile_reward_share::Reward::GatewayReward(GatewayReward {
                hotspot_key,
                dc_transfer_reward,
                rewardable_bytes: 0,
                price: 0,
            })),
        }
    }

    let rewards = mobile_rewards_stream(vec![
        make_gateway_reward(vec![1], 1),
        make_gateway_reward(vec![1], 2),
        make_gateway_reward(vec![1], 3),
        make_gateway_reward(vec![2], 4),
    ]);

    process_rewards(&pool, rewards, Utc::now()).await?;

    let key_one = PublicKeyBinary::from(vec![1]);
    let reward_one = get_reward(&pool, &key_one.to_string(), RewardType::MobileGateway).await?;
    assert_eq!(reward_one.rewards, 6);
    assert_eq!(reward_one.claimable, 6);

    let key_two = PublicKeyBinary::from(vec![2]);
    let reward_two = get_reward(&pool, &key_two.to_string(), RewardType::MobileGateway).await?;
    assert_eq!(reward_two.rewards, 4);
    assert_eq!(reward_two.claimable, 4);

    Ok(())
}

#[sqlx::test]
async fn zero_rewards_do_not_update_db_timestamp(pool: PgPool) -> anyhow::Result<()> {
    fn make_gateway_reward(hotspot_key: Vec<u8>, dc_transfer_reward: u64) -> MobileRewardShare {
        MobileRewardShare {
            start_period: Utc::now().timestamp_millis() as u64,
            end_period: Utc::now().timestamp_millis() as u64,
            reward: Some(mobile_reward_share::Reward::GatewayReward(GatewayReward {
                hotspot_key,
                dc_transfer_reward,
                rewardable_bytes: 0,
                price: 0,
            })),
        }
    }

    let rewards = mobile_rewards_stream(vec![make_gateway_reward(vec![1], 1)]);
    let before_manifest_time = Utc::now() - Duration::days(2);
    process_rewards(&pool, rewards, before_manifest_time).await?;

    let key = PublicKeyBinary::from(vec![1]).to_string();
    let reward = get_reward(&pool, &key, RewardType::MobileGateway).await?;
    assert_eq!(reward.rewards, 1);
    assert_eq!(reward.claimable, 1);
    assert_eq!(reward.last_reward, nanos_trunc(before_manifest_time));

    // Zeroed reward should have no effect
    let rewards = mobile_rewards_stream(vec![make_gateway_reward(vec![1], 0)]);
    let now_manifest_time = Utc::now();
    process_rewards(&pool, rewards, now_manifest_time).await?;

    let key = PublicKeyBinary::from(vec![1]).to_string();
    let reward = get_reward(&pool, &key, RewardType::MobileGateway).await?;
    assert_eq!(reward.rewards, 1);
    assert_eq!(reward.claimable, 1);
    assert_eq!(reward.last_reward, nanos_trunc(before_manifest_time));

    Ok(())
}

#[sqlx::test]
async fn radio_reward_v1_is_ignored(pool: PgPool) -> anyhow::Result<()> {
    let rewards = mobile_rewards_stream(vec![
        MobileRewardShare {
            start_period: Utc::now().timestamp_millis() as u64,
            end_period: Utc::now().timestamp_millis() as u64,
            reward: Some(mobile_reward_share::Reward::RadioReward(
                #[allow(deprecated)]
                helium_proto::services::poc_mobile::RadioReward {
                    hotspot_key: vec![1],
                    dc_transfer_reward: 1,
                    poc_reward: 2,
                    coverage_points: 3,
                    ..Default::default()
                },
            )),
        },
        MobileRewardShare {
            start_period: Utc::now().timestamp_millis() as u64,
            end_period: Utc::now().timestamp_millis() as u64,
            reward: Some(mobile_reward_share::Reward::RadioRewardV2(RadioRewardV2 {
                hotspot_key: vec![1],
                base_poc_reward: 4,
                boosted_poc_reward: 5,
                ..Default::default()
            })),
        },
    ]);

    process_rewards(&pool, rewards, Utc::now()).await?;

    let key = PublicKeyBinary::from(vec![1]);
    let reward = get_reward(&pool, &key.to_string(), RewardType::MobileGateway).await?;
    assert_eq!(reward.rewards, 9);
    assert_eq!(reward.claimable, 9);

    Ok(())
}

#[sqlx::test]
async fn subscriber_reward(pool: PgPool) -> anyhow::Result<()> {
    let rewards = mobile_rewards_stream(vec![MobileRewardShare {
        start_period: Utc::now().timestamp_millis() as u64,
        end_period: Utc::now().timestamp_millis() as u64,
        reward: Some(mobile_reward_share::Reward::SubscriberReward(
            SubscriberReward {
                subscriber_id: vec![1],
                discovery_location_amount: 1,
                verification_mapping_amount: 2,
            },
        )),
    }]);

    process_rewards(&pool, rewards, Utc::now()).await?;

    let reward = get_reward(
        &pool,
        &bs58::encode(vec![1]).into_string(),
        RewardType::MobileSubscriber,
    )
    .await?;
    assert_eq!(reward.rewards, 3);
    assert_eq!(reward.claimable, 3);

    Ok(())
}

#[sqlx::test]
async fn service_provider_reward(pool: PgPool) -> anyhow::Result<()> {
    let rewards = mobile_rewards_stream(vec![MobileRewardShare {
        start_period: Utc::now().timestamp_millis() as u64,
        end_period: Utc::now().timestamp_millis() as u64,
        reward: Some(mobile_reward_share::Reward::ServiceProviderReward(
            ServiceProviderReward {
                service_provider_id: ServiceProvider::HeliumMobile.into(),
                amount: 1,
            },
        )),
    }]);

    process_rewards(&pool, rewards, Utc::now()).await?;

    let reward = get_reward(
        &pool,
        &ServiceProvider::HeliumMobile.to_string(),
        RewardType::MobileServiceProvider,
    )
    .await?;
    assert_eq!(reward.rewards, 1);
    assert_eq!(reward.claimable, 1);

    Ok(())
}

#[sqlx::test]
async fn fails_on_unknown_service_provider(pool: PgPool) -> anyhow::Result<()> {
    let rewards = mobile_rewards_stream(vec![MobileRewardShare {
        start_period: Utc::now().timestamp_millis() as u64,
        end_period: Utc::now().timestamp_millis() as u64,
        reward: Some(mobile_reward_share::Reward::ServiceProviderReward(
            ServiceProviderReward {
                service_provider_id: 999,
                amount: 1,
            },
        )),
    }]);

    let res = process_rewards(&pool, rewards, Utc::now()).await;
    assert!(res.is_err());

    Ok(())
}

#[sqlx::test]
async fn unallocated_rewards_are_combined(pool: PgPool) -> anyhow::Result<()> {
    fn make_unallocated_reward(
        amount: u64,
        reward_type: UnallocatedRewardType,
    ) -> MobileRewardShare {
        MobileRewardShare {
            start_period: Utc::now().timestamp_millis() as u64,
            end_period: Utc::now().timestamp_millis() as u64,
            reward: Some(mobile_reward_share::Reward::UnallocatedReward(
                UnallocatedReward {
                    reward_type: reward_type.into(),
                    amount,
                },
            )),
        }
    }

    let rewards = mobile_rewards_stream(vec![
        make_unallocated_reward(1, UnallocatedRewardType::Poc),
        make_unallocated_reward(2, UnallocatedRewardType::DiscoveryLocation),
        make_unallocated_reward(3, UnallocatedRewardType::Mapper),
        make_unallocated_reward(4, UnallocatedRewardType::ServiceProvider),
        make_unallocated_reward(5, UnallocatedRewardType::Oracle),
        make_unallocated_reward(6, UnallocatedRewardType::Data),
    ]);

    process_rewards(&pool, rewards, Utc::now()).await?;

    let reward = get_reward(&pool, "unallocated-key", RewardType::MobileUnallocated).await?;
    assert_eq!(reward.rewards, 21);
    assert_eq!(reward.claimable, 21);

    Ok(())
}

#[sqlx::test]
async fn promotion_reward(pool: PgPool) -> anyhow::Result<()> {
    let rewards = mobile_rewards_stream(vec![MobileRewardShare {
        start_period: Utc::now().timestamp_millis() as u64,
        end_period: Utc::now().timestamp_millis() as u64,
        reward: Some(mobile_reward_share::Reward::PromotionReward(
            PromotionReward {
                entity: PublicKeyBinary::from(vec![1]).to_string(),
                service_provider_amount: 2,
                matched_amount: 1,
            },
        )),
    }]);

    process_rewards(&pool, rewards, Utc::now()).await?;

    let key = PublicKeyBinary::from(vec![1]).to_string();
    let reward = get_reward(&pool, &key, RewardType::MobilePromotion).await?;
    assert_eq!(reward.rewards, 3);
    assert_eq!(reward.claimable, 3);

    Ok(())
}
