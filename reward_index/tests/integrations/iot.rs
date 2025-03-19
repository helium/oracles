use chrono::{DateTime, Duration, Utc};
use file_store::Stream;
use helium_crypto::PublicKeyBinary;
use prost::bytes::BytesMut;
use reward_index::indexer::{handle_iot_rewards, RewardType};
use sqlx::PgPool;

use helium_proto::services::poc_lora::{
    iot_reward_share, GatewayReward, IotRewardShare, OperationalReward, UnallocatedReward,
    UnallocatedRewardType,
};

use crate::common::{get_reward, iot_rewards_stream, nanos_trunc};

async fn process_rewards(
    pool: &PgPool,
    rewards: Stream<BytesMut>,
    manifest_time: DateTime<Utc>,
) -> anyhow::Result<()> {
    let mut txn = pool.begin().await?;

    handle_iot_rewards(
        &mut txn,
        rewards,
        &manifest_time,
        "op-fund",
        "unallocated-key",
    )
    .await?;

    txn.commit().await?;

    Ok(())
}

#[sqlx::test]
async fn gateway_rewards_accumulate_by_key(pool: PgPool) -> anyhow::Result<()> {
    fn make_gateway_reward(
        hotspot_key: Vec<u8>,
        beacon_amount: u64,
        witness_amount: u64,
        dc_transfer_amount: u64,
    ) -> IotRewardShare {
        IotRewardShare {
            start_period: Utc::now().timestamp_millis() as u64,
            end_period: Utc::now().timestamp_millis() as u64,
            reward: Some(iot_reward_share::Reward::GatewayReward(GatewayReward {
                hotspot_key,
                beacon_amount,
                witness_amount,
                dc_transfer_amount,
            })),
        }
    }

    let rewards = iot_rewards_stream(vec![
        make_gateway_reward(vec![1], 1, 2, 3),
        make_gateway_reward(vec![1], 4, 5, 6),
        make_gateway_reward(vec![1], 7, 8, 9),
        make_gateway_reward(vec![2], 10, 11, 12),
    ]);

    process_rewards(&pool, rewards, Utc::now()).await?;

    let key_one = PublicKeyBinary::from(vec![1]).to_string();
    let reward_one = get_reward(&pool, &key_one, RewardType::IotGateway).await?;
    assert_eq!(reward_one.rewards, 45);
    assert_eq!(reward_one.claimable, 45);

    let key_two = PublicKeyBinary::from(vec![2]).to_string();
    let reward_two = get_reward(&pool, &key_two, RewardType::IotGateway).await?;
    assert_eq!(reward_two.rewards, 33);
    assert_eq!(reward_two.claimable, 33);

    Ok(())
}

#[sqlx::test]
async fn zero_rewards_do_not_update_db_timestamp(pool: PgPool) -> anyhow::Result<()> {
    fn make_gateway_reward(
        hotspot_key: Vec<u8>,
        beacon_amount: u64,
        witness_amount: u64,
        dc_transfer_amount: u64,
    ) -> IotRewardShare {
        IotRewardShare {
            start_period: Utc::now().timestamp_millis() as u64,
            end_period: Utc::now().timestamp_millis() as u64,
            reward: Some(iot_reward_share::Reward::GatewayReward(GatewayReward {
                hotspot_key,
                beacon_amount,
                witness_amount,
                dc_transfer_amount,
            })),
        }
    }

    let rewards = iot_rewards_stream(vec![make_gateway_reward(vec![1], 1, 2, 3)]);
    let before_manifest_time = Utc::now() - Duration::days(2);
    process_rewards(&pool, rewards, before_manifest_time).await?;

    let key = PublicKeyBinary::from(vec![1]).to_string();
    let reward = get_reward(&pool, &key, RewardType::IotGateway).await?;
    assert_eq!(reward.rewards, 6);
    assert_eq!(reward.claimable, 6);
    assert_eq!(reward.last_reward, nanos_trunc(before_manifest_time));

    // Zeroed reward should have no effect
    let rewards = iot_rewards_stream(vec![make_gateway_reward(vec![1], 0, 0, 0)]);
    let now_manifest_time = Utc::now();
    process_rewards(&pool, rewards, now_manifest_time).await?;

    let key = PublicKeyBinary::from(vec![1]).to_string();
    let reward = get_reward(&pool, &key, RewardType::IotGateway).await?;
    assert_eq!(reward.rewards, 6);
    assert_eq!(reward.claimable, 6);
    assert_eq!(reward.last_reward, nanos_trunc(before_manifest_time));

    Ok(())
}

#[sqlx::test]
async fn unallocated_reward_types_are_combined(pool: PgPool) -> anyhow::Result<()> {
    fn make_unallocated_reward(amount: u64, reward_type: UnallocatedRewardType) -> IotRewardShare {
        IotRewardShare {
            start_period: Utc::now().timestamp_millis() as u64,
            end_period: Utc::now().timestamp_millis() as u64,
            reward: Some(iot_reward_share::Reward::UnallocatedReward(
                UnallocatedReward {
                    reward_type: reward_type.into(),
                    amount,
                },
            )),
        }
    }

    let rewards = iot_rewards_stream(vec![
        make_unallocated_reward(1, UnallocatedRewardType::Poc),
        make_unallocated_reward(2, UnallocatedRewardType::Operation),
        make_unallocated_reward(3, UnallocatedRewardType::Oracle),
        make_unallocated_reward(4, UnallocatedRewardType::Data),
    ]);

    process_rewards(&pool, rewards, Utc::now()).await?;

    let gateway_reward = get_reward(&pool, "unallocated-key", RewardType::IotUnallocated).await?;
    assert_eq!(gateway_reward.rewards, 10);
    assert_eq!(gateway_reward.claimable, 10);

    Ok(())
}

#[sqlx::test]
async fn operation_rewards_are_combined(pool: PgPool) -> anyhow::Result<()> {
    fn make_unallocated_reward(amount: u64) -> IotRewardShare {
        IotRewardShare {
            start_period: Utc::now().timestamp_millis() as u64,
            end_period: Utc::now().timestamp_millis() as u64,
            reward: Some(iot_reward_share::Reward::OperationalReward(
                OperationalReward { amount },
            )),
        }
    }

    let rewards = iot_rewards_stream(vec![
        make_unallocated_reward(1),
        make_unallocated_reward(2),
        make_unallocated_reward(3),
        make_unallocated_reward(4),
    ]);

    process_rewards(&pool, rewards, Utc::now()).await?;

    let gateway_reward = get_reward(&pool, "op-fund", RewardType::IotOperational).await?;
    assert_eq!(gateway_reward.rewards, 10);
    assert_eq!(gateway_reward.claimable, 10);

    Ok(())
}
