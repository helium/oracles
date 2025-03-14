use chrono::{DateTime, Duration, Utc};
use file_store::Stream;
use helium_crypto::PublicKeyBinary;
use helium_proto::{
    services::poc_mobile::{
        mobile_reward_share, GatewayReward, MobileRewardShare, ServiceProviderReward,
    },
    ServiceProvider,
};
use prost::bytes::BytesMut;
use reward_index::{
    db,
    indexer::{handle_escrowed_mobile_rewards, EscrowStats, RewardType},
};
use sqlx::PgPool;

use crate::common::{get_reward, mobile_rewards_stream};

async fn process_rewards(
    pool: &PgPool,
    rewards: Stream<BytesMut>,
    manifest_time: DateTime<Utc>,
    default_escrow_duration: Duration,
) -> anyhow::Result<EscrowStats> {
    let mut txn = pool.begin().await?;
    let stats = handle_escrowed_mobile_rewards(
        &mut txn,
        rewards,
        "unallocated-key",
        &manifest_time,
        default_escrow_duration.num_days() as u32,
    )
    .await?;
    txn.commit().await?;

    Ok(stats)
}

#[sqlx::test]
async fn escrow_duration_of_0_days(pool: PgPool) -> anyhow::Result<()> {
    // Rewards are processed and unlocked immediately

    let rewards = mobile_rewards_stream(vec![
        make_gateway_reward(vec![1], 1),
        make_gateway_reward(vec![1], 2),
        make_gateway_reward(vec![1], 3),
        make_gateway_reward(vec![2], 4),
    ]);

    process_rewards(&pool, rewards, Utc::now(), Duration::zero()).await?;

    let key_one = PublicKeyBinary::from(vec![1]);
    let reward_one = get_reward(&pool, &key_one.to_string(), RewardType::MobileGateway).await?;
    assert_eq!(reward_one.rewards, 6);

    let key_two = PublicKeyBinary::from(vec![2]);
    let reward_two = get_reward(&pool, &key_two.to_string(), RewardType::MobileGateway).await?;
    assert_eq!(reward_two.rewards, 4);

    Ok(())
}

#[sqlx::test]
async fn escrow_duration_of_1_day(pool: PgPool) -> anyhow::Result<()> {
    // Rewards are processed, and held up for 1 day

    let key_one = PublicKeyBinary::from(vec![1]);
    let key_two = PublicKeyBinary::from(vec![2]);

    let day_0_rewards = mobile_rewards_stream(vec![
        make_gateway_reward(&key_one, 1),
        make_gateway_reward(&key_one, 2),
        make_gateway_reward(&key_one, 3),
        make_gateway_reward(&key_two, 4),
    ]);
    // Extra locked reward weill cause test to fail if escrow period is not respected.
    let day_1_rewards = mobile_rewards_stream(vec![make_gateway_reward(vec![1], 1)]);

    let day_0 = Utc::now();
    let day_1 = day_0 + Duration::days(1);
    let escrow_duration = Duration::days(1);

    // No rewards are unlocked on day 0
    let stats = process_rewards(&pool, day_0_rewards, day_0, escrow_duration).await?;
    assert_eq!(stats.inserted, 2);
    assert_eq!(stats.unlocked, 0);

    let reward_one = get_reward(&pool, &key_one.to_string(), RewardType::MobileGateway).await;
    assert!(reward_one.is_err());

    let reward_two = get_reward(&pool, &key_two.to_string(), RewardType::MobileGateway).await;
    assert!(reward_two.is_err());

    // Process the next days worth of rewards to unlock day 0
    let stats = process_rewards(&pool, day_1_rewards, day_1, escrow_duration).await?;
    assert_eq!(stats.inserted, 1);
    assert_eq!(stats.unlocked, 2);

    let reward_one = get_reward(&pool, &key_one.to_string(), RewardType::MobileGateway).await?;
    assert_eq!(reward_one.rewards, 6);

    let reward_two = get_reward(&pool, &key_two.to_string(), RewardType::MobileGateway).await?;
    assert_eq!(reward_two.rewards, 4);

    Ok(())
}

#[sqlx::test]
async fn unlocked_rewards_cannot_be_unlocked_again(pool: PgPool) -> anyhow::Result<()> {
    // Add 5 days worth of rewards for different gateways with a 30 day escrow so they don't unlock.
    for day in 1..=5 {
        // Use a different key so we can track unlocking without worrying about
        // unlocked amounts combining because of matching keys.
        let one_reward = mobile_rewards_stream(vec![make_gateway_reward(vec![day as u8], 1)]);
        let escrow = Duration::days(30);
        let manifest_time = Utc::now() + Duration::days(day);

        let stats = process_rewards(&pool, one_reward, manifest_time, escrow).await?;
        assert_eq!(stats.inserted, 1);
        assert_eq!(stats.unlocked, 0);
    }

    // Sliding the window backwards
    // [  1  1  1  1  1  empty... ] :: Sanity check
    // [  1  1  1  1  1 ]           :: nothing unlocks
    // [  1  1  1] 1  1             :: 2 days unlock
    // [  1] 1  1  1  1             :: 2 more days unlock
    // [] 1  1  1  1  1             :: final day unlocks
    let expected = vec![
        (Duration::days(30), 0),
        (Duration::days(5), 0),
        (Duration::days(3), 2),
        (Duration::days(1), 2),
        (Duration::days(0), 1),
    ];

    let manifest_time = Utc::now() + Duration::days(5);

    for (escrow_duration, expected_unlocked) in expected {
        let stats = process_rewards(
            &pool,
            mobile_rewards_stream(vec![]),
            manifest_time,
            escrow_duration,
        )
        .await?;
        assert_eq!(stats.unlocked, expected_unlocked);
    }

    Ok(())
}

#[sqlx::test]
async fn use_address_escrow_duration_override(pool: PgPool) -> anyhow::Result<()> {
    let key_one = PublicKeyBinary::from(vec![1]);
    let key_two = PublicKeyBinary::from(vec![2]);

    let rewards = mobile_rewards_stream(vec![
        make_gateway_reward(&key_one, 1),
        make_gateway_reward(&key_two, 2),
    ]);

    let default_duration = Duration::zero().num_days() as u32;
    db::insert_escrow_duration(&pool, &key_two.to_string(), default_duration).await?;

    let stats = process_rewards(&pool, rewards, Utc::now(), Duration::days(30)).await?;
    assert_eq!(stats.inserted, 2, "inserted");
    assert_eq!(stats.unlocked, 1, "unlocked");

    let reward_one = get_reward(&pool, &key_one.to_string(), RewardType::MobileGateway).await;
    assert!(reward_one.is_err());

    let reward_two = get_reward(&pool, &key_two.to_string(), RewardType::MobileGateway).await?;
    assert_eq!(reward_two.rewards, 2);

    Ok(())
}

#[sqlx::test]
async fn only_mobile_gateway_rewards_are_escrowed(pool: PgPool) -> anyhow::Result<()> {
    let mobile_key = PublicKeyBinary::from(vec![1]);

    let rewards = mobile_rewards_stream(vec![
        make_gateway_reward(&mobile_key, 99),
        make_service_provider_reward(1),
    ]);

    let stats = process_rewards(&pool, rewards, Utc::now(), Duration::days(30)).await?;
    assert_eq!(stats.inserted, 2);

    let mobile_reward = get_reward(&pool, &mobile_key.to_string(), RewardType::MobileGateway).await;
    assert!(mobile_reward.is_err());

    let sp_key = ServiceProvider::HeliumMobile.to_string();
    let sp_reward = get_reward(&pool, &sp_key, RewardType::MobileServiceProvider).await?;
    assert_eq!(sp_reward.rewards, 1);

    Ok(())
}

fn make_gateway_reward(
    hotspot_key: impl AsRef<[u8]>,
    dc_transfer_reward: u64,
) -> MobileRewardShare {
    MobileRewardShare {
        start_period: Utc::now().timestamp_millis() as u64,
        end_period: Utc::now().timestamp_millis() as u64,
        reward: Some(mobile_reward_share::Reward::GatewayReward(GatewayReward {
            hotspot_key: hotspot_key.as_ref().to_vec(),
            dc_transfer_reward,
            rewardable_bytes: 0,
            price: 0,
        })),
    }
}

fn make_service_provider_reward(amount: u64) -> MobileRewardShare {
    MobileRewardShare {
        start_period: Utc::now().timestamp_millis() as u64,
        end_period: Utc::now().timestamp_millis() as u64,
        reward: Some(mobile_reward_share::Reward::ServiceProviderReward(
            ServiceProviderReward {
                service_provider_id: ServiceProvider::HeliumMobile.into(),
                amount,
            },
        )),
    }
}
