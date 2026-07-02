use crate::common::{self, reward_info_24_hours};
use helium_proto::{services::poc_mobile::UnallocatedRewardType, ServiceProvider};
use mobile_verifier::reward_shares::RewardableEntityKey;
use mobile_verifier::{reward_shares, rewarder};
use rust_decimal::prelude::*;
use rust_decimal_macros::dec;
use sqlx::PgPool;

#[sqlx::test]
async fn test_service_provider_rewards(_pool: PgPool) -> anyhow::Result<()> {
    let (mobile_rewards_client, mobile_rewards) = common::create_file_sink();

    let reward_info = reward_info_24_hours();

    rewarder::reward_service_providers(mobile_rewards_client, &reward_info, None).await?;

    let rewards = mobile_rewards.finish().await?;

    // Verify two ServiceProviderRewards
    assert_eq!(rewards.sp_rewards.len(), 2);

    // Verify first reward is 450HNT to HeliumMobile Subscriber wallet
    let subscriber_reward = rewards.sp_rewards.first().expect("sp reward");
    assert_eq!(
        subscriber_reward.service_provider_id,
        ServiceProvider::HeliumMobile as i32
    );
    assert_eq!(
        subscriber_reward.rewardable_entity_key,
        "Helium Mobile Service Rewards"
    );
    assert_eq!(
        reward_shares::HELIUM_MOBILE_SERVICE_REWARD_BONES,
        subscriber_reward.amount
    );

    // Verify second reward is to HeliumMobile Network wallet with remaining amount
    let network_reward = rewards.sp_rewards.get(1).expect("sp reward");
    assert_eq!(
        network_reward.service_provider_id,
        ServiceProvider::HeliumMobile as i32
    );
    assert_eq!(network_reward.rewardable_entity_key, "Helium Mobile");

    // confirm the total rewards allocated matches expectations
    let expected_sum = reward_shares::hip_149_reward_pools(&reward_info).service_provider;
    assert_eq!(
        expected_sum - reward_shares::HELIUM_MOBILE_SERVICE_REWARD_BONES,
        network_reward.amount
    );

    // confirm the rewarded percentage amount matches expectations
    let percent = (Decimal::from(network_reward.amount + subscriber_reward.amount)
        / reward_info.epoch_emissions)
        .round_dp_with_strategy(2, RoundingStrategy::MidpointNearestEven);
    assert_eq!(percent, dec!(0.24));

    // Verify no unallocated service provider rewards
    assert_eq!(
        rewards
            .unallocated
            .iter()
            .filter(|r| r.reward_type == UnallocatedRewardType::ServiceProvider as i32)
            .count(),
        0
    );

    Ok(())
}

#[sqlx::test]
async fn should_not_reward_service_provider_negative_amount(_pool: PgPool) -> anyhow::Result<()> {
    let (mobile_rewards_client, mobile_rewards) = common::create_file_sink();

    let mut reward_info = reward_info_24_hours();
    // Total reward amount of 350 HNT (6% delegation carved out on-chain).
    reward_info.epoch_emissions = Decimal::from(35_000_000_000u64);
    reward_info.hnt_rewards_issued = Decimal::from(35_000_000_000u64 - 35_000_000_000u64 * 6 / 100);
    reward_info.delegation_rewards_issued = Decimal::from(35_000_000_000u64 * 6 / 100);

    rewarder::reward_service_providers(mobile_rewards_client, &reward_info, None).await?;

    let rewards = mobile_rewards.finish().await?;

    // Verify two ServiceProviderRewards
    assert_eq!(rewards.sp_rewards.len(), 2);

    // Subscriber reward should be clamped to 84 HNT (24% of 350HNT)
    let subscriber_reward = rewards.sp_rewards.first().expect("sp reward");
    assert_eq!(
        subscriber_reward.service_provider_id,
        ServiceProvider::HeliumMobile as i32
    );
    assert_eq!(
        subscriber_reward.rewardable_entity_key,
        RewardableEntityKey::Subscriber.to_string()
    );
    assert_eq!(8_400_000_000, subscriber_reward.amount);

    // Network reward should be 0 as Subscriber wallet received the full reward amount
    let network_reward = rewards.sp_rewards.get(1).expect("sp reward");
    assert_eq!(
        network_reward.service_provider_id,
        ServiceProvider::HeliumMobile as i32
    );
    assert_eq!(
        network_reward.rewardable_entity_key,
        RewardableEntityKey::Network.to_string()
    );
    assert_eq!(0, network_reward.amount);

    Ok(())
}

/// HIP-149: the service-provider pool is a flat 24% of *total* emissions, so the
/// 3× cap / backstop — which shifts HNT between issued and delegation — must leave
/// it untouched. Run a capped and a backstopped epoch at the same emissions and
/// confirm the emitted SP reward is identical. Complements the data-transfer
/// cap/backstop tests, which show the data pool absorbing the whole shift.
#[sqlx::test]
async fn test_service_provider_flat_across_cap_and_backstop(_pool: PgPool) -> anyhow::Result<()> {
    // 1e12 emissions keeps the 24% cut (240e9) clear of the 45e9 subscriber floor.
    const EMISSIONS: u64 = 1_000_000_000_000;

    async fn sp_total(hnt_issued: u64, delegation: u64) -> anyhow::Result<u64> {
        let (client, sink) = common::create_file_sink();
        let mut reward_info = reward_info_24_hours();
        reward_info.epoch_emissions = Decimal::from(hnt_issued + delegation);
        reward_info.hnt_rewards_issued = Decimal::from(hnt_issued);
        reward_info.delegation_rewards_issued = Decimal::from(delegation);

        rewarder::reward_service_providers(client, &reward_info, None).await?;
        let rewards = sink.finish().await?;
        Ok(rewards.sp_rewards.iter().map(|r| r.amount).sum())
    }

    // Cap: issued 80%, delegation 20%. Backstop: issued 98%, delegation 2%.
    let capped = sp_total(EMISSIONS * 80 / 100, EMISSIONS * 20 / 100).await?;
    let backstopped = sp_total(EMISSIONS * 98 / 100, EMISSIONS * 2 / 100).await?;

    assert_eq!(
        capped, backstopped,
        "SP pool must not move with the cap/backstop"
    );
    // Independent of `hip_149_reward_pools`: a flat 24% of total emissions.
    assert_eq!(capped, 240_000_000_000);

    Ok(())
}
