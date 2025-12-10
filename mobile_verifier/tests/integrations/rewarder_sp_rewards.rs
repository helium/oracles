use crate::common::{self, reward_info_24_hours};
use file_store_oracles::service_provider_reward_type::ServiceProviderRewardType;
use helium_proto::{services::poc_mobile::UnallocatedRewardType, ServiceProvider};
use mobile_verifier::{reward_shares, rewarder};
use rust_decimal::prelude::*;
use rust_decimal_macros::dec;
use sqlx::PgPool;

#[sqlx::test]
async fn test_service_provider_rewards(_pool: PgPool) -> anyhow::Result<()> {
    let (mobile_rewards_client, mobile_rewards) = common::create_file_sink();

    let reward_info = reward_info_24_hours();

    rewarder::reward_service_providers(mobile_rewards_client, &reward_info).await?;

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
        subscriber_reward.service_provider_reward_type,
        ServiceProviderRewardType::Subscriber.to_string()
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
    assert_eq!(
        network_reward.service_provider_reward_type,
        ServiceProviderRewardType::Network.to_string()
    );

    // confirm the total rewards allocated matches expectations
    let expected_sum =
        reward_shares::get_scheduled_tokens_for_service_providers(reward_info.epoch_emissions)
            .to_u64()
            .unwrap();
    assert_eq!(
        expected_sum - reward_shares::HELIUM_MOBILE_SERVICE_REWARD_BONES,
        network_reward.amount
    );

    // confirm the rewarded percentage amount matches expectations
    let percent =
        (Decimal::from(network_reward.amount + subscriber_reward.amount)
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
