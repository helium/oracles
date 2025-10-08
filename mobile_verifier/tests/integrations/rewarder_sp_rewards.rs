use helium_proto::{services::poc_mobile::UnallocatedRewardType, ServiceProvider};
use rust_decimal::prelude::*;
use rust_decimal_macros::dec;
use sqlx::PgPool;

use crate::common::{self, reward_info_24_hours};
use mobile_verifier::{reward_shares, rewarder};

#[sqlx::test]
async fn test_service_provider_rewards(_pool: PgPool) -> anyhow::Result<()> {
    let (mobile_rewards_client, mobile_rewards) = common::create_file_sink();

    let reward_info = reward_info_24_hours();

    rewarder::reward_service_providers(mobile_rewards_client, &reward_info).await?;

    let rewards = mobile_rewards.finish().await?;

    // Verify single ServiceProviderReward with full 24% allocation
    assert_eq!(rewards.sp_rewards.len(), 1);
    let sp_reward = rewards.sp_rewards.first().expect("sp reward");
    assert_eq!(
        sp_reward.service_provider_id,
        ServiceProvider::HeliumMobile as i32
    );

    // confirm the total rewards allocated matches expectations
    let expected_sum =
        reward_shares::get_scheduled_tokens_for_service_providers(reward_info.epoch_emissions)
            .to_u64()
            .unwrap();
    assert_eq!(expected_sum, sp_reward.amount);

    // confirm the rewarded percentage amount matches expectations
    let percent = (Decimal::from(sp_reward.amount) / reward_info.epoch_emissions)
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
