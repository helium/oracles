use crate::common::{self, reward_info_24_hours};
use helium_proto::services::poc_mobile::UnallocatedRewardType;
use mobile_verifier::{reward_shares, rewarder};
use rust_decimal::prelude::*;
use rust_decimal_macros::dec;
use sqlx::PgPool;

#[sqlx::test]
async fn test_oracle_rewards(_pool: PgPool) -> anyhow::Result<()> {
    let (mobile_rewards_client, mobile_rewards) = common::create_nonblocking_file_sink();

    let reward_info = reward_info_24_hours();

    // run rewards for oracles
    rewarder::reward_oracles(mobile_rewards_client, &reward_info).await?;

    let rewards = mobile_rewards.finish().await?;
    let unallocated_reward = rewards.unallocated.first().expect("Unallocated");

    assert_eq!(
        UnallocatedRewardType::Oracle as i32,
        unallocated_reward.reward_type
    );
    // confirm our unallocated amount
    assert_eq!(3_287_671_232_876, unallocated_reward.amount);

    // confirm the total rewards allocated matches expectations
    let expected_sum = reward_shares::get_scheduled_tokens_for_oracles(reward_info.epoch_emissions)
        .to_u64()
        .unwrap();
    assert_eq!(expected_sum, unallocated_reward.amount);

    // confirm the rewarded percentage amount matches expectations
    let percent = (Decimal::from(unallocated_reward.amount) / reward_info.epoch_emissions)
        .round_dp_with_strategy(2, RoundingStrategy::MidpointNearestEven);
    assert_eq!(percent, dec!(0.04));

    Ok(())
}
