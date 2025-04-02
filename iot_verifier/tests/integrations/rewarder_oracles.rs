use crate::common::{self, rewards_info_24_hours, MockFileSinkReceiver};
use helium_proto::services::poc_lora::{IotRewardShare, UnallocatedReward};
use iot_verifier::{reward_share, rewarder};
use rust_decimal::{prelude::ToPrimitive, Decimal, RoundingStrategy};
use rust_decimal_macros::dec;
use sqlx::PgPool;

#[sqlx::test]
async fn test_oracles(_pool: PgPool) -> anyhow::Result<()> {
    let (iot_rewards_client, mut iot_rewards) = common::create_file_sink();

    let reward_info = rewards_info_24_hours();

    let (_, rewards) = tokio::join!(
        rewarder::reward_oracles(&iot_rewards_client, &reward_info),
        receive_expected_rewards(&mut iot_rewards)
    );
    if let Ok(unallocated_oracle_reward) = rewards {
        // confirm the total rewards matches expectations
        let expected_total = reward_share::get_scheduled_oracle_tokens(reward_info.epoch_emissions)
            .to_u64()
            .unwrap();
        assert_eq!(unallocated_oracle_reward.amount, 6_232_876_712_328);
        assert_eq!(unallocated_oracle_reward.amount, expected_total);

        // confirm the ops percentage amount matches expectations
        let oracle_percent = (Decimal::from(unallocated_oracle_reward.amount)
            / reward_info.epoch_emissions)
            .round_dp_with_strategy(2, RoundingStrategy::MidpointNearestEven);
        assert_eq!(oracle_percent, dec!(0.07));
    } else {
        panic!("no rewards received");
    };
    Ok(())
}

async fn receive_expected_rewards(
    iot_rewards: &mut MockFileSinkReceiver<IotRewardShare>,
) -> anyhow::Result<UnallocatedReward> {
    // expect one unallocated reward
    // as oracle rewards are currently 100% unallocated
    let reward = iot_rewards.receive_unallocated_reward().await;

    // should be no further msgs
    iot_rewards.assert_no_messages();

    Ok(reward)
}
