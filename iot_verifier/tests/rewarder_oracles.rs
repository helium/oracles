mod common;
use crate::common::MockFileSinkReceiver;
use chrono::{Duration as ChronoDuration, Utc};
use helium_proto::services::poc_lora::UnallocatedReward;
use iot_verifier::{reward_share, rewarder};
use rust_decimal::{prelude::ToPrimitive, Decimal, RoundingStrategy};
use rust_decimal_macros::dec;
use sqlx::PgPool;

#[sqlx::test]
async fn test_oracles(_pool: PgPool) -> anyhow::Result<()> {
    let (iot_rewards_client, mut iot_rewards) = common::create_file_sink();
    let now = Utc::now();
    let epoch = (now - ChronoDuration::hours(24))..now;
    let (_, rewards) = tokio::join!(
        rewarder::reward_oracles(&iot_rewards_client, &epoch),
        receive_expected_rewards(&mut iot_rewards)
    );
    if let Ok(unallocated_oracle_reward) = rewards {
        // confirm the total rewards matches expectations
        let expected_total = reward_share::get_scheduled_oracle_tokens(epoch.end - epoch.start)
            .to_u64()
            .unwrap();
        assert_eq!(unallocated_oracle_reward.amount, 6_215_846_994_535);
        assert_eq!(unallocated_oracle_reward.amount, expected_total);

        // confirm the ops percentage amount matches expectations
        let daily_total = *reward_share::REWARDS_PER_DAY;
        let oracle_percent = (Decimal::from(unallocated_oracle_reward.amount) / daily_total)
            .round_dp_with_strategy(2, RoundingStrategy::MidpointNearestEven);
        assert_eq!(oracle_percent, dec!(0.07));
    } else {
        panic!("no rewards received");
    };
    Ok(())
}

async fn receive_expected_rewards(
    iot_rewards: &mut MockFileSinkReceiver,
) -> anyhow::Result<UnallocatedReward> {
    // expect one unallocated reward
    // as oracle rewards are currently 100% unallocated
    let reward = iot_rewards.receive_unallocated_reward().await;

    // should be no further msgs
    iot_rewards.assert_no_messages();

    Ok(reward)
}
