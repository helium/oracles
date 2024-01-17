mod common;
use chrono::{Duration as ChronoDuration, Utc};
use iot_verifier::{reward_share, rewarder};
use rust_decimal::{prelude::ToPrimitive, Decimal, RoundingStrategy};
use rust_decimal_macros::dec;
use sqlx::PgPool;

#[sqlx::test]
async fn test_oracles(_pool: PgPool) -> anyhow::Result<()> {
    let (iot_rewards_client, mut iot_rewards) = common::create_file_sink();
    let now = Utc::now();
    let epoch = (now - ChronoDuration::hours(24))..now;
    tokio::select!(
        _ = rewarder::reward_oracles(&iot_rewards_client, &epoch) => {},
        // oracles rewards are 100% unallocated atm
        unallocated_oracle_reward = iot_rewards.receive_unallocated_reward()  =>
            {
                println!("unallocated oracles reward {:?}", unallocated_oracle_reward);
                // confirm the total rewards matches expectations
                let expected_total = reward_share::get_scheduled_oracle_tokens(epoch.end - epoch.start)
                    .to_u64()
                    .unwrap();
                assert_eq!(unallocated_oracle_reward.amount, 6_215_846_994_535);
                assert_eq!(unallocated_oracle_reward.amount, expected_total);

                // confirm the ops percentage amount matches expectations
                let daily_total = *reward_share::REWARDS_PER_DAY;
                let oracle_percent = (Decimal::from(unallocated_oracle_reward.amount) / daily_total).round_dp_with_strategy(2, RoundingStrategy::MidpointNearestEven);
                assert_eq!(oracle_percent, dec!(0.07));

                // should be no further msgs
                iot_rewards.assert_no_messages();
            },
    );
    Ok(())
}
