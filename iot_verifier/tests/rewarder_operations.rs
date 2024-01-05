mod common;
use chrono::{Duration as ChronoDuration, Utc};
use iot_verifier::{reward_share, rewarder};
use rust_decimal::{prelude::ToPrimitive, Decimal, RoundingStrategy};
use rust_decimal_macros::dec;
use sqlx::PgPool;

#[sqlx::test]
async fn test_operations(_pool: PgPool) -> anyhow::Result<()> {
    let (iot_rewards_client, mut iot_rewards) = common::create_file_sink();
    let now = Utc::now();
    let epoch = (now - ChronoDuration::hours(24))..now;
    tokio::select!(
        _ = rewarder::reward_operational(&iot_rewards_client, &epoch) => { println!("point 1")},
        ops_reward = iot_rewards.receive_operational_reward()  =>
            {
                println!("ops reward {:?}", ops_reward);
                // confirm the total rewards allocated matches expectations
                let expected_total = reward_share::get_scheduled_ops_fund_tokens(epoch.end - epoch.start)
                    .to_u64()
                    .unwrap();
                assert_eq!(ops_reward.amount, 6_215_846_994_535);
                assert_eq!(ops_reward.amount, expected_total);

                // confirm the ops percentage amount matches expectations
                let daily_total = *reward_share::REWARDS_PER_DAY;
                let ops_percent = (Decimal::from(ops_reward.amount) / daily_total).round_dp_with_strategy(2, RoundingStrategy::MidpointNearestEven);
                assert_eq!(ops_percent, dec!(0.07));

                // should be no further msgs
                iot_rewards.assert_no_messages();
            },
    );
    Ok(())
}
