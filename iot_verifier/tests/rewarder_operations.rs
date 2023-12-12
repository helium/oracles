mod common;
use chrono::{Duration as ChronoDuration, Utc};
use iot_verifier::{reward_share, rewarder};
use rust_decimal::prelude::ToPrimitive;
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
                assert_eq!(ops_reward.amount, 6215846994535);
                assert_eq!(ops_reward.amount, expected_total);

                // should be no further msgs
                iot_rewards.assert_no_messages();
            },
    );
    Ok(())
}
