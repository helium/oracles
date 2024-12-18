use crate::common::{
    self, default_rewards_info, MockFileSinkReceiver, EMISSIONS_POOL_IN_BONES_24_HOURS,
};
use chrono::Duration;
use helium_proto::services::poc_lora::{IotRewardShare, OperationalReward};
use iot_verifier::{reward_share, rewarder};
use rust_decimal::{prelude::ToPrimitive, Decimal, RoundingStrategy};
use rust_decimal_macros::dec;

#[tokio::test]
async fn test_operations() -> anyhow::Result<()> {
    let (iot_rewards_client, mut iot_rewards) = common::create_file_sink();

    let reward_info = default_rewards_info(EMISSIONS_POOL_IN_BONES_24_HOURS, Duration::hours(24));

    let (_, rewards) = tokio::join!(
        rewarder::reward_operational(&iot_rewards_client, &reward_info),
        receive_expected_rewards(&mut iot_rewards)
    );
    if let Ok(ops_reward) = rewards {
        // confirm the total rewards allocated matches expectations
        let expected_total =
            reward_share::get_scheduled_ops_fund_tokens(reward_info.epoch_emissions)
                .to_u64()
                .unwrap();
        assert_eq!(ops_reward.amount, 6_232_876_712_328);
        assert_eq!(ops_reward.amount, expected_total);

        // confirm the ops percentage amount matches expectations
        let ops_percent = (Decimal::from(ops_reward.amount) / reward_info.epoch_emissions)
            .round_dp_with_strategy(2, RoundingStrategy::MidpointNearestEven);
        assert_eq!(ops_percent, dec!(0.07));
    } else {
        panic!("no rewards received");
    };
    Ok(())
}

async fn receive_expected_rewards(
    iot_rewards: &mut MockFileSinkReceiver<IotRewardShare>,
) -> anyhow::Result<OperationalReward> {
    // expect one operational reward msg
    let reward = iot_rewards.receive_operational_reward().await;

    // should be no further msgs
    iot_rewards.assert_no_messages();

    Ok(reward)
}
