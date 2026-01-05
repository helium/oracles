use crate::common::{
    create_file_sink, default_price_info, reward_info_24_hours,
    seed::{seed_data_sessions, seed_heartbeats, seed_speedtests, update_assignments},
    MockHexBoostingClient, RadioRewardV2Ext,
};
use mobile_verifier::reward_shares::{
    MAX_DATA_TRANSFER_REWARDS_PERCENT, POC_REWARDS_PERCENT, SERVICE_PROVIDER_PERCENT,
};
use mobile_verifier::rewarder;
use rust_decimal::{prelude::ToPrimitive, Decimal};
use sqlx::PgPool;

#[sqlx::test]
async fn test_distribute_rewards(pool: PgPool) -> anyhow::Result<()> {
    let (mobile_rewards_client, mobile_rewards) = create_file_sink();
    let reward_info = reward_info_24_hours();

    // seed all the things
    let mut txn = pool.clone().begin().await?;
    seed_heartbeats(reward_info.epoch_period.start, &mut txn).await?;
    seed_speedtests(reward_info.epoch_period.end, &mut txn).await?;
    seed_data_sessions(reward_info.epoch_period.start, &mut txn).await?;
    txn.commit().await?;
    update_assignments(&pool).await?;

    let hex_boosting_client = MockHexBoostingClient::new(vec![]);
    let price_info = default_price_info();

    // Run rewards
    rewarder::distribute_rewards(
        &pool,
        &hex_boosting_client,
        mobile_rewards_client,
        &reward_info,
        price_info,
    )
    .await?;

    // Retrieve distributed rewards
    let rewards = mobile_rewards.finish().await?;
    let poc_rewards = rewards.radio_reward_v2s;
    let dc_rewards = rewards.gateway_rewards;
    let sp_rewards = rewards.sp_rewards;
    let unallocated_rewards = rewards.unallocated;

    let poc_sum: u64 = poc_rewards.iter().map(|r| r.total_poc_reward()).sum();
    let dc_sum: u64 = dc_rewards.iter().map(|r| r.dc_transfer_reward).sum();
    let sp_sum: u64 = sp_rewards.iter().map(|r| r.amount).sum();
    let unallocated_sum: u64 = unallocated_rewards.iter().map(|r| r.amount).sum();

    let total: u64 = poc_sum + dc_sum + sp_sum + unallocated_sum;

    // Calculate expected rewards
    let expected_total = calculate_expected_total_rewards(reward_info.epoch_emissions);

    // Assert total
    assert_eq!(total, expected_total);

    Ok(())
}

fn calculate_expected_total_rewards(total_emission_pool: Decimal) -> u64 {
    (total_emission_pool
        * (SERVICE_PROVIDER_PERCENT + POC_REWARDS_PERCENT + MAX_DATA_TRANSFER_REWARDS_PERCENT))
        .to_u64()
        .unwrap()
}
