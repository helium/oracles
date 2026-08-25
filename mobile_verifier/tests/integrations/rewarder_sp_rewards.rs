use crate::common::{self, reward_info_24_hours};
use helium_proto::{services::poc_mobile::UnallocatedRewardType, ServiceProvider};
use mobile_verifier::reward_shares::RewardableEntityKey;
use mobile_verifier::{reward_shares, rewarder};
use rust_decimal::prelude::*;
use rust_decimal_macros::dec;

#[tokio::test]
async fn test_service_provider_rewards() -> anyhow::Result<()> {
    let (mobile_rewards_client, mobile_rewards) = common::create_file_sink();

    let reward_info = reward_info_24_hours();

    let harness = common::setup_iceberg().await?;
    let reward_writers = common::reward_writers(&harness).await?;

    rewarder::reward_service_providers(
        mobile_rewards_client,
        &reward_info,
        &reward_writers,
        "test-epoch",
    )
    .await?;

    let rewards = mobile_rewards.finish().await?;

    // The entire service-provider pool goes to the HeliumMobile Network wallet.
    assert_eq!(rewards.sp_rewards.len(), 1);

    let network_reward = rewards.sp_rewards.first().expect("sp reward");
    assert_eq!(
        network_reward.service_provider_id,
        ServiceProvider::HeliumMobile as i32
    );
    assert_eq!(
        network_reward.rewardable_entity_key,
        RewardableEntityKey::Network.to_string()
    );

    // confirm the total rewards allocated matches the full 24% pool
    let expected_sum = reward_shares::hip_149_reward_pools(&reward_info).service_provider;
    assert_eq!(expected_sum, network_reward.amount);

    // confirm the rewarded percentage amount matches expectations
    let percent = (Decimal::from(network_reward.amount) / reward_info.epoch_emissions)
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

/// HIP-149: the service-provider pool is a flat 24% of *total* emissions, so the
/// 3× cap / backstop — which shifts HNT between issued and delegation — must leave
/// it untouched. Run a capped and a backstopped epoch at the same emissions and
/// confirm the emitted SP reward is identical. Complements the data-transfer
/// cap/backstop tests, which show the data pool absorbing the whole shift.
#[tokio::test]
async fn test_service_provider_flat_across_cap_and_backstop() -> anyhow::Result<()> {
    const EMISSIONS: u64 = 1_000_000_000_000;

    let harness = common::setup_iceberg().await?;
    let reward_writers = common::reward_writers(&harness).await?;

    async fn sp_total(
        reward_writers: &mobile_verifier::iceberg::RewardWriters,
        hnt_issued: u64,
        delegation: u64,
    ) -> anyhow::Result<u64> {
        let (client, sink) = common::create_file_sink();
        let mut reward_info = reward_info_24_hours();
        reward_info.epoch_emissions = Decimal::from(hnt_issued + delegation);
        reward_info.hnt_rewards_issued = Decimal::from(hnt_issued);
        reward_info.delegation_rewards_issued = Decimal::from(delegation);

        // A distinct write id per call: the writers are idempotent, so reusing
        // one would silently drop the second epoch's row.
        let write_id = format!("test-epoch-{hnt_issued}-{delegation}");
        rewarder::reward_service_providers(client, &reward_info, reward_writers, &write_id).await?;
        let rewards = sink.finish().await?;
        Ok(rewards.sp_rewards.iter().map(|r| r.amount).sum())
    }

    // Cap: issued 80%, delegation 20%. Backstop: issued 98%, delegation 2%.
    let capped = sp_total(&reward_writers, EMISSIONS * 80 / 100, EMISSIONS * 20 / 100).await?;
    let backstopped = sp_total(&reward_writers, EMISSIONS * 98 / 100, EMISSIONS * 2 / 100).await?;

    assert_eq!(
        capped, backstopped,
        "SP pool must not move with the cap/backstop"
    );
    // Independent of `hip_149_reward_pools`: a flat 24% of total emissions.
    assert_eq!(capped, 240_000_000_000);

    Ok(())
}
