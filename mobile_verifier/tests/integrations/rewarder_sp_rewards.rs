//! HIP-150 Decision 3: Nova Labs contributes its Service Provider Rewards to the
//! Deployer Data Reward Pool, so the service-provider pool is zero and the
//! rewarder emits nothing for it.
//!
//! These tests pin the *suspension*. The contribution runs to 2027-07-31 and may
//! be extended once by a year — when it ends and `SERVICE_PROVIDER_PERCENT`
//! returns to a non-zero value, these become wrong and the pre-HIP-150 versions
//! (asserting one reward at the configured percent) should come back.

use crate::common::{self, reward_info_24_hours};
use mobile_verifier::{reward_shares, rewarder};
use rust_decimal::Decimal;

// No database involved: these exercise the pool split and the file sink only.
// (The pre-HIP-150 versions took an unused `PgPool` via `#[sqlx::test]`, which
// made them require a live Postgres to run.)
#[tokio::test]
async fn test_no_service_provider_rewards_while_contribution_active() -> anyhow::Result<()> {
    let (mobile_rewards_client, mobile_rewards) = common::create_file_sink();

    let reward_info = reward_info_24_hours();

    // The pool itself is zero...
    assert_eq!(
        reward_shares::hip_149_reward_pools(&reward_info).service_provider,
        0
    );

    rewarder::reward_service_providers(mobile_rewards_client, &reward_info, None).await?;

    let rewards = mobile_rewards.finish().await?;

    // ...and no reward is written at all — not a reward of zero. A consumer sees
    // no service-provider rows for the epoch rather than a row claiming an award
    // of nothing.
    assert!(
        rewards.sp_rewards.is_empty(),
        "expected no service provider rewards, got {:?}",
        rewards.sp_rewards
    );

    // Nor is the suspended pool reported as unallocated: there is no pool to
    // leave unallocated, because it was never carved out of the issued HNT.
    // `emissions_split` gives the whole issued amount to data transfer.
    assert!(
        rewards.unallocated.is_empty(),
        "expected no unallocated rewards, got {:?}",
        rewards.unallocated
    );

    Ok(())
}

/// The 3× cap and the backstop shift HNT between issued and delegation. Under
/// HIP-149 that had to leave the flat service-provider pool untouched; under
/// HIP-150 there is no pool to move, so the rewarder stays silent in both
/// regimes. Complements the data-transfer cap/backstop tests, which show the data
/// pool absorbing the whole shift.
#[tokio::test]
async fn test_no_service_provider_rewards_across_cap_and_backstop() -> anyhow::Result<()> {
    const EMISSIONS: u64 = 1_000_000_000_000;

    async fn sp_reward_count(hnt_issued: u64, delegation: u64) -> anyhow::Result<usize> {
        let (client, sink) = common::create_file_sink();
        let mut reward_info = reward_info_24_hours();
        reward_info.epoch_emissions = Decimal::from(hnt_issued + delegation);
        reward_info.hnt_rewards_issued = Decimal::from(hnt_issued);
        reward_info.delegation_rewards_issued = Decimal::from(delegation);

        rewarder::reward_service_providers(client, &reward_info, None).await?;
        let rewards = sink.finish().await?;
        Ok(rewards.sp_rewards.len())
    }

    // Cap: issued 80%, delegation 20%. Backstop: issued 98%, delegation 2%.
    let capped = sp_reward_count(EMISSIONS * 80 / 100, EMISSIONS * 20 / 100).await?;
    let backstopped = sp_reward_count(EMISSIONS * 98 / 100, EMISSIONS * 2 / 100).await?;

    assert_eq!(capped, 0, "cap must not produce a service provider reward");
    assert_eq!(
        backstopped, 0,
        "backstop must not produce a service provider reward"
    );

    Ok(())
}
