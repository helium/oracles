//! Sub-DAO reward split.
//!
//! Each epoch the chain hands the rewarder two figures via [`EpochRewardInfo`]:
//! `epoch_emissions` (the 100% total) and `hnt_rewards_issued` (the slice this
//! rewarder must distribute). The remainder — `delegation_rewards_issued` — is
//! paid to veHNT delegators on-chain and is never touched here.
//!
//! The rewarder splits `hnt_rewards_issued` into two pools:
//!
//! * **Service providers** — a flat [`SERVICE_PROVIDER_PERCENT`] of *total*
//!   emissions, fixed regardless of the cap/backstop.
//! * **Data transfer** — the *residual*: `hnt_rewards_issued − service_provider`.
//!
//! **Under HIP-150 the service-provider percent is zero**, so data transfer is
//! the whole issued amount. The split is kept rather than collapsed because the
//! contribution is a suspension with an end date (2027-07-31, extendable once),
//! not a retirement — see [`SERVICE_PROVIDER_PERCENT`]. Everything below
//! describes the mechanism that governs both states.
//!
//! Making data transfer the residual (rather than a fixed percentage of its own)
//! is what keeps the split exact. The 3× cap moves HNT out of the data bucket and
//! into delegation, shrinking `hnt_rewards_issued`; the backstop re-emits HNT
//! into it, growing `hnt_rewards_issued`. A fixed percentage of emissions would
//! over-allocate when earnings run over the cap (minting HNT that was moved to
//! delegators) and under-allocate when they fall under the backstop (stranding
//! the re-emitted HNT). The residual instead absorbs the shift — and the sub-bone
//! dropped when flooring the service-provider share — so
//!
//! ```text
//! service_provider + data_transfer == floor(hnt_rewards_issued)
//! ```
//!
//! holds exactly, every epoch, at any percent. The proptests below pin that
//! invariant across the full input range.

use crate::rewarder::EpochRewardInfo;
use rust_decimal::Decimal;

use super::{floor_to_u64, SERVICE_PROVIDER_PERCENT};

/// The two pools the rewarder distributes, in HNT bones. By construction
/// `service_provider + data_transfer == floor(hnt_rewards_issued)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RewardPools {
    pub service_provider: u64,
    pub data_transfer: u64,
}

/// Split an epoch's `hnt_rewards_issued` into the service-provider and
/// data-transfer pools (see module docs).
///
/// The split mechanism is HIP-149's and is unchanged; HIP-150 sets
/// [`SERVICE_PROVIDER_PERCENT`] to zero, which makes data transfer the whole
/// issued amount.
pub fn hip_149_reward_pools(reward_info: &EpochRewardInfo) -> RewardPools {
    split(reward_info.epoch_emissions, reward_info.hnt_rewards_issued)
}

/// Core split, factored out from [`hip_149_reward_pools`] for property testing.
/// `total_emissions` is the 100% pool; `hnt_rewards_issued` is the rewarder's
/// slice of it.
fn split(total_emissions: Decimal, hnt_rewards_issued: Decimal) -> RewardPools {
    let hnt = floor_to_u64(hnt_rewards_issued);

    // Service providers take a flat `SERVICE_PROVIDER_PERCENT` of *total*
    // emissions, floored. We never round *up* — that is what guarantees the
    // service-provider pool can't push the distributed total past
    // `hnt_rewards_issued`. The `.min(hnt)` clamp keeps the residual non-negative
    // if the chain ever reported delegation so high that the percent exceeds what
    // was issued: in that case service providers simply take whatever HNT there
    // was. Dormant while the percent is zero (HIP-150), and kept for when the
    // contribution ends.
    let service_provider = floor_to_u64(total_emissions * SERVICE_PROVIDER_PERCENT).min(hnt);

    // Data transfer is the residual. `service_provider <= hnt`, so this never
    // underflows, and the sub-bone dropped by flooring above lands here rather
    // than leaking — keeping `service_provider + data_transfer == hnt` exact.
    let data_transfer = hnt - service_provider;

    RewardPools {
        service_provider,
        data_transfer,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    const MAX_BONES: u64 = 10_000_000_000_000_000; // ~ HNT max supply, in bones

    fn run(total: u64, hnt: u64) -> RewardPools {
        split(Decimal::from(total), Decimal::from(hnt))
    }

    // The expected service-provider pool, reconstructed independently of `split`.
    fn expected_sp(total: u64, hnt: u64) -> u64 {
        floor_to_u64(Decimal::from(total) * SERVICE_PROVIDER_PERCENT).min(hnt)
    }

    #[test]
    fn baseline_6pct_delegation_gives_data_the_whole_94() {
        // total = 100, delegation = 6 → hnt = 94. HIP-150: SP = 0, so data takes
        // all 94 — the 70 it had under HIP-149 plus the 24 SP contributed.
        assert_eq!(
            run(100, 94),
            RewardPools {
                service_provider: 0,
                data_transfer: 94
            }
        );
    }

    #[test]
    fn cap_shrinks_data_only() {
        // 3× cap moved 14 from data → delegation: delegation 6+14=20, hnt = 80.
        // Data absorbs the whole cut.
        assert_eq!(
            run(100, 80),
            RewardPools {
                service_provider: 0,
                data_transfer: 80
            }
        );
    }

    #[test]
    fn backstop_grows_data_only() {
        // Backstop re-emitted HNT into the data bucket: hnt = 98 (> 94).
        // Data absorbs the boost.
        assert_eq!(
            run(100, 98),
            RewardPools {
                service_provider: 0,
                data_transfer: 98
            }
        );
    }

    #[test]
    fn no_delegation_gives_data_everything() {
        assert_eq!(
            run(100, 100),
            RewardPools {
                service_provider: 0,
                data_transfer: 100
            }
        );
    }

    #[test]
    fn far_below_baseline_issuance_still_accounts_exactly() {
        // Out-of-spec: only 20 HNT issued against 100 emitted. At percent 0 the
        // `.min(hnt)` clamp is dormant and data simply takes what was issued.
        // (Under a non-zero percent this is the case where SP would be clamped.)
        assert_eq!(
            run(100, 20),
            RewardPools {
                service_provider: 0,
                data_transfer: 20
            }
        );
    }

    #[test]
    fn zero_hnt_allocates_nothing() {
        assert_eq!(
            run(100, 0),
            RewardPools {
                service_provider: 0,
                data_transfer: 0
            }
        );
    }

    proptest! {
        /// THE invariant: the rewarder distributes exactly `hnt_rewards_issued`,
        /// never more (minting HNT that was moved to delegators) nor less
        /// (stranding re-emitted HNT). Inputs are generated as (hnt, delegation)
        /// so `total = hnt + delegation` and `hnt <= total` always — covering the
        /// cap (large delegation), the backstop (~0 delegation), and the baseline.
        #[test]
        fn never_over_or_under_allocates_hnt(
            hnt in 0u64..=MAX_BONES,
            delegation in 0u64..=MAX_BONES,
        ) {
            let total = hnt + delegation;
            let pools = run(total, hnt);
            prop_assert_eq!(
                pools.service_provider + pools.data_transfer,
                hnt,
                "sp {} + data {} != hnt {}",
                pools.service_provider,
                pools.data_transfer,
                hnt
            );
        }

        /// Neither pool can exceed the issued HNT (and, being u64, neither can go
        /// negative).
        #[test]
        fn pools_are_bounded_by_issued_hnt(
            hnt in 0u64..=MAX_BONES,
            delegation in 0u64..=MAX_BONES,
        ) {
            let total = hnt + delegation;
            let pools = run(total, hnt);
            prop_assert!(pools.service_provider <= hnt);
            prop_assert!(pools.data_transfer <= hnt);
        }

        /// Service providers get a flat `SERVICE_PROVIDER_PERCENT` of *total*
        /// emissions, independent of how the cap/backstop split that total between
        /// hnt and delegation — except in the out-of-spec regime where less than
        /// that percent was issued, where SP is clamped to what's available.
        /// Percent-agnostic: `expected_sp` reads the same constant, so this keeps
        /// holding when the HIP-150 contribution ends and the percent returns.
        #[test]
        fn service_provider_matches_configured_percent(
            hnt in 0u64..=MAX_BONES,
            delegation in 0u64..=MAX_BONES,
        ) {
            let total = hnt + delegation;
            let pools = run(total, hnt);
            prop_assert_eq!(pools.service_provider, expected_sp(total, hnt));
        }

        /// Holding total emissions fixed, data transfer tracks issued HNT
        /// one-for-one — the cap (less hnt) shrinks it, the backstop (more hnt)
        /// grows it — while the service-provider pool is unchanged, as long as a
        /// full `SERVICE_PROVIDER_PERCENT` of total was issued (clamp not
        /// engaged).
        #[test]
        fn data_tracks_hnt_one_for_one_with_sp_fixed(
            (total, hnt_a, hnt_b) in (1u64..=MAX_BONES).prop_flat_map(|total| {
                let sp = floor_to_u64(Decimal::from(total) * SERVICE_PROVIDER_PERCENT);
                (Just(total), sp..=total, sp..=total)
            })
        ) {
            let a = run(total, hnt_a);
            let b = run(total, hnt_b);
            prop_assert_eq!(a.service_provider, b.service_provider);
            prop_assert_eq!(
                a.data_transfer as i128 - b.data_transfer as i128,
                hnt_a as i128 - hnt_b as i128
            );
        }

        /// HIP-150 Decision 3: with the service-provider contribution active, the
        /// data-transfer pool is the *entire* issued amount and nothing is held
        /// back. Distinct from `never_over_or_under_allocates_hnt`, which only
        /// pins that the two pools sum to it — this pins which pool gets it.
        ///
        /// This test is specific to the suspension: when the contribution ends and
        /// `SERVICE_PROVIDER_PERCENT` returns to a non-zero value, delete it. The
        /// invariant tests above are the ones that hold in both states.
        #[test]
        fn hip_150_data_transfer_takes_the_whole_issued_amount(
            hnt in 0u64..=MAX_BONES,
            delegation in 0u64..=MAX_BONES,
        ) {
            let pools = run(hnt + delegation, hnt);
            prop_assert_eq!(pools.service_provider, 0);
            prop_assert_eq!(pools.data_transfer, hnt);
        }
    }
}
