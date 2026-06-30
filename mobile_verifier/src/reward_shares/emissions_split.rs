//! HIP-149 sub-DAO reward split.
//!
//! Each epoch the chain hands the rewarder two figures via [`EpochRewardInfo`]:
//! `epoch_emissions` (the 100% total) and `hnt_rewards_issued` (the slice this
//! rewarder must distribute). The remainder — `delegation_rewards_issued` — is
//! paid to veHNT delegators on-chain and is never touched here.
//!
//! The rewarder splits `hnt_rewards_issued` into two pools:
//!
//! * **Service providers** — a flat 24% of *total* emissions. HIP-149 keeps this
//!   fixed regardless of the cap/backstop.
//! * **Data transfer** — the *residual*: `hnt_rewards_issued − service_provider`.
//!
//! Making data transfer the residual (rather than a fixed 70%) is what keeps the
//! split exact under HIP-149. The 3× cap moves HNT out of the data bucket and
//! into delegation, shrinking `hnt_rewards_issued`; the backstop re-emits HNT
//! into it, growing `hnt_rewards_issued`. A fixed `0.70 × emissions` would
//! over-allocate when earnings run over the cap (minting HNT that was moved to
//! delegators) and under-allocate when they fall under the backstop (stranding
//! the re-emitted HNT). The residual
//! instead absorbs the shift — and the sub-bone dropped when flooring the
//! service-provider 24% — so
//!
//! ```text
//! service_provider + data_transfer == floor(hnt_rewards_issued)
//! ```
//!
//! holds exactly, every epoch. The proptests below pin that invariant across the
//! full input range.

use mobile_config::sub_dao_epoch_reward_info::EpochRewardInfo;
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
pub fn hip_149_reward_pools(reward_info: &EpochRewardInfo) -> RewardPools {
    split(reward_info.epoch_emissions, reward_info.hnt_rewards_issued)
}

/// Core split, factored out from [`hip_149_reward_pools`] for property testing.
/// `total_emissions` is the 100% pool; `hnt_rewards_issued` is the rewarder's
/// slice of it.
fn split(total_emissions: Decimal, hnt_rewards_issued: Decimal) -> RewardPools {
    let hnt = floor_to_u64(hnt_rewards_issued);

    // Service providers take a flat 24% of *total* emissions, floored. We never
    // round *up* — that is what guarantees the service-provider pool can't push
    // the distributed total past `hnt_rewards_issued`. The `.min(hnt)` clamp
    // keeps the residual non-negative even if the chain ever reported delegation
    // above 76% of total (beyond what the 3× cap can produce, since it can move
    // at most the 70% data bucket): in that case service providers simply take
    // whatever HNT was issued.
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
    fn baseline_6pct_delegation_is_70_24() {
        // total = 100, delegation = 6 → hnt = 94. SP = 24, data = 70.
        assert_eq!(
            run(100, 94),
            RewardPools {
                service_provider: 24,
                data_transfer: 70
            }
        );
    }

    #[test]
    fn cap_shrinks_data_only() {
        // 3× cap moved 14 from data → delegation: delegation 6+14=20, hnt = 80.
        // SP stays at 24% of total; data absorbs the whole 14 (70 → 56).
        assert_eq!(
            run(100, 80),
            RewardPools {
                service_provider: 24,
                data_transfer: 56
            }
        );
    }

    #[test]
    fn backstop_grows_data_only() {
        // Backstop re-emitted HNT into the data bucket: hnt = 98 (> 94).
        // SP unchanged; data absorbs the boost (70 → 74).
        assert_eq!(
            run(100, 98),
            RewardPools {
                service_provider: 24,
                data_transfer: 74
            }
        );
    }

    #[test]
    fn no_delegation_gives_sp_24_data_76() {
        assert_eq!(
            run(100, 100),
            RewardPools {
                service_provider: 24,
                data_transfer: 76
            }
        );
    }

    #[test]
    fn extreme_cap_clamps_sp_to_issued_hnt() {
        // Out-of-spec: only 20 HNT issued though 24% of total would be 24. SP
        // takes all 20, data is 0 — still accounts for exactly `hnt`.
        assert_eq!(
            run(100, 20),
            RewardPools {
                service_provider: 20,
                data_transfer: 0
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

        /// Service providers get a flat 24% of *total* emissions, independent of
        /// how the cap/backstop split that total between hnt and delegation —
        /// except in the out-of-spec regime where less than 24% was issued, where
        /// SP is clamped to what's available.
        #[test]
        fn service_provider_is_24pct_of_total(
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
        /// full 24% of total was issued (clamp not engaged).
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
    }
}
