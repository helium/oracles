//! Data-transfer reward allocation.
//!
//! With Proof-of-Coverage removed, the entire data-transfer pool is distributed
//! to hotspots every epoch in proportion to the data credits (`num_dcs`) each
//! burned. There is no cap at a hotspot's raw DC value: if the pool is larger
//! than the value of all transferred data, rewards scale *up* to consume the
//! pool; if smaller, they scale *down*. Because the result is a pure
//! proportional share of a fixed pool, the HNT price cancels out and plays no
//! part in the allocation (it is still recorded on each `GatewayReward` for
//! reference).
//!
//! The arithmetic mirrors the existing reward path so the rounding is
//! behavior-preserving: a `Decimal` rate (`pool / total_dc`, the analogue of the
//! old `reward_scale`) and a `round_dp_with_strategy(0, ToZero)` floor per
//! hotspot. Integer bones can't be split infinitely, so the rounded-down rewards
//! leave a small remainder — at most one bone per rewarded hotspot. That
//! remainder, or the entire pool when no data credits were burned, is reported
//! as unallocated.
//!
//! Input and output are keyed structs ([`GatewayDataTransfer`] /
//! [`DataTransferReward`]), so a gateway's key and `rewardable_bytes` travel with
//! its reward — there is no positional collection to keep in sync and no lookup
//! back into the input.

use std::ops::Range;

use chrono::{DateTime, Utc};
use file_store::traits::TimestampEncode;
use helium_crypto::PublicKeyBinary;
use rust_decimal::{prelude::*, Decimal, RoundingStrategy};

use crate::iceberg::gateway_reward::IcebergGatewayReward;

mod proto {
    pub use helium_proto::services::poc_mobile::mobile_reward_share::Reward;
    pub use helium_proto::services::poc_mobile::GatewayReward;
    pub use helium_proto::services::poc_mobile::MobileRewardShare;
}

/// A gateway's rewardable data transfer for an epoch — the input to [`allocate`].
/// Generic over the key type (`PublicKeyBinary` in production).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GatewayDataTransfer<K> {
    pub hotspot_key: K,
    /// Data credits burned — what the reward is proportional to.
    pub rewardable_dc: u64,
    /// Carried through to the reward for the `GatewayReward.rewardable_bytes`
    /// field; does not affect the allocation.
    pub rewardable_bytes: u64,
}

/// A gateway's data-transfer reward — the output of [`allocate`], carrying
/// everything needed to emit a `GatewayReward`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataTransferReward<K> {
    pub hotspot_key: K,
    /// Reward in HNT bones. May be zero (e.g. a gateway with no data credits).
    pub reward: u64,
    pub rewardable_bytes: u64,
}

/// The outcome of distributing a data-transfer pool.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataTransferAllocation<K> {
    /// One entry per input gateway, in input order. Includes zero-reward
    /// gateways; callers that emit `GatewayReward`s should skip `reward == 0`.
    pub rewards: Vec<DataTransferReward<K>>,
    /// Bones left undistributed: integer-rounding dust, or the entire pool when
    /// the total data credits were zero.
    pub unallocated: u64,
}

/// Distribute the `pool` (HNT bones) across gateways in proportion to their data
/// credits, carrying each gateway's `rewardable_bytes` through to its reward.
///
/// `rate = pool / total_dc`; `reward = floor_to_zero(rewardable_dc * rate)`; the
/// remainder `floor(pool) - sum(rewards)` is returned as `unallocated`. When
/// `total_dc == 0` the whole pool is unallocated (nothing to distribute against).
///
/// Guarantees (exercised by the proptests below):
/// * `sum(rewards) + unallocated == floor(pool)` — the pool is fully accounted.
/// * `sum(rewards) <= floor(pool)` — never over-allocates.
/// * `total_dc > 0  =>  unallocated <= rewards.len()` — only rounding dust.
/// * each input key (and its `rewardable_bytes`) appears once in the output.
pub fn allocate<K>(
    pool: Decimal,
    gateways: impl IntoIterator<Item = GatewayDataTransfer<K>>,
) -> DataTransferAllocation<K> {
    let gateways: Vec<GatewayDataTransfer<K>> = gateways.into_iter().collect();
    let pool_bones = floor_to_u64(pool);

    let total_dc: Decimal = gateways
        .iter()
        .map(|g| Decimal::from(g.rewardable_dc))
        .sum();

    // No data credits to weigh against -> nobody is paid; the whole pool is
    // unallocated. (`rate` below would divide by zero otherwise.)
    if total_dc.is_zero() {
        return DataTransferAllocation {
            rewards: gateways
                .into_iter()
                .map(|g| DataTransferReward {
                    hotspot_key: g.hotspot_key,
                    reward: 0,
                    rewardable_bytes: g.rewardable_bytes,
                })
                .collect(),
            unallocated: pool_bones,
        };
    }

    // Bones per data credit — the analogue of the existing `reward_scale`. The
    // HNT price is not involved; it cancels out in a proportional split.
    let rate = pool / total_dc;
    let rewards: Vec<DataTransferReward<K>> = gateways
        .into_iter()
        .map(|g| DataTransferReward {
            hotspot_key: g.hotspot_key,
            reward: floor_to_u64(Decimal::from(g.rewardable_dc) * rate),
            rewardable_bytes: g.rewardable_bytes,
        })
        .collect();

    let allocated: u64 = rewards.iter().map(|r| r.reward).sum();
    DataTransferAllocation {
        rewards,
        // allocated <= floor(pool), so this cannot underflow; the proptests
        // assert that saturation never actually triggers.
        unallocated: pool_bones.saturating_sub(allocated),
    }
}

/// Floor a non-negative `Decimal` to a `u64` (`ToZero` matches the existing
/// reward rounding). Values above `u64::MAX` saturate, which never happens for a
/// real reward pool.
fn floor_to_u64(value: Decimal) -> u64 {
    value
        .round_dp_with_strategy(0, RoundingStrategy::ToZero)
        .to_u64()
        .unwrap_or(0)
}

// ================================================================
// Helpers
//
impl DataTransferReward<PublicKeyBinary> {
    pub fn into_gateway_reward(self, price: u64) -> proto::GatewayReward {
        proto::GatewayReward {
            hotspot_key: self.hotspot_key.into(),
            dc_transfer_reward: self.reward,
            rewardable_bytes: self.rewardable_bytes,
            price,
        }
    }
}

pub fn to_iceberg_rewards(
    coll: &[proto::GatewayReward],
    period: &Range<DateTime<Utc>>,
) -> Vec<IcebergGatewayReward> {
    coll.iter()
        .map(|gateway| IcebergGatewayReward::from_gateway_reward(gateway, period))
        .collect()
}

pub fn into_proto_rewards(
    coll: Vec<proto::GatewayReward>,
    period: &Range<DateTime<Utc>>,
) -> Vec<proto::MobileRewardShare> {
    coll.into_iter()
        .map(|gateway| proto::MobileRewardShare {
            start_period: period.start.encode_timestamp(),
            end_period: period.end.encode_timestamp(),
            reward: Some(proto::Reward::GatewayReward(gateway)),
        })
        .collect()
}

// ================================================================
// Reward metrics
//
// Observability-only helpers (proptested below): they derive the data-transfer
// reward gauges and never feed back into the allocation above.

/// Bytes per gigabyte (decimal GB — the data-transfer convention).
const BYTES_PER_GB: u64 = 1_000_000_000;

/// Total bones actually paid out to gateways — the sum of every
/// `dc_transfer_reward`. Pairs with [`scale`] / [`price_per_gb`] to report the
/// realized (post-rounding) reward rate.
pub fn distributed_bones(rewards: &[proto::GatewayReward]) -> Decimal {
    let total: u64 = rewards.iter().map(|r| r.dc_transfer_reward).sum();
    Decimal::from(total)
}

/// `pool / demand` — the per-DC reward multiplier (`< 1` scaled down, `> 1` scaled
/// up, `1` at parity; `0` when there is no demand).
pub fn scale(pool: Decimal, demand: Decimal) -> Decimal {
    if demand.is_zero() {
        Decimal::ZERO
    } else {
        pool / demand
    }
}

/// Price per GB transferred: `value_bones` priced via `price_per_bone` and spread
/// over the total GB (`0` when no data moved). Used both for the full-pool rate
/// (`value = pool`) and the realized post-rounding rate (`value = distributed`).
pub fn price_per_gb(value_bones: Decimal, total_bytes: u64, price_per_bone: Decimal) -> Decimal {
    if total_bytes == 0 {
        Decimal::ZERO
    } else {
        value_bones * price_per_bone * Decimal::from(BYTES_PER_GB) / Decimal::from(total_bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::PriceInfo;
    use proptest::prelude::*;
    use rust_decimal_macros::dec;

    fn gw<K>(hotspot_key: K, rewardable_dc: u64, rewardable_bytes: u64) -> GatewayDataTransfer<K> {
        GatewayDataTransfer {
            hotspot_key,
            rewardable_dc,
            rewardable_bytes,
        }
    }

    fn reward<K>(hotspot_key: K, reward: u64, rewardable_bytes: u64) -> DataTransferReward<K> {
        DataTransferReward {
            hotspot_key,
            reward,
            rewardable_bytes,
        }
    }

    /// Reward amounts in output order — handy when keys/bytes aren't the point.
    fn amounts<K>(alloc: &DataTransferAllocation<K>) -> Vec<u64> {
        alloc.rewards.iter().map(|r| r.reward).collect()
    }

    #[test]
    fn single_hotspot_takes_whole_pool() {
        let alloc = allocate(dec!(1000), [gw("a", 20, 64)]);
        assert_eq!(alloc.rewards, vec![reward("a", 1000, 64)]);
        assert_eq!(alloc.unallocated, 0);
    }

    #[test]
    fn into_gateway_reward_records_price() {
        // The HNT price is stamped onto the reward here — and only here. It
        // plays no part in the allocation, ensure it makes it is unchanged.
        let key: PublicKeyBinary = vec![1u8; 33].into();
        let gateway = reward(key.clone(), 4_200, 99).into_gateway_reward(123_456);

        assert_eq!(gateway.price, 123_456);
        assert_eq!(gateway.dc_transfer_reward, 4_200);
        assert_eq!(gateway.rewardable_bytes, 99);
        assert_eq!(gateway.hotspot_key, Vec::<u8>::from(key));
    }

    #[test]
    fn excess_demand_scales_down() {
        // 200 DC of demand sharing a 100-bone pool -> each scaled down to 50.
        let alloc = allocate(dec!(100), [gw("a", 100, 0), gw("b", 100, 0)]);
        assert_eq!(amounts(&alloc), vec![50, 50]);
        assert_eq!(alloc.unallocated, 0);
    }

    #[test]
    fn low_demand_scales_up() {
        // Tiny demand (2 DC total) -> scaled up to consume the whole 1000 pool.
        let alloc = allocate(dec!(1000), [gw("a", 1, 0), gw("b", 1, 0)]);
        assert_eq!(amounts(&alloc), vec![500, 500]);
        assert_eq!(alloc.unallocated, 0);
    }

    #[test]
    fn key_and_bytes_stay_with_their_reward() {
        // Distinct keys, DC, and bytes -> each gateway keeps its own share and
        // its own byte count, regardless of position.
        let alloc = allocate(
            dec!(1000),
            [gw("a", 1, 111), gw("b", 3, 222), gw("c", 6, 333)],
        );
        assert_eq!(
            alloc.rewards,
            vec![
                reward("a", 100, 111),
                reward("b", 300, 222),
                reward("c", 600, 333),
            ]
        );
        assert_eq!(alloc.unallocated, 0);
    }

    #[test]
    fn rounding_dust_goes_to_unallocated() {
        // 10 bones across 3 equal shares -> 3 each, 1 bone of dust.
        let alloc = allocate(dec!(10), [gw("a", 1, 0), gw("b", 1, 0), gw("c", 1, 0)]);
        assert_eq!(amounts(&alloc), vec![3, 3, 3]);
        assert_eq!(alloc.unallocated, 1);
    }

    #[test]
    fn fractional_pool_floors_into_unallocated() {
        // The sub-bone fraction of the pool can't be distributed; it floors away.
        let alloc = allocate(dec!(100.9), [gw("a", 1, 0)]);
        assert_eq!(amounts(&alloc), vec![100]);
        assert_eq!(alloc.unallocated, 0);
    }

    #[test]
    fn zero_dc_entries_get_nothing_but_keep_their_bytes() {
        let alloc = allocate(dec!(100), [gw("a", 0, 7), gw("b", 10, 9), gw("c", 0, 11)]);
        assert_eq!(
            alloc.rewards,
            vec![reward("a", 0, 7), reward("b", 100, 9), reward("c", 0, 11)]
        );
        assert_eq!(alloc.unallocated, 0);
    }

    #[test]
    fn no_data_credits_leaves_whole_pool_unallocated() {
        let alloc = allocate(dec!(1000), [gw("a", 0, 5), gw("b", 0, 6)]);
        assert_eq!(alloc.rewards, vec![reward("a", 0, 5), reward("b", 0, 6)]);
        assert_eq!(alloc.unallocated, 1000);

        let empty = allocate(dec!(1000), Vec::<GatewayDataTransfer<&str>>::new());
        assert!(empty.rewards.is_empty());
        assert_eq!(empty.unallocated, 1000);
    }

    // Pool with a realistic magnitude and a fractional part, like 0.7 * emissions.
    fn pool_strategy() -> impl Strategy<Value = Decimal> {
        (any::<u64>(), 0i64..100).prop_map(|(n, frac)| Decimal::from(n) + Decimal::new(frac, 2))
    }

    // Keyed input: the index is the key, so we can assert keys/bytes never desync.
    fn keyed(data: &[(u64, u64)]) -> Vec<GatewayDataTransfer<usize>> {
        data.iter()
            .copied()
            .enumerate()
            .map(|(i, (dc, bytes))| gw(i, dc, bytes))
            .collect()
    }

    proptest! {

        /// The pool is fully accounted for, never over-allocated, and every key
        /// (and its bytes) is preserved 1:1 — across the full u64 range.
        #[test]
        fn pool_is_fully_accounted(
            pool in pool_strategy(),
            data in prop::collection::vec((any::<u64>(), any::<u64>()), 0..256),
        ) {
            let alloc = allocate(pool, keyed(&data));
            let pool_bones = floor_to_u64(pool);

            prop_assert_eq!(alloc.rewards.len(), data.len());
            // Keys and bytes are preserved 1:1 and in order — no desync possible.
            for (i, r) in alloc.rewards.iter().enumerate() {
                prop_assert_eq!(r.hotspot_key, i);
                prop_assert_eq!(r.rewardable_bytes, data[i].1);
            }

            let allocated: u128 = alloc.rewards.iter().map(|r| r.reward as u128).sum();
            prop_assert!(allocated <= pool_bones as u128);
            prop_assert_eq!(allocated + alloc.unallocated as u128, pool_bones as u128);

            let total_dc: u128 = data.iter().map(|&(dc, _)| dc as u128).sum();
            if total_dc == 0 {
                prop_assert_eq!(alloc.unallocated, pool_bones);
                prop_assert!(alloc.rewards.iter().all(|r| r.reward == 0));
            } else {
                // Only rounding dust: at most one bone per entry.
                prop_assert!((alloc.unallocated as usize) <= data.len());
            }
        }

        /// More data credits never earns a smaller reward within the same epoch.
        #[test]
        fn rewards_are_monotonic_in_dc(
            pool in pool_strategy(),
            data in prop::collection::vec((any::<u64>(), any::<u64>()), 1..64),
        ) {
            let alloc = allocate(pool, keyed(&data));
            for i in 0..data.len() {
                for j in 0..data.len() {
                    if data[i].0 <= data[j].0 {
                        prop_assert!(alloc.rewards[i].reward <= alloc.rewards[j].reward);
                    }
                }
            }
        }

        /// Hotspots with equal DC receive equal rewards.
        #[test]
        fn equal_dc_yields_equal_reward(
            pool in pool_strategy(),
            dc in 1u64..=u64::MAX,
            n in 1usize..32,
        ) {
            let data: Vec<(u64, u64)> = (0..n).map(|_| (dc, 0)).collect();
            let alloc = allocate(pool, keyed(&data));
            prop_assert!(alloc.rewards.windows(2).all(|w| w[0].reward == w[1].reward));
        }

        /// Rounding dust is bounded by the gateway count: when any data credits
        /// were burned (`total_dc > 0`), each gateway's floored reward loses at
        /// most one bone, so the unallocated remainder can never exceed the number
        /// of gateways being rewarded. (The whole pool is unallocated only when
        /// `total_dc == 0`, excluded here by giving every gateway `dc >= 1` — the
        /// worst case for dust, since every gateway can floor down.)
        #[test]
        fn unallocated_dust_never_exceeds_gateway_count(
            pool in pool_strategy(),
            data in prop::collection::vec((1u64..=u64::MAX, any::<u64>()), 1..256),
        ) {
            let alloc = allocate(pool, keyed(&data));
            prop_assert!(
                (alloc.unallocated as usize) <= data.len(),
                "unallocated {} exceeded gateway count {}",
                alloc.unallocated,
                data.len(),
            );
        }

        #[test]
        fn unallocated_never_exceeds_individual_gw_rewards(
            pool in pool_strategy(),
            data in prop::collection::vec((any::<u64>(), any::<u64>()), 0..256),
        ) {

            let alloc = allocate(pool, keyed(&data));

            for reward in alloc.rewards {
                prop_assert!(alloc.unallocated < reward.reward);
            }
        }
    }

    // ---- Reward-metric helpers: scale / price_per_gb / distributed_bones ----

    #[test]
    fn distributed_bones_sums_transfer_rewards() {
        let rewards = vec![
            proto::GatewayReward {
                hotspot_key: vec![],
                dc_transfer_reward: 3,
                rewardable_bytes: 0,
                price: 0,
            },
            proto::GatewayReward {
                hotspot_key: vec![],
                dc_transfer_reward: 7,
                rewardable_bytes: 0,
                price: 0,
            },
        ];
        assert_eq!(distributed_bones(&rewards), dec!(10));
    }

    #[test]
    fn scale_is_zero_without_demand() {
        // No demand -> no multiplier (and, crucially, no 0/0 panic).
        assert_eq!(scale(dec!(1000), Decimal::ZERO), Decimal::ZERO);
    }

    #[test]
    fn price_per_gb_is_zero_without_bytes() {
        // No data moved -> no price per GB (this guard is the divide-by-zero check).
        let price_per_bone = PriceInfo::new(10_000_000, 8).price_per_bone;
        assert_eq!(
            price_per_gb(dec!(1000000), 0, price_per_bone),
            Decimal::ZERO
        );
    }

    #[test]
    fn price_per_gb_is_zero_without_value() {
        // Nothing distributed -> $0 per GB, no matter how much data moved.
        let price_per_bone = PriceInfo::new(10_000_000, 8).price_per_bone;
        assert_eq!(
            price_per_gb(Decimal::ZERO, 1_000_000_000, price_per_bone),
            Decimal::ZERO
        );
    }

    proptest! {
        /// With demand present, `scale` is non-negative and tracks the pool-vs-demand
        /// direction (`< 1` over-subscribed, `> 1` under, `= 1` at parity). The
        /// zero-demand case is covered by `scale_is_zero_without_demand`.
        #[test]
        fn scale_invariants(
            pool_bones in 0u64..=1_000_000_000_000_000,
            demand_bones in 1u64..=1_000_000_000_000_000,
        ) {
            let pool = Decimal::from(pool_bones);
            let demand = Decimal::from(demand_bones);
            let multiplier = scale(pool, demand);

            prop_assert!(multiplier >= Decimal::ZERO);
            // the multiplier crosses 1 exactly as the pool crosses demand.
            prop_assert_eq!(multiplier.cmp(&Decimal::ONE), pool.cmp(&demand));
        }

        /// With data present, `price_per_gb` is positive and monotonic in the
        /// distributed value — so the realized rate (we pay out `pool` minus rounding
        /// dust) never exceeds the full-pool rate. The zero-bytes and zero-value
        /// cases are covered by the explicit tests above.
        #[test]
        fn price_per_gb_invariants(
            pool_bones in 1u64..=1_000_000_000_000_000,
            // rounding only ever leaves dust unallocated, so distributed <= pool.
            dust in 0u64..=100_000,
            total_bytes in 1u64..=u64::MAX,
            // a realistic token price (kept small so the Decimal math can't overflow).
            price_in_bones in 5_000_000u64..=20_000_000,
        ) {
            let pool = Decimal::from(pool_bones);
            let distributed = Decimal::from(pool_bones.saturating_sub(dust));
            let price_per_bone = PriceInfo::new(price_in_bones, 8).price_per_bone;

            let pool_rate = price_per_gb(pool, total_bytes, price_per_bone);
            let distributed_rate = price_per_gb(distributed, total_bytes, price_per_bone);

            prop_assert!(distributed_rate >= Decimal::ZERO);
            // distributed <= pool, so the realized rate never exceeds the full-pool rate.
            prop_assert!(distributed_rate <= pool_rate);
            // real money spread over real data -> a strictly positive rate.
            prop_assert!(pool_rate > Decimal::ZERO);
        }
    }
}
