use std::ops::Range;

use chrono::{DateTime, Utc};

use file_store::traits::TimestampEncode;
use rust_decimal::{Decimal, RoundingStrategy};
use rust_decimal_macros::dec;

use crate::reward_shares::dc_to_mobile_bones;

use super::{dc_sessions::ServiceProviderDCSessions, promotions::ServiceProviderPromotions};

mod proto {
    pub use helium_proto::{
        service_provider_promotions::Promotion,
        services::poc_mobile::{
            mobile_reward_share::Reward, MobileRewardShare, PromotionReward, ServiceProviderReward,
        },
    };
}

/// Container for all Service Provider rewarding
#[derive(Debug)]
pub struct ServiceProviderRewardInfos {
    coll: Vec<RewardInfo>,
    total_sp_allocation: Decimal,
    reward_epoch: Range<DateTime<Utc>>,
}

// Represents a single Service Providers information for rewarding,
// only used internally.
#[derive(Debug, Clone, PartialEq)]
struct RewardInfo {
    // proto::ServiceProvider enum repr
    sp_id: i32,

    // Total DC transferred for reward epoch in Bones
    bones: Decimal,
    // % of total allocated rewards for data transfer
    data_perc: Decimal,
    // % allocated from DC to promo rewards (found in db from file from on chain)
    allocated_promo_perc: Decimal,

    // % of total allocated rewards going towards promotions
    realized_promo_perc: Decimal,
    // % of total allocated rewards awarded for data transfer
    realized_data_perc: Decimal,
    // % matched promotions from unallocated, can never exceed realized_promo_perc
    matched_promo_perc: Decimal,

    // Active promotions for the epoch
    promotions: Vec<proto::Promotion>,
}

impl ServiceProviderRewardInfos {
    pub fn new(
        dc_sessions: ServiceProviderDCSessions,
        promotions: ServiceProviderPromotions,
        total_sp_allocation: Decimal, // Bones
        mobile_bone_price: Decimal,   // Price in Bones
        reward_epoch: Range<DateTime<Utc>>,
    ) -> Self {
        let all_transfer = dc_sessions.all_transfer(); // DC

        let mut me = Self {
            coll: vec![],
            total_sp_allocation,
            reward_epoch,
        };

        // After this point, we enter percentage land. This number is the basis
        // for all percentages, our 100%. If the DC transferred in Bones is
        // greater than the amount of Bones allocated for rewarding, we use the
        // greater number; to not exceed 100% allocation.
        //
        // When rewards are output, the percentages are taken from the allocated
        // Bones for service providers. Which has the effect of scaling the rewards.
        let used_allocation =
            total_sp_allocation.max(dc_to_mobile_bones(all_transfer, mobile_bone_price));

        for (service_provider, dc_transfer) in dc_sessions.iter() {
            let promo_fund_perc = promotions.get_fund_percent(service_provider);
            let promos = promotions.get_active_promotions(service_provider);

            me.coll.push(RewardInfo::new(
                service_provider,
                dc_to_mobile_bones(dc_transfer, mobile_bone_price),
                promo_fund_perc,
                used_allocation,
                promos,
            ));
        }

        me.coll.sort_by_key(|x| x.sp_id);

        distribute_unallocated(&mut me.coll);

        me
    }

    pub fn iter_rewards(&self) -> Vec<(u64, proto::MobileRewardShare)> {
        self.coll
            .iter()
            .flat_map(|sp| sp.iter_rewards(self.total_sp_allocation, &self.reward_epoch))
            .filter(|(amount, _r)| *amount > 0)
            .collect::<Vec<_>>()
    }
}

impl RewardInfo {
    fn new(
        sp_id: i32,
        bones_transfer: Decimal, // Bones
        promo_fund_perc: Decimal,
        total_sp_allocation: Decimal, // Bones
        promotions: Vec<proto::Promotion>,
    ) -> Self {
        let data_perc = bones_transfer / total_sp_allocation;
        let realized_promo_perc = if promotions.is_empty() {
            dec!(0)
        } else {
            data_perc * promo_fund_perc
        };
        let realized_dc_perc = data_perc - realized_promo_perc;

        Self {
            sp_id,
            bones: bones_transfer,
            allocated_promo_perc: promo_fund_perc,

            data_perc,
            realized_promo_perc,
            realized_data_perc: realized_dc_perc,
            matched_promo_perc: dec!(0),

            promotions,
        }
    }

    pub fn iter_rewards(
        &self,
        total_allocation: Decimal,
        reward_period: &Range<DateTime<Utc>>,
    ) -> Vec<(u64, proto::MobileRewardShare)> {
        let mut rewards = self.promo_rewards(total_allocation, reward_period);
        rewards.push(self.carrier_reward(total_allocation, reward_period));
        rewards
    }

    pub fn carrier_reward(
        &self,
        total_allocation: Decimal,
        reward_period: &Range<DateTime<Utc>>,
    ) -> (u64, proto::MobileRewardShare) {
        let amount = (total_allocation * self.realized_data_perc).to_u64_floored(); // Rewarded BONES

        (
            amount,
            proto::MobileRewardShare {
                start_period: reward_period.start.encode_timestamp(),
                end_period: reward_period.end.encode_timestamp(),
                reward: Some(proto::Reward::ServiceProviderReward(
                    proto::ServiceProviderReward {
                        service_provider_id: self.sp_id,
                        amount,
                    },
                )),
            },
        )
    }

    pub fn promo_rewards(
        &self,
        total_allocation: Decimal,
        reward_period: &Range<DateTime<Utc>>,
    ) -> Vec<(u64, proto::MobileRewardShare)> {
        if self.promotions.is_empty() {
            return vec![];
        }

        let mut rewards = vec![];

        let sp_amount = total_allocation * self.realized_promo_perc;
        let matched_amount = total_allocation * self.matched_promo_perc;

        let total_shares = self
            .promotions
            .iter()
            .map(|x| Decimal::from(x.shares))
            .sum::<Decimal>();
        let sp_amount_per_share = sp_amount / total_shares;
        let matched_amount_per_share = matched_amount / total_shares;

        for r in self.promotions.iter() {
            let shares = Decimal::from(r.shares);

            let service_provider_amount = (sp_amount_per_share * shares).to_u64_floored();
            let matched_amount = (matched_amount_per_share * shares).to_u64_floored();

            let total_amount = service_provider_amount + matched_amount;

            rewards.push((
                total_amount,
                proto::MobileRewardShare {
                    start_period: reward_period.start.encode_timestamp(),
                    end_period: reward_period.end.encode_timestamp(),
                    reward: Some(proto::Reward::PromotionReward(proto::PromotionReward {
                        service_provider_amount,
                        matched_amount,
                        entity: r.entity.to_owned(),
                    })),
                },
            ))
        }

        rewards
    }
}

fn distribute_unallocated(coll: &mut [RewardInfo]) {
    let allocated_perc = coll.iter().map(|x| x.data_perc).sum::<Decimal>();
    let unallocated_perc = dec!(1) - allocated_perc;

    let maybe_matching_perc = coll
        .iter()
        .filter(|x| !x.promotions.is_empty())
        .map(|x| x.realized_promo_perc)
        .sum::<Decimal>();

    if maybe_matching_perc > unallocated_perc {
        distribute_unalloc_over_limit(coll, unallocated_perc);
    } else {
        distribute_unalloc_under_limit(coll);
    }
}

fn distribute_unalloc_over_limit(coll: &mut [RewardInfo], unallocated_perc: Decimal) {
    // NOTE: This can also allocate based off the dc_perc of each carrier.
    let total = coll.iter().map(|x| x.realized_promo_perc).sum::<Decimal>() * dec!(100);

    for sp in coll.iter_mut() {
        if sp.promotions.is_empty() {
            continue;
        }
        let shares = sp.realized_promo_perc * dec!(100);
        sp.matched_promo_perc = (shares / total) * unallocated_perc;
    }
}

fn distribute_unalloc_under_limit(coll: &mut [RewardInfo]) {
    for sp in coll.iter_mut() {
        if sp.promotions.is_empty() {
            continue;
        }
        sp.matched_promo_perc = sp.realized_promo_perc
    }
}

trait DecimalRoundingExt {
    fn to_u64_floored(&self) -> u64;
}

impl DecimalRoundingExt for Decimal {
    fn to_u64_floored(&self) -> u64 {
        use rust_decimal::prelude::ToPrimitive;

        self.round_dp_with_strategy(0, RoundingStrategy::ToZero)
            .to_u64()
            .unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use chrono::Duration;
    use helium_proto::services::poc_mobile::{MobileRewardShare, PromotionReward};

    use crate::service_provider;

    use super::*;

    fn epoch() -> Range<DateTime<Utc>> {
        let now = Utc::now();
        now - Duration::hours(24)..now
    }

    #[test]
    fn no_promotions() {
        let sp_infos = ServiceProviderRewardInfos::new(
            ServiceProviderDCSessions::from([(0, dec!(12)), (1, dec!(6))]),
            ServiceProviderPromotions::default(),
            dec!(100),
            dec!(0.00001),
            epoch(),
        );

        let mut iter = sp_infos.iter_rewards().into_iter();

        let sp_1 = iter.next().unwrap().1.sp_reward();
        let sp_2 = iter.next().unwrap().1.sp_reward();

        assert_eq!(sp_1.amount, 12);
        assert_eq!(sp_2.amount, 6);

        assert_eq!(None, iter.next());
    }

    #[test]
    fn unallocated_reward_scaling_1() {
        let sp_infos = ServiceProviderRewardInfos::new(
            ServiceProviderDCSessions::from([(0, dec!(12)), (1, dec!(6))]),
            ServiceProviderPromotions::from(vec![
                make_test_promotion(0, "promo-0", 5000, 1),
                make_test_promotion(1, "promo-1", 5000, 1),
            ]),
            dec!(100),
            dec!(0.00001),
            epoch(),
        );

        let (promo_1, sp_1) = sp_infos.single_sp_rewards(0);
        assert_eq!(promo_1.service_provider_amount, 6);
        assert_eq!(promo_1.matched_amount, 6);
        assert_eq!(sp_1.amount, 6);

        let (promo_2, sp_2) = sp_infos.single_sp_rewards(1);
        assert_eq!(promo_2.service_provider_amount, 3);
        assert_eq!(promo_2.matched_amount, 3);
        assert_eq!(sp_2.amount, 3);
    }

    #[test]
    fn unallocated_reward_scaling_2() {
        let sp_infos = ServiceProviderRewardInfos::new(
            ServiceProviderDCSessions::from([(0, dec!(12)), (1, dec!(6))]),
            ServiceProviderPromotions::from(vec![
                helium_proto::ServiceProviderPromotions {
                    service_provider: 0,
                    incentive_escrow_fund_bps: 5000,
                    promotions: vec![helium_proto::service_provider_promotions::Promotion {
                        entity: "promo-0".to_string(),
                        shares: 1,
                        ..Default::default()
                    }],
                },
                helium_proto::ServiceProviderPromotions {
                    service_provider: 1,
                    incentive_escrow_fund_bps: 10000,
                    promotions: vec![helium_proto::service_provider_promotions::Promotion {
                        entity: "promo-1".to_string(),
                        shares: 1,
                        ..Default::default()
                    }],
                },
            ]),
            dec!(100),
            dec!(0.00001),
            epoch(),
        );

        let (promo_1, sp_1) = sp_infos.single_sp_rewards(0);
        assert_eq!(promo_1.service_provider_amount, 6);
        assert_eq!(promo_1.matched_amount, 6);
        assert_eq!(sp_1.amount, 6);

        let (promo_2, sp_2) = sp_infos.single_sp_rewards(1);
        assert_eq!(promo_2.service_provider_amount, 6);
        assert_eq!(promo_2.matched_amount, 6);
        assert_eq!(sp_2.amount, 0);
    }

    #[test]
    fn unallocated_reward_scaling_3() {
        let sp_infos = ServiceProviderRewardInfos::new(
            ServiceProviderDCSessions::from([(0, dec!(10)), (1, dec!(1000))]),
            ServiceProviderPromotions::from(vec![
                make_test_promotion(0, "promo-0", 10000, 1),
                make_test_promotion(1, "promo-1", 200, 1),
            ]),
            dec!(2000),
            dec!(0.00001),
            epoch(),
        );

        let (promo_1, sp_1) = sp_infos.single_sp_rewards(0);
        assert_eq!(promo_1.service_provider_amount, 10);
        assert_eq!(promo_1.matched_amount, 10);
        assert_eq!(sp_1.amount, 0);

        let (promo_2, sp_2) = sp_infos.single_sp_rewards(1);
        assert_eq!(promo_2.service_provider_amount, 20);
        assert_eq!(promo_2.matched_amount, 20);
        assert_eq!(sp_2.amount, 980);
    }

    #[test]
    fn no_rewards_if_none_allocated() {
        let sp_infos = ServiceProviderRewardInfos::new(
            ServiceProviderDCSessions::from([(0, dec!(100))]),
            ServiceProviderPromotions::from(vec![make_test_promotion(0, "promo-0", 5000, 1)]),
            dec!(0),
            dec!(0.0001),
            epoch(),
        );

        assert!(sp_infos.iter_rewards().is_empty());
    }

    #[test]
    fn no_matched_rewards_if_no_unallocated() {
        let total_rewards = dec!(1000);

        let sp_infos = ServiceProviderRewardInfos::new(
            ServiceProviderDCSessions::from([(0, dec!(5000))]),
            ServiceProviderPromotions::from(vec![make_test_promotion(0, "promo-0", 5000, 1)]),
            total_rewards,
            dec!(0.00001),
            epoch(),
        );

        let promo_rewards = sp_infos.iter_rewards().only_promotion_rewards();

        assert!(!promo_rewards.is_empty());
        for reward in promo_rewards {
            assert_eq!(reward.matched_amount, 0);
        }
    }

    #[test]
    fn single_sp_unallocated_less_than_matched_distributed_by_shares() {
        // 100 unallocated
        let total_rewards = dec!(1100);
        let sp_session = dec!(1000);

        let sp_infos = ServiceProviderRewardInfos::new(
            ServiceProviderDCSessions::from([(0, sp_session)]),
            ServiceProviderPromotions::from(vec![helium_proto::ServiceProviderPromotions {
                service_provider: 0,
                incentive_escrow_fund_bps: 10000,
                promotions: vec![
                    helium_proto::service_provider_promotions::Promotion {
                        entity: "promo-0".to_string(),
                        shares: 1,
                        ..Default::default()
                    },
                    helium_proto::service_provider_promotions::Promotion {
                        entity: "promo-1".to_string(),
                        shares: 2,
                        ..Default::default()
                    },
                ],
            }]),
            total_rewards,
            dec!(0.00001),
            epoch(),
        );

        let promo_rewards = sp_infos.iter_rewards().only_promotion_rewards();
        assert_eq!(2, promo_rewards.len());

        assert_eq!(promo_rewards[0].service_provider_amount, 333);
        assert_eq!(promo_rewards[0].matched_amount, 33);
        //
        assert_eq!(promo_rewards[1].service_provider_amount, 666);
        assert_eq!(promo_rewards[1].matched_amount, 66);
    }

    #[test]
    fn single_sp_unallocated_more_than_matched_promotion() {
        // 1,000 unallocated
        let total_rewards = dec!(11_000);
        let sp_session = dec!(1000);

        let sp_infos = ServiceProviderRewardInfos::new(
            ServiceProviderDCSessions::from([(0, sp_session)]),
            ServiceProviderPromotions::from(vec![helium_proto::ServiceProviderPromotions {
                service_provider: 0,
                incentive_escrow_fund_bps: 10000,
                promotions: vec![
                    helium_proto::service_provider_promotions::Promotion {
                        entity: "promo-0".to_string(),
                        shares: 1,
                        ..Default::default()
                    },
                    helium_proto::service_provider_promotions::Promotion {
                        entity: "promo-1".to_string(),
                        shares: 2,
                        ..Default::default()
                    },
                ],
            }]),
            total_rewards,
            dec!(0.00001),
            epoch(),
        );

        let promo_rewards = sp_infos.iter_rewards().only_promotion_rewards();
        assert_eq!(2, promo_rewards.len());

        assert_eq!(promo_rewards[0].service_provider_amount, 333);
        assert_eq!(promo_rewards[0].matched_amount, 333);
        //
        assert_eq!(promo_rewards[1].service_provider_amount, 666);
        assert_eq!(promo_rewards[1].matched_amount, 666);
    }

    #[test]
    fn unallocated_matching_does_not_exceed_promotion() {
        // 100 unallocated
        let total_rewards = dec!(1100);
        let sp_session = dec!(1000);

        let sp_infos = ServiceProviderRewardInfos::new(
            ServiceProviderDCSessions::from([(0, sp_session)]),
            ServiceProviderPromotions::from(vec![helium_proto::ServiceProviderPromotions {
                service_provider: 0,
                incentive_escrow_fund_bps: 100, // severely limit promotions
                promotions: vec![
                    helium_proto::service_provider_promotions::Promotion {
                        entity: "promo-0".to_string(),
                        shares: 1,
                        ..Default::default()
                    },
                    helium_proto::service_provider_promotions::Promotion {
                        entity: "promo-1".to_string(),
                        shares: 2,
                        ..Default::default()
                    },
                ],
            }]),
            total_rewards,
            dec!(0.00001),
            epoch(),
        );

        let promo_rewards = sp_infos.iter_rewards().only_promotion_rewards();
        assert_eq!(2, promo_rewards.len());

        assert_eq!(promo_rewards[0].service_provider_amount, 3);
        assert_eq!(promo_rewards[0].matched_amount, 3);
        //
        assert_eq!(promo_rewards[1].service_provider_amount, 6);
        assert_eq!(promo_rewards[1].matched_amount, 6);
    }

    #[test]
    fn no_matched_promotions_full_bucket_allocation() {
        // The service providers DC session represents _more_ than the
        // available amount of sp_rewards for an epoch.
        // No matching on promotions should occur.
        let total_rewards = dec!(8_219_178_082_191);
        let sp_session = dec!(553_949_301);

        let sp_infos = ServiceProviderRewardInfos::new(
            ServiceProviderDCSessions::from([(0, sp_session)]),
            ServiceProviderPromotions::from(vec![helium_proto::ServiceProviderPromotions {
                service_provider: 0,
                incentive_escrow_fund_bps: 396, // severely limit promotions
                promotions: vec![helium_proto::service_provider_promotions::Promotion {
                    entity: "promo-1".to_string(),
                    shares: 100,
                    ..Default::default()
                }],
            }]),
            total_rewards,
            dec!(629) / dec!(1_000_000) / dec!(1_000_000),
            epoch(),
        );

        let (promo_1, sp_1) = sp_infos.single_sp_rewards(0);
        assert_eq!(promo_1.service_provider_amount, 325_479_452_054);
        assert_eq!(promo_1.matched_amount, 0);
        assert_eq!(sp_1.amount, 7_893_698_630_136);

        let mut unallocated = total_rewards.to_u64_floored();
        for (amount, _reward) in sp_infos.iter_rewards() {
            unallocated -= amount;
        }

        assert_eq!(unallocated, 1);
    }

    #[test]
    fn no_matched_promotions_multiple_sp_full_bucket_allocation() {
        // The Service Providers DC sessions far surpass the
        // available amount of sp_rewards for an epoch.
        // No matching on promotions should occur.
        let total_rewards = dec!(8_219_178_082_191);
        let sp_session = dec!(553_949_301);

        let sp_infos = ServiceProviderRewardInfos::new(
            ServiceProviderDCSessions::from([(0, sp_session), (1, sp_session)]),
            ServiceProviderPromotions::from(vec![
                helium_proto::ServiceProviderPromotions {
                    service_provider: 0,
                    incentive_escrow_fund_bps: 396,
                    promotions: vec![helium_proto::service_provider_promotions::Promotion {
                        entity: "promo-1".to_string(),
                        shares: 100,
                        ..Default::default()
                    }],
                },
                helium_proto::ServiceProviderPromotions {
                    service_provider: 1,
                    incentive_escrow_fund_bps: 400,
                    promotions: vec![helium_proto::service_provider_promotions::Promotion {
                        entity: "promo-2".to_string(),
                        shares: 100,
                        ..Default::default()
                    }],
                },
            ]),
            total_rewards,
            dec!(629) / dec!(1_000_000) / dec!(1_000_000),
            epoch(),
        );

        let sp_base_reward = dec!(4_109_589_041_095.50);
        let sp_1_promotion_bones = dec!(162_739_726_027.38); // 3.96%
        let sp_2_promotion_bones = dec!(164_383_561_643.82); // 4.00%

        let (promo_1, sp_1) = sp_infos.single_sp_rewards(0);
        assert_eq!(
            sp_1.amount,
            (sp_base_reward - sp_1_promotion_bones).to_u64_floored()
        );
        assert_eq!(
            promo_1.service_provider_amount,
            sp_1_promotion_bones.to_u64_floored()
        );
        assert_eq!(promo_1.matched_amount, 0);

        let (promo_2, sp_2) = sp_infos.single_sp_rewards(1);
        assert_eq!(
            sp_2.amount,
            (sp_base_reward - sp_2_promotion_bones).to_u64_floored()
        );
        assert_eq!(
            promo_2.service_provider_amount,
            sp_2_promotion_bones.to_u64_floored()
        );
        assert_eq!(promo_2.matched_amount, 0);

        let mut unallocated = total_rewards.to_u64_floored();
        for (amount, _reward) in sp_infos.iter_rewards() {
            unallocated -= amount;
        }
        assert_eq!(unallocated, 2);
    }

    use proptest::prelude::*;

    prop_compose! {
        fn arb_promotion()(entity: String, shares in 1..=100u32) -> helium_proto::service_provider_promotions::Promotion {
            proto::Promotion { entity, shares, ..Default::default() }
        }
    }

    prop_compose! {
        fn arb_sp_promotion()(
            sp_id in 0..10_i32,
            bps in arb_bps(),
            promotions in prop::collection::vec(arb_promotion(), 0..10)
        ) -> helium_proto::ServiceProviderPromotions {
            helium_proto::ServiceProviderPromotions {
                service_provider: sp_id,
                incentive_escrow_fund_bps: bps,
                promotions
            }
        }
    }

    prop_compose! {
        fn arb_bps()(bps in 0..=10_000u32) -> u32 { bps }
    }

    prop_compose! {
        fn arb_dc_session()(
            sp_id in 0..10_i32,
            // below 1 trillion
            dc_session in (0..=1_000_000_000_000_u64).prop_map(Decimal::from)
        ) -> (i32, Decimal) {
            (sp_id, dc_session)
        }
    }

    proptest! {
        // #![proptest_config(ProptestConfig::with_cases(100_000))]

        #[test]
        fn single_provider_does_not_overallocate(
            dc_session in any::<u64>().prop_map(Decimal::from),
            promotions in prop::collection::vec(arb_sp_promotion(), 0..10),
            total_allocation in any::<u64>().prop_map(Decimal::from)
        ) {

            let sp_infos = ServiceProviderRewardInfos::new(
                ServiceProviderDCSessions::from([(0, dc_session)]),
                ServiceProviderPromotions::from(promotions),
                total_allocation,
                dec!(0.00001),
                epoch()
            );

            let total_perc= sp_infos.total_percent();
            assert!(total_perc <= dec!(1));

            let mut allocated = dec!(0);
            for (amount, _) in sp_infos.iter_rewards() {
                allocated += Decimal::from(amount);
            }
            assert!(allocated <= total_allocation);
        }


        #[test]
        fn multiple_provider_does_not_overallocate(
            dc_sessions in prop::collection::vec(arb_dc_session(), 0..10),
            promotions in prop::collection::vec(arb_sp_promotion(), 0..10),
            mobile_bone_price in 1..5000
        ) {
            let epoch = epoch();
            let total_allocation = service_provider::get_scheduled_tokens(&epoch);

            let sp_infos = ServiceProviderRewardInfos::new(
                ServiceProviderDCSessions::from(dc_sessions),
                ServiceProviderPromotions::from(promotions),
                total_allocation,
                Decimal::from(mobile_bone_price) / dec!(1_000_000) / dec!(1_000_000),
                epoch
            );

            // NOTE: This can be a sanity check when debugging. There are cases
            // generated where the total percentage is
            // 1.0000000000000000000000000001%, but as long as we don't
            // allocated more than what is available, this is okay.

            // let total_perc = sp_infos.total_percent();
            // println!("total_perc: {}", total_perc);
            // prop_assert!(total_perc <= dec!(1));

            let mut allocated = dec!(0);
            for (amount, _) in sp_infos.iter_rewards() {
                allocated += Decimal::from(amount);
            }
            prop_assert!(allocated <= total_allocation);
        }

    }

    trait RewardExt {
        fn promotion_reward(self) -> proto::PromotionReward;
        fn sp_reward(self) -> proto::ServiceProviderReward;
    }

    impl RewardExt for proto::MobileRewardShare {
        fn promotion_reward(self) -> proto::PromotionReward {
            match self.reward {
                Some(proto::Reward::PromotionReward(promo)) => promo.clone(),
                other => panic!("expected promotion reward, got {other:?}"),
            }
        }

        fn sp_reward(self) -> proto::ServiceProviderReward {
            match self.reward {
                Some(proto::Reward::ServiceProviderReward(promo)) => promo.clone(),
                other => panic!("expected sp reward, got {other:?}"),
            }
        }
    }

    trait PromoRewardFiltersExt {
        fn only_promotion_rewards(&self) -> Vec<PromotionReward>;
    }

    impl PromoRewardFiltersExt for Vec<(u64, MobileRewardShare)> {
        fn only_promotion_rewards(&self) -> Vec<PromotionReward> {
            self.clone()
                .into_iter()
                .filter_map(|(_, r)| {
                    if let Some(proto::Reward::PromotionReward(reward)) = r.reward {
                        Some(reward)
                    } else {
                        None
                    }
                })
                .collect()
        }
    }

    impl RewardInfo {
        fn total_percent(&self) -> Decimal {
            self.realized_data_perc + self.realized_promo_perc + self.matched_promo_perc
        }
    }

    impl ServiceProviderRewardInfos {
        fn total_percent(&self) -> Decimal {
            self.coll.iter().map(|x| x.total_percent()).sum()
        }

        fn iter_sp_rewards(&self, sp_id: i32) -> Vec<MobileRewardShare> {
            for info in self.coll.iter() {
                if info.sp_id == sp_id {
                    return info
                        .iter_rewards(self.total_sp_allocation, &self.reward_epoch)
                        .into_iter()
                        .map(|(_, x)| x)
                        .collect();
                }
            }
            vec![]
        }

        fn single_sp_rewards(
            &self,
            sp_id: i32,
        ) -> (proto::PromotionReward, proto::ServiceProviderReward) {
            let binding = self.iter_sp_rewards(sp_id);
            let mut rewards = binding.iter();

            let promo = rewards.next().cloned().unwrap().promotion_reward();
            let sp = rewards.next().cloned().unwrap().sp_reward();

            (promo, sp)
        }
    }

    fn make_test_promotion(
        sp_id: i32,
        entity: &str,
        incentive_escrow_fund_bps: u32,
        shares: u32,
    ) -> helium_proto::ServiceProviderPromotions {
        helium_proto::ServiceProviderPromotions {
            service_provider: sp_id,
            incentive_escrow_fund_bps,
            promotions: vec![helium_proto::service_provider_promotions::Promotion {
                entity: entity.to_string(),
                start_ts: Utc::now().encode_timestamp_millis(),
                end_ts: Utc::now().encode_timestamp_millis(),
                shares,
            }],
        }
    }
}
