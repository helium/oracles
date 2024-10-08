use std::ops::Range;

use chrono::{DateTime, Utc};

use file_store::traits::TimestampEncode;
use rust_decimal::{Decimal, RoundingStrategy};
use rust_decimal_macros::dec;

use crate::reward_shares::{dc_to_mobile_bones, DEFAULT_PREC};

use super::{
    dc_sessions::ServiceProviderDCSessions,
    promotions::{funds::ServiceProviderFunds, rewards::ServiceProviderPromotions},
};

mod proto {
    pub use helium_proto::services::poc_mobile::{
        mobile_reward_share::Reward, MobileRewardShare, ServiceProviderReward,
    };
}

pub fn rewards_per_share(
    total_sp_dc: Decimal,
    total_sp_rewards: Decimal,
    mobile_bone_price: Decimal,
) -> Decimal {
    let total_sp_rewards_used = dc_to_mobile_bones(total_sp_dc, mobile_bone_price);
    let capped_sp_rewards_used = total_sp_rewards_used.min(total_sp_rewards);

    if capped_sp_rewards_used > Decimal::ZERO {
        (capped_sp_rewards_used / total_sp_dc)
            .round_dp_with_strategy(DEFAULT_PREC, RoundingStrategy::MidpointNearestEven)
    } else {
        Decimal::ZERO
    }
}

/// Container for all Service Provider rewarding
#[derive(Debug)]
pub struct ServiceProviderRewardInfos {
    coll: Vec<RewardInfo>,
    total_sp_allocation: Decimal,
    all_transfer: Decimal,
    mobile_bone_price: Decimal,
    reward_epoch: Range<DateTime<Utc>>,
}

// Represents a single Service Providers information for rewarding,
// only used internally.
#[derive(Debug, Clone, PartialEq)]
struct RewardInfo {
    // proto::ServiceProvider enum repr
    sp_id: i32,

    // Total DC transferred for reward epoch
    dc: Decimal,
    // % of total allocated rewards for dc transfer
    dc_perc: Decimal,
    // % allocated from DC to promo rewards (found in db from file from on chain)
    allocated_promo_perc: Decimal,

    // % of total allocated rewards going towards promotions
    realized_promo_perc: Decimal,
    // % of total allocated rewards awarded for dc transfer
    realized_dc_perc: Decimal,
    // % matched promotions from unallocated, can never exceed realized_promo_perc
    matched_promo_perc: Decimal,

    // Rewards for the epoch
    promotion_rewards: ServiceProviderPromotions,
}

impl ServiceProviderRewardInfos {
    pub fn new(
        dc_sessions: ServiceProviderDCSessions,
        promo_funds: ServiceProviderFunds,
        rewards: ServiceProviderPromotions,
        total_sp_allocation: Decimal,
        mobile_bone_price: Decimal,
        reward_epoch: Range<DateTime<Utc>>,
    ) -> Self {
        let all_transfer = dc_sessions.all_transfer();

        let mut me = Self {
            coll: vec![],
            all_transfer,
            total_sp_allocation,
            mobile_bone_price,
            reward_epoch,
        };

        let used_allocation = total_sp_allocation.max(all_transfer);
        for (dc_session, dc_transfer) in dc_sessions.iter() {
            let promo_fund_perc = promo_funds.get_fund_percent(dc_session);
            me.coll.push(RewardInfo::new(
                dc_session,
                dc_transfer,
                promo_fund_perc,
                used_allocation,
                rewards.for_service_provider(dc_session),
            ));
        }

        distribute_unallocated(&mut me.coll);

        me
    }

    pub fn iter_rewards(&self) -> Vec<(u64, proto::MobileRewardShare)> {
        let rewards_per_share = rewards_per_share(
            self.all_transfer,
            self.total_sp_allocation,
            self.mobile_bone_price,
        );
        let sp_rewards = self.total_sp_allocation * rewards_per_share;
        // NOTE(mj): `rewards_per_share * self.dc` vs `sp_rewards * self.dc_perc`
        // They're veeeeery close. But the % multiplication produces a floating point number
        // that will typically be rounded down.

        self.coll
            .iter()
            .flat_map(|sp| {
                let mut rewards = sp.promo_rewards(sp_rewards, &self.reward_epoch);
                rewards.push(sp.carrier_reward(sp_rewards, &self.reward_epoch));
                rewards
            })
            .filter(|(amount, _r)| *amount > 0)
            .collect()
    }
}

impl RewardInfo {
    pub fn new(
        sp_id: i32,
        dc_transfer: Decimal,
        promo_fund_perc: Decimal,
        total_sp_allocation: Decimal,
        rewards: ServiceProviderPromotions,
    ) -> Self {
        let dc_perc = dc_transfer / total_sp_allocation;
        let realized_promo_perc = if rewards.is_empty() {
            dec!(0)
        } else {
            dc_perc * promo_fund_perc
        };
        let realized_dc_perc = dc_perc - realized_promo_perc;

        Self {
            sp_id,
            dc: dc_transfer,
            allocated_promo_perc: promo_fund_perc,

            dc_perc,
            realized_promo_perc,
            realized_dc_perc,
            matched_promo_perc: dec!(0),

            promotion_rewards: rewards,
        }
    }

    pub fn carrier_reward(
        &self,
        total_allocation: Decimal,
        reward_period: &Range<DateTime<Utc>>,
    ) -> (u64, proto::MobileRewardShare) {
        let amount = (total_allocation * self.realized_dc_perc).to_u64_rounded();

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
        _total_allocation: Decimal,
        _reward_period: &Range<DateTime<Utc>>,
    ) -> Vec<(u64, proto::MobileRewardShare)> {
        vec![]
        // if self.promotion_rewards.is_empty() {
        //     return vec![];
        // }

        // let mut rewards = vec![];

        // let sp_amount = total_allocation * self.realized_promo_perc;
        // let matched_amount = total_allocation * self.matched_promo_perc;

        // let total_shares = self.promotion_rewards.total_shares();
        // let sp_amount_per_share = sp_amount / total_shares;
        // let matched_amount_per_share = matched_amount / total_shares;

        // for r in self.promotion_rewards.iter() {
        //     let shares = Decimal::from(r.shares);

        //     let service_provider_amount = (sp_amount_per_share * shares).to_u64_rounded();
        //     let matched_amount = (matched_amount_per_share * shares).to_u64_rounded();

        //     let total_amount = service_provider_amount + matched_amount;

        //     rewards.push((
        //         total_amount,
        //         proto::MobileRewardShare {
        //             start_period: reward_period.start.encode_timestamp(),
        //             end_period: reward_period.end.encode_timestamp(),
        //             reward: Some(proto::Reward::PromotionReward(proto::PromotionReward {
        //                 service_provider_amount,
        //                 matched_amount,
        //                 entity: Some(r.rewardable_entity.clone().into()),
        //             })),
        //         },
        //     ))
        // }

        // rewards
    }
}

fn distribute_unallocated(coll: &mut [RewardInfo]) {
    let allocated_perc = coll.iter().map(|x| x.dc_perc).sum::<Decimal>();
    let unallocated_perc = dec!(1) - allocated_perc;

    let maybe_matching_perc = coll
        .iter()
        .filter(|x| !x.promotion_rewards.is_empty())
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
        if sp.promotion_rewards.is_empty() {
            continue;
        }
        let shares = sp.realized_promo_perc * dec!(100);
        sp.matched_promo_perc = (shares / total) * unallocated_perc;
    }
}

fn distribute_unalloc_under_limit(coll: &mut [RewardInfo]) {
    for sp in coll.iter_mut() {
        if sp.promotion_rewards.is_empty() {
            continue;
        }
        sp.matched_promo_perc = sp.realized_promo_perc
    }
}

trait DecimalRoundingExt {
    fn to_u64_rounded(&self) -> u64;
}

impl DecimalRoundingExt for Decimal {
    fn to_u64_rounded(&self) -> u64 {
        use rust_decimal::{prelude::ToPrimitive, RoundingStrategy};

        self.round_dp_with_strategy(0, RoundingStrategy::ToZero)
            .to_u64()
            .unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use chrono::Duration;
    use file_store::promotion_reward::Entity;
    use helium_proto::services::poc_mobile::MobileRewardShare;

    use crate::service_provider::{self, promotions::rewards::PromotionRewardShare};

    use super::*;

    use super::ServiceProviderRewardInfos;

    fn epoch() -> Range<DateTime<Utc>> {
        let now = Utc::now();
        now - Duration::hours(24)..now
    }

    impl ServiceProviderRewardInfos {
        fn iter_sp_rewards(&self, sp_id: i32) -> Vec<MobileRewardShare> {
            let rewards_per_share = rewards_per_share(
                self.all_transfer,
                self.total_sp_allocation,
                self.mobile_bone_price,
            );
            let sp_rewards = self.total_sp_allocation * rewards_per_share;

            for info in self.coll.iter() {
                if info.sp_id == sp_id {
                    let mut result = info.promo_rewards(sp_rewards, &self.reward_epoch);
                    result.push(info.carrier_reward(sp_rewards, &self.reward_epoch));
                    return result.into_iter().map(|(_, x)| x).collect();
                }
            }
            vec![]
        }

        fn single_sp_rewards(&self, sp_id: i32) -> proto::ServiceProviderReward {
            let binding = self.iter_sp_rewards(sp_id);
            let mut rewards = binding.iter();
            rewards.next().cloned().unwrap().sp_reward()
        }

        // fn single_sp_rewards(
        //     &self,
        //     sp_id: i32,
        // ) -> (proto::PromotionReward, proto::ServiceProviderReward) {
        //     let binding = self.iter_sp_rewards(sp_id);
        //     let mut rewards = binding.iter();

        //     let promo = rewards.next().cloned().unwrap().promotion_reward();
        //     let sp = rewards.next().cloned().unwrap().sp_reward();

        //     (promo, sp)
        // }
    }

    trait RewardExt {
        // fn promotion_reward(self) -> proto::PromotionReward;
        fn sp_reward(self) -> proto::ServiceProviderReward;
    }

    impl RewardExt for proto::MobileRewardShare {
        // fn promotion_reward(self) -> proto::PromotionReward {
        //     match self.reward {
        //         Some(proto::Reward::PromotionReward(promo)) => promo.clone(),
        //         other => panic!("expected promotion reward, got {other:?}"),
        //     }
        // }

        fn sp_reward(self) -> proto::ServiceProviderReward {
            match self.reward {
                Some(proto::Reward::ServiceProviderReward(promo)) => promo.clone(),
                other => panic!("expected sp reward, got {other:?}"),
            }
        }
    }

    #[test]
    fn unallocated_reward_scaling_1() {
        let sp_infos = ServiceProviderRewardInfos::new(
            ServiceProviderDCSessions::from([(0, dec!(12)), (1, dec!(6))]),
            ServiceProviderFunds::from([(0, 5000), (1, 5000)]),
            ServiceProviderPromotions::from([
                PromotionRewardShare {
                    service_provider_id: 0,
                    rewardable_entity: Entity::SubscriberId(vec![0]),
                    shares: 1,
                },
                PromotionRewardShare {
                    service_provider_id: 1,
                    rewardable_entity: Entity::SubscriberId(vec![1]),
                    shares: 1,
                },
            ]),
            dec!(100),
            dec!(0.00001),
            epoch(),
        );

        let sp_1 = sp_infos.single_sp_rewards(0);
        assert_eq!(sp_1.amount, 6);

        let sp_2 = sp_infos.single_sp_rewards(1);
        assert_eq!(sp_2.amount, 3);

        // let (promo_1, sp_1) = sp_infos.single_sp_rewards(0);
        // assert_eq!(promo_1.service_provider_amount, 6);
        // assert_eq!(promo_1.matched_amount, 6);
        // assert_eq!(sp_1.amount, 6);

        // let (promo_2, sp_2) = sp_infos.single_sp_rewards(1);
        // assert_eq!(promo_2.service_provider_amount, 3);
        // assert_eq!(promo_2.matched_amount, 3);
        // assert_eq!(sp_2.amount, 3);
    }

    #[test]
    fn unallocated_reward_scaling_2() {
        let sp_infos = ServiceProviderRewardInfos::new(
            ServiceProviderDCSessions::from([(0, dec!(12)), (1, dec!(6))]),
            ServiceProviderFunds::from([(0, 5000), (1, 10000)]),
            ServiceProviderPromotions::from([
                PromotionRewardShare {
                    service_provider_id: 0,
                    rewardable_entity: Entity::SubscriberId(vec![0]),
                    shares: 1,
                },
                PromotionRewardShare {
                    service_provider_id: 1,
                    rewardable_entity: Entity::SubscriberId(vec![1]),
                    shares: 1,
                },
            ]),
            dec!(100),
            dec!(0.00001),
            epoch(),
        );

        let sp_1 = sp_infos.single_sp_rewards(0);
        assert_eq!(sp_1.amount, 6);

        let sp_2 = sp_infos.single_sp_rewards(1);
        assert_eq!(sp_2.amount, 0);
        

        // let (promo_1, sp_1) = sp_infos.single_sp_rewards(0);
        // assert_eq!(promo_1.service_provider_amount, 6);
        // assert_eq!(promo_1.matched_amount, 6);
        // assert_eq!(sp_1.amount, 6);

        // let (promo_2, sp_2) = sp_infos.single_sp_rewards(1);
        // assert_eq!(promo_2.service_provider_amount, 6);
        // assert_eq!(promo_2.matched_amount, 6);
        // assert_eq!(sp_2.amount, 0);
    }

    #[test]
    fn unallocated_reward_scaling_3() {
        let sp_infos = ServiceProviderRewardInfos::new(
            ServiceProviderDCSessions::from([(0, dec!(10)), (1, dec!(1000))]),
            ServiceProviderFunds::from([(0, 10000), (1, 200)]),
            ServiceProviderPromotions::from([
                PromotionRewardShare {
                    service_provider_id: 0,
                    rewardable_entity: Entity::SubscriberId(vec![0]),
                    shares: 1,
                },
                PromotionRewardShare {
                    service_provider_id: 1,
                    rewardable_entity: Entity::SubscriberId(vec![1]),
                    shares: 1,
                },
            ]),
            dec!(2000),
            dec!(0.00001),
            epoch(),
        );

        let sp_1 = sp_infos.single_sp_rewards(0);
        assert_eq!(sp_1.amount, 0);

        let sp_2 = sp_infos.single_sp_rewards(1);
        assert_eq!(sp_2.amount, 980);

        // let (promo_1, sp_1) = sp_infos.single_sp_rewards(0);
        // assert_eq!(promo_1.service_provider_amount, 10);
        // assert_eq!(promo_1.matched_amount, 10);
        // assert_eq!(sp_1.amount, 0);

        // let (promo_2, sp_2) = sp_infos.single_sp_rewards(1);
        // assert_eq!(promo_2.service_provider_amount, 20);
        // assert_eq!(promo_2.matched_amount, 20);
        // assert_eq!(sp_2.amount, 980);
    }

    #[test]
    fn no_rewards_if_none_allocated() {
        let sp_infos = ServiceProviderRewardInfos::new(
            ServiceProviderDCSessions::from([(0, dec!(100))]),
            ServiceProviderFunds::from([(0, 5000)]),
            ServiceProviderPromotions::from([PromotionRewardShare {
                service_provider_id: 0,
                rewardable_entity: Entity::SubscriberId(vec![0]),
                shares: 1,
            }]),
            dec!(0),
            dec!(0.0001),
            epoch(),
        );

        assert!(sp_infos.iter_rewards().is_empty());
    }

    // #[test]
    // fn no_matched_rewards_if_no_unallocated() {
    //     let total_rewards = dec!(1000);

    //     let sp_infos = ServiceProviderRewardInfos::new(
    //         ServiceProviderDCSessions::from([(0, total_rewards)]),
    //         ServiceProviderFunds::from([(0, 5000)]),
    //         ServiceProviderPromotions::from([PromotionRewardShare {
    //             service_provider_id: 0,
    //             rewardable_entity: Entity::SubscriberId(vec![0]),
    //             shares: 1,
    //         }]),
    //         total_rewards,
    //         dec!(0.001),
    //         epoch(),
    //     );

    //     let promo_rewards = sp_infos.iter_rewards().only_promotion_rewards();

    //     assert!(!promo_rewards.is_empty());
    //     for reward in promo_rewards {
    //         assert_eq!(reward.matched_amount, 0);
    //     }
    // }

    // #[test]
    // fn single_sp_unallocated_less_than_matched_distributed_by_shares() {
    //     // 100 unallocated
    //     let total_rewards = dec!(1100);
    //     let sp_session = dec!(1000);

    //     let sp_infos = ServiceProviderRewardInfos::new(
    //         ServiceProviderDCSessions::from([(0, sp_session)]),
    //         ServiceProviderFunds::from([(0, 10000)]), // All rewards allocated to promotions
    //         ServiceProviderPromotions::from([
    //             PromotionRewardShare {
    //                 service_provider_id: 0,
    //                 rewardable_entity: Entity::SubscriberId(vec![0]),
    //                 shares: 1,
    //             },
    //             PromotionRewardShare {
    //                 service_provider_id: 0,
    //                 rewardable_entity: Entity::SubscriberId(vec![1]),
    //                 shares: 2,
    //             },
    //         ]),
    //         total_rewards,
    //         dec!(0.00001),
    //         epoch(),
    //     );

    //     let promo_rewards = sp_infos.iter_rewards().only_promotion_rewards();
    //     assert_eq!(2, promo_rewards.len());

    //     assert_eq!(promo_rewards[0].service_provider_amount, 333);
    //     assert_eq!(promo_rewards[0].matched_amount, 33);
    //     //
    //     assert_eq!(promo_rewards[1].service_provider_amount, 666);
    //     assert_eq!(promo_rewards[1].matched_amount, 66);
    // }

    // #[test]
    // fn single_sp_unallocated_more_than_matched_promotion() {
    //     // 1,000 unallocated
    //     let total_rewards = dec!(11_000);
    //     let sp_session = dec!(1000);

    //     let sp_infos = ServiceProviderRewardInfos::new(
    //         ServiceProviderDCSessions::from([(0, sp_session)]),
    //         ServiceProviderFunds::from([(0, 10000)]), // All rewards allocated to promotions
    //         ServiceProviderPromotions::from([
    //             PromotionRewardShare {
    //                 service_provider_id: 0,
    //                 rewardable_entity: Entity::SubscriberId(vec![0]),
    //                 shares: 1,
    //             },
    //             PromotionRewardShare {
    //                 service_provider_id: 0,
    //                 rewardable_entity: Entity::SubscriberId(vec![1]),
    //                 shares: 2,
    //             },
    //         ]),
    //         total_rewards,
    //         dec!(0.00001),
    //         epoch(),
    //     );

    //     let promo_rewards = sp_infos.iter_rewards().only_promotion_rewards();
    //     assert_eq!(2, promo_rewards.len());

    //     assert_eq!(promo_rewards[0].service_provider_amount, 333);
    //     assert_eq!(promo_rewards[0].matched_amount, 333);
    //     //
    //     assert_eq!(promo_rewards[1].service_provider_amount, 666);
    //     assert_eq!(promo_rewards[1].matched_amount, 666);
    // }

    // #[test]
    // fn unallocated_matching_does_not_exceed_promotion() {
    //     // 100 unallocated
    //     let total_rewards = dec!(1100);
    //     let sp_session = dec!(1000);

    //     let sp_infos = ServiceProviderRewardInfos::new(
    //         ServiceProviderDCSessions::from([(0, sp_session)]),
    //         ServiceProviderFunds::from([(0, 100)]), // Severely limit promotion rewards
    //         ServiceProviderPromotions::from([
    //             PromotionRewardShare {
    //                 service_provider_id: 0,
    //                 rewardable_entity: Entity::SubscriberId(vec![0]),
    //                 shares: 1,
    //             },
    //             PromotionRewardShare {
    //                 service_provider_id: 0,
    //                 rewardable_entity: Entity::SubscriberId(vec![1]),
    //                 shares: 2,
    //             },
    //         ]),
    //         total_rewards,
    //         dec!(0.00001),
    //         epoch(),
    //     );

    //     let promo_rewards = sp_infos.iter_rewards().only_promotion_rewards();
    //     assert_eq!(2, promo_rewards.len());

    //     assert_eq!(promo_rewards[0].service_provider_amount, 3);
    //     assert_eq!(promo_rewards[0].matched_amount, 3);
    //     //
    //     assert_eq!(promo_rewards[1].service_provider_amount, 6);
    //     assert_eq!(promo_rewards[1].matched_amount, 6);
    // }

    

    // trait PromoRewardFiltersExt {
    //     fn only_promotion_rewards(&self) -> Vec<PromotionReward>;
    // }

    // impl PromoRewardFiltersExt for Vec<(u64, MobileRewardShare)> {
    //     fn only_promotion_rewards(&self) -> Vec<PromotionReward> {
    //         self.clone()
    //             .into_iter()
    //             .filter_map(|(_, r)| {
    //                 if let Some(proto::Reward::PromotionReward(reward)) = r.reward {
    //                     Some(reward)
    //                 } else {
    //                     None
    //                 }
    //             })
    //             .collect()
    //     }
    // }

    use proptest::prelude::*;

    prop_compose! {
        fn arb_share()(sp_id in 0..10_i32, ent_id in 0..200u8, shares in 1..=100u64) -> PromotionRewardShare  {
            PromotionRewardShare {
                service_provider_id: sp_id,
                rewardable_entity: Entity::SubscriberId(vec![ent_id]),
                shares
            }
        }
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

    prop_compose! {
        fn arb_fund()(sp_id in 0..10_i32, bps in arb_bps()) -> (i32, u16) {
            (sp_id, bps)
        }
    }

    prop_compose! {
        fn arb_bps()(bps in 0..=10_000u16) -> u16 { bps }
    }

    proptest! {
        // #![proptest_config(ProptestConfig::with_cases(100_000))]

        #[test]
        fn single_provider_does_not_overallocate(
            dc_session in any::<u64>().prop_map(Decimal::from),
            fund_bps in arb_bps(),
            shares in prop::collection::vec(arb_share(), 0..10),
            total_allocation in any::<u64>().prop_map(Decimal::from)
        ) {

            let sp_infos = ServiceProviderRewardInfos::new(
                ServiceProviderDCSessions::from([(0, dc_session)]),
                ServiceProviderFunds::from([(0, fund_bps)]),
                ServiceProviderPromotions::from(shares),
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
            funds in prop::collection::vec(arb_fund(), 0..10),
            promotions in prop::collection::vec(arb_share(), 0..100),
        ) {
            let epoch = epoch();
            let total_allocation = service_provider::get_scheduled_tokens(&epoch);

            let sp_infos = ServiceProviderRewardInfos::new(
                ServiceProviderDCSessions::from(dc_sessions),
                ServiceProviderFunds::from(funds),
                ServiceProviderPromotions::from(promotions),
                total_allocation,
                dec!(0.00001),
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

    impl RewardInfo {
        fn total_percent(&self) -> Decimal {
            self.realized_dc_perc + self.realized_promo_perc + self.matched_promo_perc
        }
    }

    impl ServiceProviderRewardInfos {
        fn total_percent(&self) -> Decimal {
            self.coll.iter().map(|x| x.total_percent()).sum()
        }
    }
}
