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
        mobile_reward_share::Reward, MobileRewardShare, PromotionReward, ServiceProviderReward,
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
        total_allocation: Decimal,
        reward_period: &Range<DateTime<Utc>>,
    ) -> Vec<(u64, proto::MobileRewardShare)> {
        if self.promotion_rewards.is_empty() {
            return vec![];
        }

        let mut rewards = vec![];

        let sp_amount = total_allocation * self.realized_promo_perc;
        let matched_amount = total_allocation * self.matched_promo_perc;

        let total_shares = self.promotion_rewards.total_shares();
        let sp_amount_per_share = sp_amount / total_shares;
        let matched_amount_per_share = matched_amount / total_shares;

        for r in self.promotion_rewards.iter() {
            let shares = Decimal::from(r.shares);

            let service_provider_amount = (sp_amount_per_share * shares).to_u64_rounded();
            let matched_amount = (matched_amount_per_share * shares).to_u64_rounded();

            let total_amount = service_provider_amount + matched_amount;

            rewards.push((
                total_amount,
                proto::MobileRewardShare {
                    start_period: reward_period.start.encode_timestamp(),
                    end_period: reward_period.end.encode_timestamp(),
                    reward: Some(proto::Reward::PromotionReward(proto::PromotionReward {
                        service_provider_amount,
                        matched_amount,
                        entity: Some(r.rewardable_entity.clone().into()),
                    })),
                },
            ))
        }

        rewards
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
        sp.matched_promo_perc = ((shares / total) * unallocated_perc).round_dp(5);
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
    use helium_proto::services::poc_mobile::{MobileRewardShare, PromotionReward};

    use crate::service_provider::promotions::rewards::PromotionRewardShare;

    use super::*;

    use super::ServiceProviderRewardInfos;

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

    #[test]
    fn no_matched_rewards_if_no_unallocated() {
        let total_rewards = dec!(1000);

        let sp_infos = ServiceProviderRewardInfos::new(
            ServiceProviderDCSessions::from([(0, total_rewards)]),
            ServiceProviderFunds::from([(0, 5000)]),
            ServiceProviderPromotions::from([PromotionRewardShare {
                service_provider_id: 0,
                rewardable_entity: Entity::SubscriberId(vec![0]),
                shares: 1,
            }]),
            total_rewards,
            dec!(0.001),
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
            ServiceProviderFunds::from([(0, 10000)]), // All rewards allocated to promotions
            ServiceProviderPromotions::from([
                PromotionRewardShare {
                    service_provider_id: 0,
                    rewardable_entity: Entity::SubscriberId(vec![0]),
                    shares: 1,
                },
                PromotionRewardShare {
                    service_provider_id: 0,
                    rewardable_entity: Entity::SubscriberId(vec![1]),
                    shares: 2,
                },
            ]),
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
            ServiceProviderFunds::from([(0, 10000)]), // All rewards allocated to promotions
            ServiceProviderPromotions::from([
                PromotionRewardShare {
                    service_provider_id: 0,
                    rewardable_entity: Entity::SubscriberId(vec![0]),
                    shares: 1,
                },
                PromotionRewardShare {
                    service_provider_id: 0,
                    rewardable_entity: Entity::SubscriberId(vec![1]),
                    shares: 2,
                },
            ]),
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
            ServiceProviderFunds::from([(0, 100)]), // Severely limit promotion rewards
            ServiceProviderPromotions::from([
                PromotionRewardShare {
                    service_provider_id: 0,
                    rewardable_entity: Entity::SubscriberId(vec![0]),
                    shares: 1,
                },
                PromotionRewardShare {
                    service_provider_id: 0,
                    rewardable_entity: Entity::SubscriberId(vec![1]),
                    shares: 2,
                },
            ]),
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

    fn epoch() -> Range<DateTime<Utc>> {
        let now = Utc::now();
        let epoch = now - Duration::hours(24)..now;
        epoch
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
}
