use std::ops::Range;

use chrono::{DateTime, Utc};

use file_store::traits::TimestampEncode;
use rust_decimal::{prelude::ToPrimitive, Decimal, RoundingStrategy};
use rust_decimal_macros::dec;

use crate::reward_shares::{dc_to_mobile_bones, DEFAULT_PREC};

use super::{
    dc_sessions::ServiceProviderDCSessions,
    promotions::{funds::ServiceProviderFunds, rewards::PromotionRewardShares},
};

mod proto {

    pub use helium_proto::services::poc_mobile::{
        mobile_reward_share::Reward, MobileRewardShare, PromotionReward, ServiceProviderReward,
    };
}

/// Container for ['ServiceProvideRewardInfo']
#[derive(Debug)]
pub struct RewardInfoColl {
    coll: Vec<RewardInfo>,
    total_sp_allocation: Decimal,
    all_transfer: Decimal,
    mobile_bone_price: Decimal,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RewardInfo {
    // proto::ServiceProvider enum repr
    sp_id: i32,

    // How much DC in sessions
    dc: Decimal,
    // % allocated from DC to promo rewards (found in db from file from on chain)
    allocated_promo_perc: Decimal,

    // % of total allocated rewards for all service providers
    dc_perc: Decimal,
    // % of total allocated rewards going towards rewards
    realized_promo_perc: Decimal,
    // % usable dc percentage of total rewards
    realized_dc_perc: Decimal,
    // % matched promotions from unallocated
    matched_promo_perc: Decimal,

    // Rewards for the epoch
    rewards: Vec<PromotionRewardShares>,
}

impl RewardInfoColl {
    pub fn new(
        dc_sessions: ServiceProviderDCSessions,
        promo_funds: ServiceProviderFunds,
        rewards: Vec<PromotionRewardShares>,
        total_sp_allocation: Decimal,
        mobile_bone_price: Decimal,
    ) -> Self {
        let all_transfer = dc_sessions.all_transfer();

        let mut me = Self {
            coll: vec![],
            all_transfer,
            total_sp_allocation,
            mobile_bone_price,
        };

        let used_allocation = total_sp_allocation.max(all_transfer);
        for (dc_session, dc_transfer) in dc_sessions.iter() {
            let promo_fund_perc = promo_funds.get_fund_percent(dc_session);
            me.coll.push(RewardInfo::new(
                dc_session,
                dc_transfer,
                promo_fund_perc,
                used_allocation,
                rewards
                    .iter()
                    .filter(|r| r.service_provider_id == dc_session)
                    .cloned()
                    .collect(),
            ));
        }

        distribute_unallocated(&mut me.coll);

        me
    }

    pub fn iter_rewards(
        &self,
        reward_epoch: &Range<DateTime<Utc>>,
    ) -> Vec<(u64, proto::MobileRewardShare)> {
        let rewards_per_share = rewards_per_share(
            self.all_transfer,
            self.total_sp_allocation,
            self.mobile_bone_price,
        );
        let sp_rewards = self.total_sp_allocation * rewards_per_share;

        let mut rewards = vec![];
        for sp in self.coll.iter() {
            rewards.extend(sp.promo_rewards(sp_rewards, reward_epoch));
            rewards.push(sp.carrier_reward(sp_rewards, reward_epoch));
        }
        rewards
    }
}

impl RewardInfo {
    fn new(
        sp_id: i32,
        dc_transfer: Decimal,
        promo_fund_perc: Decimal,
        used_allocation: Decimal,
        rewards: Vec<PromotionRewardShares>,
    ) -> Self {
        let dc_perc = dc_transfer / used_allocation;
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

            rewards,
        }
    }

    pub fn carrier_reward(
        &self,
        total_allocation: Decimal,
        reward_period: &Range<DateTime<Utc>>,
    ) -> (u64, proto::MobileRewardShare) {
        let amount = total_allocation * self.realized_dc_perc;

        (
            amount
                .round_dp_with_strategy(0, RoundingStrategy::MidpointNearestEven)
                .to_u64()
                .unwrap_or(0),
            proto::MobileRewardShare {
                start_period: reward_period.start.encode_timestamp(),
                end_period: reward_period.end.encode_timestamp(),
                reward: Some(proto::Reward::ServiceProviderReward(
                    proto::ServiceProviderReward {
                        service_provider_id: self.sp_id,
                        amount: amount
                            .round_dp_with_strategy(0, RoundingStrategy::MidpointNearestEven)
                            .to_u64()
                            .unwrap_or(0),
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
        if self.rewards.is_empty() {
            return vec![];
        }

        let mut rewards = vec![];

        let sp_amount = total_allocation * self.realized_promo_perc;
        let matched_amount = total_allocation * self.matched_promo_perc;

        let total_shares = self
            .rewards
            .iter()
            .map(|r| Decimal::from(r.shares))
            .sum::<Decimal>();
        let sp_amount_per_share = sp_amount / total_shares;
        let matched_amount_per_share = matched_amount / total_shares;

        for r in self.rewards.iter() {
            let shares = Decimal::from(r.shares);

            let service_provider_amount = sp_amount_per_share * shares;
            let matched_amount = matched_amount_per_share * shares;

            let total_amount = (service_provider_amount + matched_amount)
                .round_dp_with_strategy(0, RoundingStrategy::MidpointNearestEven)
                .to_u64()
                .unwrap_or(0);

            rewards.push((
                total_amount,
                proto::MobileRewardShare {
                    start_period: reward_period.start.encode_timestamp(),
                    end_period: reward_period.end.encode_timestamp(),
                    reward: Some(proto::Reward::PromotionReward(proto::PromotionReward {
                        service_provider_amount: service_provider_amount
                            .round_dp_with_strategy(0, RoundingStrategy::MidpointNearestEven)
                            .to_u64()
                            .unwrap_or(0),
                        matched_amount: matched_amount
                            .round_dp_with_strategy(0, RoundingStrategy::MidpointNearestEven)
                            .to_u64()
                            .unwrap_or(0),
                        entity: Some(r.rewardable_entity.clone().into()),
                    })),
                },
            ))
        }

        rewards
    }
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

fn distribute_unallocated(coll: &mut [RewardInfo]) {
    let allocated_perc = coll.iter().map(|x| x.dc_perc).sum::<Decimal>();
    let unallocated_perc = dec!(1) - allocated_perc;

    let maybe_matching_perc = coll
        .iter()
        .filter(|x| !x.rewards.is_empty())
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
        if sp.rewards.is_empty() {
            continue;
        }
        let shares = sp.realized_promo_perc * dec!(100);
        sp.matched_promo_perc = ((shares / total) * unallocated_perc).round_dp(5);
    }
}

fn distribute_unalloc_under_limit(coll: &mut [RewardInfo]) {
    for sp in coll.iter_mut() {
        if sp.rewards.is_empty() {
            continue;
        }
        sp.matched_promo_perc = sp.realized_promo_perc
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use file_store::promotion_reward::Entity;
    use std::collections::HashMap;
    use uuid::Uuid;

    fn get_unallocated_percent(sps: &RewardInfoColl) -> Decimal {
        dec!(1.0) - sps.coll.iter().map(|x| x.dc_perc).sum::<Decimal>()
    }

    #[test]
    fn test_multiple_with_reward() {
        let dc_sessions = HashMap::from_iter([(0, dec!(600)), (1, dec!(600))]);
        let promo_funds = HashMap::from_iter([(0, 2000), (1, 4000)]); // bps values
        let total_allocation = dec!(1000);

        let sps = RewardInfoColl::new(
            ServiceProviderDCSessions(dc_sessions),
            ServiceProviderFunds(promo_funds),
            vec![
                PromotionRewardShares {
                    service_provider_id: 0,
                    rewardable_entity: Entity::SubscriberId(Uuid::new_v4().into()),
                    shares: 1,
                },
                PromotionRewardShares {
                    service_provider_id: 0,
                    rewardable_entity: Entity::SubscriberId(Uuid::new_v4().into()),
                    shares: 2,
                },
                PromotionRewardShares {
                    service_provider_id: 0,
                    rewardable_entity: Entity::SubscriberId(Uuid::new_v4().into()),
                    shares: 3,
                },
            ],
            total_allocation,
            dec!(0.0001),
        );
        let x = get_unallocated_percent(&sps);

        println!("leftover: {x:?}");

        let reward_period = DateTime::<Utc>::MIN_UTC..DateTime::<Utc>::MAX_UTC;
        for sp in sps.coll {
            println!("{sp:?}");
            for reward in sp.promo_rewards(total_allocation, &reward_period) {
                println!("--> {reward:?}");
            }
        }
    }

    #[test]
    fn test_multiple_no_reward() {
        let dc_sessions = HashMap::from_iter([(0, dec!(600)), (1, dec!(600))]);
        let promo_funds = HashMap::from_iter([(0, 2000), (1, 4000)]); // bps values
        let total_allocation = dec!(1200);

        let mut sps = RewardInfoColl::new(
            ServiceProviderDCSessions(dc_sessions),
            ServiceProviderFunds(promo_funds),
            vec![],
            total_allocation,
            dec!(0.0001),
        );

        distribute_unallocated(&mut sps.coll);

        println!("{sps:#?}");
    }

    #[test]
    fn over_limit_with_rewards() {
        let promo_0 = PromotionRewardShares {
            service_provider_id: 0,
            rewardable_entity: file_store::promotion_reward::Entity::SubscriberId(
                uuid::Uuid::new_v4().into(),
            ),
            shares: 1,
        };
        let promo_1 = PromotionRewardShares {
            service_provider_id: 0,
            rewardable_entity: file_store::promotion_reward::Entity::SubscriberId(
                uuid::Uuid::new_v4().into(),
            ),
            shares: 1,
        };
        let expected_one = RewardInfo {
            sp_id: 0,
            dc: dec!(200),
            allocated_promo_perc: dec!(0.2),
            dc_perc: dec!(0.2),
            realized_promo_perc: dec!(0.04),
            realized_dc_perc: dec!(0.16),
            matched_promo_perc: dec!(0.02667),

            rewards: vec![promo_0.clone()],
        };

        let one = RewardInfo::new(0, dec!(200), dec!(0.2), dec!(1000), vec![promo_0]);
        let two = RewardInfo::new(1, dec!(200), dec!(0.4), dec!(1000), vec![promo_1]);

        let unallocated_perc = dec!(0.08);
        let mut x = vec![one, two];
        distribute_unalloc_over_limit(&mut x, unallocated_perc);

        println!("{:#?}", x[0]);
        println!("{:#?}", x[1]);

        assert_eq!(expected_one, x[0]);
    }

    #[test]
    fn over_limit_no_rewards() {
        let expected_one = RewardInfo {
            sp_id: 0,
            dc: dec!(200),
            allocated_promo_perc: dec!(0.20),
            dc_perc: dec!(0.20),
            realized_promo_perc: dec!(0.00),
            realized_dc_perc: dec!(0.20),
            matched_promo_perc: dec!(0.0),

            rewards: vec![],
        };

        let one = RewardInfo::new(0, dec!(200), dec!(0.2), dec!(1000), vec![]);
        let two = RewardInfo::new(1, dec!(200), dec!(0.4), dec!(1000), vec![]);

        let unallocated_perc = dec!(0.08);
        let mut x = vec![one, two];
        distribute_unalloc_over_limit(&mut x, unallocated_perc);

        println!("{:#?}", x[0]);
        println!("{:#?}", x[1]);

        assert_eq!(expected_one, x[0]);
    }
}
