use helium_proto::SubnetworkReward as ProtoSubnetworkReward;
use std::cmp::{Eq, Ord, Ordering, PartialEq, PartialOrd};

// use crate::PublicKey;

#[derive(Clone, Debug)]
struct SubnetworkReward(ProtoSubnetworkReward);

// TODO: Implement SubnetworkRewardDetails such that gateways and rewards are grouped by owners.
// i.e. SubnetworkRewardDetails(Hashmap<owner, Vec<(gateway, amount)>)

// #[derive(Clone, Debug)]
// pub struct SubnetworkRewardDetails {
//     reward: ProtoSubnetworkReward,
//     gateway: PublicKey,
// }

// impl SubnetworkRewardDetails {
//     pub fn new(reward: ProtoSubnetworkReward, gateway: PublicKey) -> SubnetworkRewardDetails {
//         Self { reward, gateway }
//     }
// }

impl From<SubnetworkReward> for ProtoSubnetworkReward {
    fn from(subnet_reward: SubnetworkReward) -> Self {
        subnet_reward.0
    }
}

impl From<ProtoSubnetworkReward> for SubnetworkReward {
    fn from(r: ProtoSubnetworkReward) -> Self {
        Self(r)
    }
}

pub fn sorted_rewards(unsorted_rewards: Vec<ProtoSubnetworkReward>) -> Vec<ProtoSubnetworkReward> {
    let mut rewards: Vec<SubnetworkReward> = unsorted_rewards
        .iter()
        .map(|r| SubnetworkReward(r.to_owned()))
        .collect();
    rewards.sort();
    rewards.iter().map(|r| r.0.to_owned()).collect()
}

impl Ord for SubnetworkReward {
    fn cmp(&self, other: &Self) -> Ordering {
        let inner = self.0.clone();
        let other_inner = other.0.clone();
        (inner.account, &inner.amount).cmp(&(other_inner.account, &other_inner.amount))
    }
}

impl PartialOrd for SubnetworkReward {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for SubnetworkReward {
    fn eq(&self, other: &Self) -> bool {
        let inner = self.0.clone();
        let other_inner = other.0.clone();
        (inner.account, &inner.amount) == (other_inner.account, &other_inner.amount)
    }
}

impl Eq for SubnetworkReward {}
