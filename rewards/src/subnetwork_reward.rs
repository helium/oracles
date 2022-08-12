use helium_proto::SubnetworkReward as ProtoSubnetworkReward;
use std::cmp::{Eq, Ord, Ordering, PartialEq, PartialOrd};

#[derive(Clone, Debug)]
struct SubnetworkReward(ProtoSubnetworkReward);

pub fn sorted_rewards(unsorted_rewards: Vec<ProtoSubnetworkReward>) -> Vec<ProtoSubnetworkReward> {
    let mut rewards: Vec<SubnetworkReward> = unsorted_rewards
        .iter()
        .map(|r| SubnetworkReward(r.to_owned()))
        .collect();
    rewards.sort();
    rewards.iter().map(|r| r.0.to_owned()).collect()
}

impl From<ProtoSubnetworkReward> for SubnetworkReward {
    fn from(r: helium_proto::SubnetworkReward) -> Self {
        Self(r)
    }
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
