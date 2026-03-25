use chrono::{DateTime, FixedOffset};
use helium_iceberg::{FieldDefinition, PartitionDefinition, SortFieldDefinition, TableDefinition};
use helium_proto::services::poc_mobile::{
    mobile_reward_share::Reward as ProtoReward, MobileRewardShare,
};
use serde::{Deserialize, Serialize};
use trino_rust_client::Trino;

use super::{timestamp_to_dt, REWARDS_NAMESPACE};

pub const TABLE_NAME: &str = "service_provider";

#[derive(Debug, Clone, Trino, Serialize, Deserialize, PartialEq)]
pub struct IcebergServiceProviderReward {
    service_provider_id: i32,
    amount: i64,
    rewardable_entity_key: String,
    start_period: DateTime<FixedOffset>,
    end_period: DateTime<FixedOffset>,
}

pub fn table_definition() -> helium_iceberg::Result<TableDefinition> {
    TableDefinition::builder(REWARDS_NAMESPACE, TABLE_NAME)
        .with_fields([
            FieldDefinition::required_int("service_provider_id"),
            FieldDefinition::required_long("amount"),
            FieldDefinition::required_string("rewardable_entity_key"),
            FieldDefinition::required_timestamptz("start_period"),
            FieldDefinition::required_timestamptz("end_period"),
        ])
        .with_partition(PartitionDefinition::day("start_period", "start_period_day"))
        .with_sort_fields([SortFieldDefinition::ascending("start_period")])
        .wap_enabled()
        .build()
}

impl From<&MobileRewardShare> for IcebergServiceProviderReward {
    fn from(share: &MobileRewardShare) -> Self {
        let reward = match &share.reward {
            Some(ProtoReward::ServiceProviderReward(r)) => r,
            other => panic!("expected ServiceProviderReward, got {other:?}"),
        };

        Self {
            service_provider_id: reward.service_provider_id,
            amount: reward.amount as i64,
            rewardable_entity_key: reward.rewardable_entity_key.clone(),
            start_period: timestamp_to_dt(share.start_period),
            end_period: timestamp_to_dt(share.end_period),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use helium_proto::services::poc_mobile as proto;

    #[test]
    fn table_definition_builds_successfully() {
        let def = table_definition().expect("table definition should build");
        assert_eq!(def.name(), TABLE_NAME);
        assert_eq!(def.namespace(), REWARDS_NAMESPACE);
    }

    #[test]
    fn convert_service_provider_reward() {
        let share = MobileRewardShare {
            start_period: 1_700_000_000,
            end_period: 1_700_086_400,
            reward: Some(ProtoReward::ServiceProviderReward(
                proto::ServiceProviderReward {
                    service_provider_id: 0,
                    amount: 45_000_000_000,
                    rewardable_entity_key: "Helium Mobile Service Rewards".to_string(),
                },
            )),
        };

        let iceberg = IcebergServiceProviderReward::from(&share);

        assert_eq!(iceberg.service_provider_id, 0);
        assert_eq!(iceberg.amount, 45_000_000_000);
        assert_eq!(
            iceberg.rewardable_entity_key,
            "Helium Mobile Service Rewards"
        );
    }
}
