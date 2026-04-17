use chrono::{DateTime, FixedOffset, Utc};
use helium_iceberg::{FieldDefinition, PartitionDefinition, SortFieldDefinition, TableDefinition};
use serde::{Deserialize, Serialize};
use trino_rust_client::Trino;

mod proto {
    pub use helium_proto::services::poc_mobile::ServiceProviderReward;
}

use super::REWARDS_NAMESPACE;

pub const TABLE_NAME: &str = "service_provider";

#[derive(Debug, Clone, Trino, Serialize, Deserialize, PartialEq)]
pub struct IcebergServiceProviderReward {
    service_provider_id: i32,
    amount: u64,
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
        .build()
}

impl IcebergServiceProviderReward {
    pub fn from_sp_reward(
        reward: &proto::ServiceProviderReward,
        start_period: DateTime<Utc>,
        end_period: DateTime<Utc>,
    ) -> Self {
        Self {
            service_provider_id: reward.service_provider_id,
            amount: reward.amount,
            rewardable_entity_key: reward.rewardable_entity_key.clone(),
            start_period: start_period.into(),
            end_period: end_period.into(),
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
        let reward = proto::ServiceProviderReward {
            service_provider_id: 0,
            amount: 45_000_000_000,
            rewardable_entity_key: "Helium Mobile Service Rewards".to_string(),
        };

        let iceberg = IcebergServiceProviderReward::from_sp_reward(&reward, Utc::now(), Utc::now());

        assert_eq!(iceberg.service_provider_id, 0);
        assert_eq!(iceberg.amount, 45_000_000_000);
        assert_eq!(
            iceberg.rewardable_entity_key,
            "Helium Mobile Service Rewards"
        );
    }
}
