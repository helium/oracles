use chrono::{DateTime, FixedOffset, Utc};
use helium_iceberg::{FieldDefinition, PartitionDefinition, SortFieldDefinition, TableDefinition};
use serde::{Deserialize, Serialize};
use trino_rust_client::Trino;

mod proto {
    pub use helium_proto::services::poc_mobile::UnallocatedReward;
}

use super::REWARDS_NAMESPACE;

pub const TABLE_NAME: &str = "unallocated";

#[derive(Debug, Clone, Trino, Serialize, Deserialize, PartialEq)]
pub struct IcebergUnallocatedReward {
    reward_type: String,
    amount: u64,
    start_period: DateTime<FixedOffset>,
    end_period: DateTime<FixedOffset>,
}

pub fn table_definition() -> helium_iceberg::Result<TableDefinition> {
    TableDefinition::builder(REWARDS_NAMESPACE, TABLE_NAME)
        .with_fields([
            FieldDefinition::required_string("reward_type"),
            FieldDefinition::required_long("amount"),
            FieldDefinition::required_timestamptz("start_period"),
            FieldDefinition::required_timestamptz("end_period"),
        ])
        .with_partition(PartitionDefinition::day("start_period", "start_period_day"))
        .with_sort_fields([SortFieldDefinition::ascending("start_period")])
        .build()
}

impl IcebergUnallocatedReward {
    pub fn from_reward(
        reward: &proto::UnallocatedReward,
        start_period: DateTime<Utc>,
        end_period: DateTime<Utc>,
    ) -> Self {
        Self {
            reward_type: reward.reward_type().as_str_name().to_string(),
            amount: reward.amount,
            start_period: start_period.into(),
            end_period: end_period.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use helium_proto::services::poc_mobile::UnallocatedRewardType;

    #[test]
    fn table_definition_builds_successfully() {
        let def = table_definition().expect("table definition should build");
        assert_eq!(def.name(), TABLE_NAME);
        assert_eq!(def.namespace(), REWARDS_NAMESPACE);
    }

    #[test]
    fn convert_unallocated_reward() {
        let reward = proto::UnallocatedReward {
            reward_type: UnallocatedRewardType::Poc as i32,
            amount: 999_999,
        };

        let iceberg = IcebergUnallocatedReward::from_reward(&reward, Utc::now(), Utc::now());

        assert_eq!(iceberg.amount, 999_999);
    }
}
