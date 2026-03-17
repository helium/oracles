use chrono::{DateTime, FixedOffset};
use helium_iceberg::{FieldDefinition, PartitionDefinition, SortFieldDefinition, TableDefinition};
use helium_proto::services::poc_mobile::{
    mobile_reward_share::Reward as ProtoReward, MobileRewardShare, UnallocatedRewardType,
};
use serde::{Deserialize, Serialize};
use trino_rust_client::Trino;

use super::{timestamp_to_dt, NAMESPACE};

pub const TABLE_NAME: &str = "unallocated_rewards";

#[derive(Debug, Clone, Trino, Serialize, Deserialize, PartialEq)]
pub struct IcebergUnallocatedReward {
    reward_type: String,
    amount: i64,
    start_period: DateTime<FixedOffset>,
    end_period: DateTime<FixedOffset>,
}

pub fn table_definition() -> helium_iceberg::Result<TableDefinition> {
    TableDefinition::builder(NAMESPACE, TABLE_NAME)
        .with_fields([
            FieldDefinition::required_string("reward_type"),
            FieldDefinition::required_long("amount"),
            FieldDefinition::required_timestamptz("start_period"),
            FieldDefinition::required_timestamptz("end_period"),
        ])
        .with_partition(PartitionDefinition::day("start_period", "start_period_day"))
        .with_sort_fields([SortFieldDefinition::ascending("start_period")])
        .wap_enabled()
        .build()
}

impl From<&MobileRewardShare> for IcebergUnallocatedReward {
    fn from(share: &MobileRewardShare) -> Self {
        let reward = match &share.reward {
            Some(ProtoReward::UnallocatedReward(r)) => r,
            other => panic!("expected UnallocatedReward, got {other:?}"),
        };

        Self {
            reward_type: UnallocatedRewardType::try_from(reward.reward_type)
                .unwrap_or(UnallocatedRewardType::Poc)
                .as_str_name()
                .to_string(),
            amount: reward.amount as i64,
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
        assert_eq!(def.namespace(), NAMESPACE);
    }

    #[test]
    fn convert_unallocated_reward() {
        let share = MobileRewardShare {
            start_period: 1_700_000_000,
            end_period: 1_700_086_400,
            reward: Some(ProtoReward::UnallocatedReward(proto::UnallocatedReward {
                reward_type: UnallocatedRewardType::Poc as i32,
                amount: 999_999,
            })),
        };

        let iceberg = IcebergUnallocatedReward::from(&share);

        assert_eq!(iceberg.amount, 999_999);
    }
}
