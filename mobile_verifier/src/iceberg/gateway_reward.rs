use chrono::{DateTime, FixedOffset};
use helium_crypto::PublicKeyBinary;
use helium_iceberg::{FieldDefinition, PartitionDefinition, SortFieldDefinition, TableDefinition};
use helium_proto::services::poc_mobile::{
    mobile_reward_share::Reward as ProtoReward, MobileRewardShare,
};
use serde::{Deserialize, Serialize};
use trino_rust_client::Trino;

use super::{timestamp_to_dt, REWARDS_NAMESPACE};

pub const TABLE_NAME: &str = "data_transfer";

#[derive(Debug, Clone, Trino, Serialize, Deserialize, PartialEq)]
pub struct IcebergGatewayReward {
    hotspot_key: String,
    dc_transfer_reward: i64,
    rewardable_bytes: i64,
    price: i64,
    start_period: DateTime<FixedOffset>,
    end_period: DateTime<FixedOffset>,
}

pub fn table_definition() -> helium_iceberg::Result<TableDefinition> {
    TableDefinition::builder(REWARDS_NAMESPACE, TABLE_NAME)
        .with_fields([
            FieldDefinition::required_string("hotspot_key"),
            FieldDefinition::required_long("dc_transfer_reward"),
            FieldDefinition::required_long("rewardable_bytes"),
            FieldDefinition::required_long("price"),
            FieldDefinition::required_timestamptz("start_period"),
            FieldDefinition::required_timestamptz("end_period"),
        ])
        .with_partition(PartitionDefinition::day("start_period", "start_period_day"))
        .with_sort_fields([
            SortFieldDefinition::ascending("hotspot_key"),
            SortFieldDefinition::ascending("start_period"),
        ])
        .wap_enabled()
        .build()
}

impl From<&MobileRewardShare> for IcebergGatewayReward {
    fn from(share: &MobileRewardShare) -> Self {
        let reward = match &share.reward {
            Some(ProtoReward::GatewayReward(r)) => r,
            other => panic!("expected GatewayReward, got {other:?}"),
        };

        Self {
            hotspot_key: PublicKeyBinary::from(reward.hotspot_key.as_slice()).to_string(),
            dc_transfer_reward: reward.dc_transfer_reward as i64,
            rewardable_bytes: reward.rewardable_bytes as i64,
            price: reward.price as i64,
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
    fn convert_gateway_reward() {
        let share = MobileRewardShare {
            start_period: 1_700_000_000,
            end_period: 1_700_086_400,
            reward: Some(ProtoReward::GatewayReward(proto::GatewayReward {
                hotspot_key: vec![1, 2, 3],
                dc_transfer_reward: 5000,
                rewardable_bytes: 1_000_000,
                price: 100_000_000,
            })),
        };

        let iceberg = IcebergGatewayReward::from(&share);

        assert_eq!(iceberg.dc_transfer_reward, 5000);
        assert_eq!(iceberg.rewardable_bytes, 1_000_000);
        assert_eq!(iceberg.price, 100_000_000);
    }
}
