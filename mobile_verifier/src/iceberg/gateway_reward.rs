use chrono::{DateTime, FixedOffset, Utc};
use helium_crypto::PublicKeyBinary;
use helium_iceberg::{FieldDefinition, PartitionDefinition, SortFieldDefinition, TableDefinition};
use serde::{Deserialize, Serialize};
use trino_rust_client::Trino;

mod proto {
    pub use helium_proto::services::poc_mobile::GatewayReward;
}

use super::REWARDS_NAMESPACE;

pub const TABLE_NAME: &str = "data_transfer";

#[derive(Debug, Clone, Trino, Serialize, Deserialize, PartialEq)]
pub struct IcebergGatewayReward {
    hotspot_key: String,
    dc_transfer_reward: u64,
    rewardable_bytes: u64,
    price: u64,
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

impl IcebergGatewayReward {
    pub fn from_gateway_reward(
        gateway: &proto::GatewayReward,
        start_period: DateTime<Utc>,
        end_period: DateTime<Utc>,
    ) -> Self {
        Self {
            hotspot_key: PublicKeyBinary::from(gateway.hotspot_key.as_slice()).to_string(),
            dc_transfer_reward: gateway.dc_transfer_reward,
            rewardable_bytes: gateway.rewardable_bytes,
            price: gateway.price,
            start_period: start_period.into(),
            end_period: end_period.into(),
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn table_definition_builds_successfully() {
        let def = table_definition().expect("table definition should build");
        assert_eq!(def.name(), TABLE_NAME);
        assert_eq!(def.namespace(), REWARDS_NAMESPACE);
    }

    #[test]
    fn convert_gateway_reward() {
        let start_period = Utc::now();
        let end_period = Utc::now();

        let gateway = proto::GatewayReward {
            hotspot_key: vec![1, 2, 3],
            dc_transfer_reward: 5000,
            rewardable_bytes: 1_000_000,
            price: 100_000_000,
        };

        let iceberg = IcebergGatewayReward::from_gateway_reward(&gateway, start_period, end_period);

        assert_eq!(iceberg.dc_transfer_reward, 5000);
        assert_eq!(iceberg.rewardable_bytes, 1_000_000);
        assert_eq!(iceberg.price, 100_000_000);
    }
}
