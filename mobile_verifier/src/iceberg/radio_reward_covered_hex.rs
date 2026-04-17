use chrono::{DateTime, FixedOffset, Utc};
use helium_crypto::PublicKeyBinary;
use helium_iceberg::{FieldDefinition, PartitionDefinition, SortFieldDefinition, TableDefinition};
use serde::{Deserialize, Serialize};
use trino_rust_client::Trino;

mod proto {
    pub use helium_proto::services::poc_mobile::RadioRewardV2;
}

use crate::FromProtoDecimal;

use super::REWARDS_NAMESPACE;

pub const TABLE_NAME: &str = "covered_hexes";

#[derive(Debug, Clone, Trino, Serialize, Deserialize, PartialEq)]
pub struct IcebergRadioRewardCoveredHex {
    hotspot_key: String,
    location: u64,
    base_coverage_points: f64,
    boosted_coverage_points: f64,
    urbanized: String,
    footfall: String,
    landtype: String,
    service_provider_override: String,
    assignment_multiplier: f64,
    rank: u32,
    rank_multiplier: f64,
    boosted_multiplier: u32,
    start_period: DateTime<FixedOffset>,
    end_period: DateTime<FixedOffset>,
}

pub fn table_definition() -> helium_iceberg::Result<TableDefinition> {
    TableDefinition::builder(REWARDS_NAMESPACE, TABLE_NAME)
        .with_fields([
            FieldDefinition::required_string("hotspot_key"),
            FieldDefinition::required_long("location"),
            FieldDefinition::required_double("base_coverage_points"),
            FieldDefinition::required_double("boosted_coverage_points"),
            FieldDefinition::required_string("urbanized"),
            FieldDefinition::required_string("footfall"),
            FieldDefinition::required_string("landtype"),
            FieldDefinition::required_string("service_provider_override"),
            FieldDefinition::required_double("assignment_multiplier"),
            FieldDefinition::required_int("rank"),
            FieldDefinition::required_double("rank_multiplier"),
            FieldDefinition::required_int("boosted_multiplier"),
            FieldDefinition::required_timestamptz("start_period"),
            FieldDefinition::required_timestamptz("end_period"),
        ])
        .with_partition(PartitionDefinition::day("start_period", "start_period_day"))
        .with_sort_fields([
            SortFieldDefinition::ascending("hotspot_key"),
            SortFieldDefinition::ascending("location"),
        ])
        .build()
}

pub fn from_radio_reward(
    reward: &proto::RadioRewardV2,
    start_period: DateTime<Utc>,
    end_period: DateTime<Utc>,
) -> Vec<IcebergRadioRewardCoveredHex> {
    let hotspot_key = PublicKeyBinary::from(reward.hotspot_key.as_slice()).to_string();
    let start_period = start_period.into();
    let end_period = end_period.into();

    reward
        .covered_hexes
        .iter()
        .map(|hex| IcebergRadioRewardCoveredHex {
            hotspot_key: hotspot_key.clone(),
            location: hex.location,
            base_coverage_points: hex.base_coverage_points.to_f64(),
            boosted_coverage_points: hex.boosted_coverage_points.to_f64(),
            urbanized: hex.urbanized().as_str_name().to_string(),
            footfall: hex.footfall().as_str_name().to_string(),
            landtype: hex.landtype().as_str_name().to_string(),
            service_provider_override: hex.service_provider_override.to_string(),
            assignment_multiplier: hex.assignment_multiplier.to_f64(),
            rank: hex.rank,
            rank_multiplier: hex.rank_multiplier.to_f64(),
            boosted_multiplier: hex.boosted_multiplier,
            start_period,
            end_period,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::ToProtoDecimal;

    use helium_proto::services::poc_mobile::{radio_reward_v2, OracleBoostingAssignment};
    use rust_decimal::dec;

    #[test]
    fn table_definition_builds_successfully() {
        let def = table_definition().expect("table definition should build");
        assert_eq!(def.name(), TABLE_NAME);
        assert_eq!(def.namespace(), REWARDS_NAMESPACE);
    }

    #[test]
    fn convert_covered_hexes() {
        let reward = proto::RadioRewardV2 {
            hotspot_key: vec![1, 2, 3],
            covered_hexes: vec![
                radio_reward_v2::CoveredHex {
                    location: 0x8a1fb466d2dffff,
                    base_coverage_points: Some(dec!(10.5).proto_decimal()),
                    boosted_coverage_points: Some(dec!(5.25).proto_decimal()),
                    urbanized: OracleBoostingAssignment::A as i32,
                    footfall: OracleBoostingAssignment::B as i32,
                    landtype: OracleBoostingAssignment::C as i32,
                    service_provider_override: true,
                    assignment_multiplier: Some(dec!(1.0).proto_decimal()),
                    rank: 1,
                    rank_multiplier: Some(dec!(1.0).proto_decimal()),
                    boosted_multiplier: 5,
                },
                radio_reward_v2::CoveredHex {
                    location: 0x8a1fb466d2e0000,
                    base_coverage_points: Some(dec!(8.0).proto_decimal()),
                    boosted_coverage_points: None,
                    urbanized: OracleBoostingAssignment::B as i32,
                    footfall: OracleBoostingAssignment::A as i32,
                    landtype: OracleBoostingAssignment::A as i32,
                    service_provider_override: false,
                    assignment_multiplier: Some(dec!(0.5).proto_decimal()),
                    rank: 2,
                    rank_multiplier: Some(dec!(0.5).proto_decimal()),
                    boosted_multiplier: 0,
                },
            ],
            ..Default::default()
        };

        let hexes = from_radio_reward(&reward, Utc::now(), Utc::now());

        assert_eq!(hexes.len(), 2);
        assert_eq!(hexes[0].location, 0x8a1fb466d2dffff_u64);
        assert!((hexes[0].base_coverage_points - 10.5).abs() < f64::EPSILON);
        assert!((hexes[0].boosted_coverage_points - 5.25).abs() < f64::EPSILON);
        assert_eq!(hexes[0].service_provider_override, "true");
        assert_eq!(hexes[0].rank, 1);
        assert_eq!(hexes[0].boosted_multiplier, 5);

        assert_eq!(hexes[1].location, 0x8a1fb466d2e0000_u64);
        assert!((hexes[1].base_coverage_points - 8.0).abs() < f64::EPSILON);
        assert!((hexes[1].boosted_coverage_points).abs() < f64::EPSILON);
        assert_eq!(hexes[1].service_provider_override, "false");
        assert_eq!(hexes[1].rank, 2);
        assert_eq!(hexes[1].boosted_multiplier, 0);
    }

    #[test]
    fn convert_empty_covered_hexes() {
        let reward = proto::RadioRewardV2 {
            hotspot_key: vec![1, 2, 3],
            covered_hexes: vec![],
            ..Default::default()
        };

        let hexes = from_radio_reward(&reward, Utc::now(), Utc::now());
        assert!(hexes.is_empty());
    }
}
