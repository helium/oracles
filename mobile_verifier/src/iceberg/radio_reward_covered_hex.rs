use chrono::{DateTime, FixedOffset};
use helium_crypto::PublicKeyBinary;
use helium_iceberg::{FieldDefinition, PartitionDefinition, SortFieldDefinition, TableDefinition};
use helium_proto::services::poc_mobile::{
    self as proto, mobile_reward_share::Reward as ProtoReward, MobileRewardShare,
};
use serde::{Deserialize, Serialize};
use trino_rust_client::Trino;

use super::{proto_decimal_to_f64, timestamp_to_dt, NAMESPACE};

pub const TABLE_NAME: &str = "radio_reward_covered_hexes";

#[derive(Debug, Clone, Trino, Serialize, Deserialize, PartialEq)]
pub struct IcebergRadioRewardCoveredHex {
    hotspot_key: String,
    location: i64,
    base_coverage_points: f64,
    boosted_coverage_points: f64,
    urbanized: String,
    footfall: String,
    landtype: String,
    service_provider_override: String,
    assignment_multiplier: f64,
    rank: i32,
    rank_multiplier: f64,
    boosted_multiplier: i32,
    start_period: DateTime<FixedOffset>,
    end_period: DateTime<FixedOffset>,
}

pub fn table_definition() -> helium_iceberg::Result<TableDefinition> {
    TableDefinition::builder(NAMESPACE, TABLE_NAME)
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
        .wap_enabled()
        .build()
}

fn oracle_boosting_assignment_name(value: i32) -> String {
    proto::OracleBoostingAssignment::try_from(value)
        .unwrap_or(proto::OracleBoostingAssignment::A)
        .as_str_name()
        .to_string()
}

pub fn from_reward_share(share: &MobileRewardShare) -> Vec<IcebergRadioRewardCoveredHex> {
    let reward = match &share.reward {
        Some(ProtoReward::RadioRewardV2(r)) => r,
        other => panic!("expected RadioRewardV2, got {other:?}"),
    };

    let hotspot_key = PublicKeyBinary::from(reward.hotspot_key.as_slice()).to_string();
    let start_period = timestamp_to_dt(share.start_period);
    let end_period = timestamp_to_dt(share.end_period);

    reward
        .covered_hexes
        .iter()
        .map(|hex| IcebergRadioRewardCoveredHex {
            hotspot_key: hotspot_key.clone(),
            location: hex.location as i64,
            base_coverage_points: proto_decimal_to_f64(&hex.base_coverage_points),
            boosted_coverage_points: proto_decimal_to_f64(&hex.boosted_coverage_points),
            urbanized: oracle_boosting_assignment_name(hex.urbanized),
            footfall: oracle_boosting_assignment_name(hex.footfall),
            landtype: oracle_boosting_assignment_name(hex.landtype),
            service_provider_override: hex.service_provider_override.to_string(),
            assignment_multiplier: proto_decimal_to_f64(&hex.assignment_multiplier),
            rank: hex.rank as i32,
            rank_multiplier: proto_decimal_to_f64(&hex.rank_multiplier),
            boosted_multiplier: hex.boosted_multiplier as i32,
            start_period,
            end_period,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn table_definition_builds_successfully() {
        let def = table_definition().expect("table definition should build");
        assert_eq!(def.name(), TABLE_NAME);
        assert_eq!(def.namespace(), NAMESPACE);
    }

    #[test]
    fn convert_covered_hexes() {
        let share = MobileRewardShare {
            start_period: 1_700_000_000,
            end_period: 1_700_086_400,
            reward: Some(ProtoReward::RadioRewardV2(proto::RadioRewardV2 {
                hotspot_key: vec![1, 2, 3],
                covered_hexes: vec![
                    proto::radio_reward_v2::CoveredHex {
                        location: 0x8a1fb466d2dffff,
                        base_coverage_points: Some(helium_proto::Decimal {
                            value: "10.5".to_string(),
                        }),
                        boosted_coverage_points: Some(helium_proto::Decimal {
                            value: "5.25".to_string(),
                        }),
                        urbanized: proto::OracleBoostingAssignment::A as i32,
                        footfall: proto::OracleBoostingAssignment::B as i32,
                        landtype: proto::OracleBoostingAssignment::C as i32,
                        service_provider_override: true,
                        assignment_multiplier: Some(helium_proto::Decimal {
                            value: "1.0".to_string(),
                        }),
                        rank: 1,
                        rank_multiplier: Some(helium_proto::Decimal {
                            value: "1.0".to_string(),
                        }),
                        boosted_multiplier: 5,
                    },
                    proto::radio_reward_v2::CoveredHex {
                        location: 0x8a1fb466d2e0000,
                        base_coverage_points: Some(helium_proto::Decimal {
                            value: "8.0".to_string(),
                        }),
                        boosted_coverage_points: None,
                        urbanized: proto::OracleBoostingAssignment::B as i32,
                        footfall: proto::OracleBoostingAssignment::A as i32,
                        landtype: proto::OracleBoostingAssignment::A as i32,
                        service_provider_override: false,
                        assignment_multiplier: Some(helium_proto::Decimal {
                            value: "0.5".to_string(),
                        }),
                        rank: 2,
                        rank_multiplier: Some(helium_proto::Decimal {
                            value: "0.5".to_string(),
                        }),
                        boosted_multiplier: 0,
                    },
                ],
                ..Default::default()
            })),
        };

        let hexes = from_reward_share(&share);

        assert_eq!(hexes.len(), 2);
        assert_eq!(hexes[0].location, 0x8a1fb466d2dffff_i64);
        assert!((hexes[0].base_coverage_points - 10.5).abs() < f64::EPSILON);
        assert!((hexes[0].boosted_coverage_points - 5.25).abs() < f64::EPSILON);
        assert_eq!(hexes[0].service_provider_override, "true");
        assert_eq!(hexes[0].rank, 1);
        assert_eq!(hexes[0].boosted_multiplier, 5);

        assert_eq!(hexes[1].location, 0x8a1fb466d2e0000_i64);
        assert!((hexes[1].base_coverage_points - 8.0).abs() < f64::EPSILON);
        assert!((hexes[1].boosted_coverage_points).abs() < f64::EPSILON);
        assert_eq!(hexes[1].service_provider_override, "false");
        assert_eq!(hexes[1].rank, 2);
        assert_eq!(hexes[1].boosted_multiplier, 0);
    }

    #[test]
    fn convert_empty_covered_hexes() {
        let share = MobileRewardShare {
            start_period: 1_700_000_000,
            end_period: 1_700_086_400,
            reward: Some(ProtoReward::RadioRewardV2(proto::RadioRewardV2 {
                hotspot_key: vec![1, 2, 3],
                covered_hexes: vec![],
                ..Default::default()
            })),
        };

        let hexes = from_reward_share(&share);
        assert!(hexes.is_empty());
    }
}
