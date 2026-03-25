use chrono::{DateTime, FixedOffset};
use helium_crypto::PublicKeyBinary;
use helium_iceberg::{FieldDefinition, PartitionDefinition, SortFieldDefinition, TableDefinition};
use helium_proto::services::poc_mobile::{
    self as proto, mobile_reward_share::Reward as ProtoReward, MobileRewardShare,
};
use serde::{Deserialize, Serialize};
use trino_rust_client::Trino;
use uuid::Uuid;

use super::{proto_decimal_to_f64, timestamp_to_dt, REWARDS_NAMESPACE};

pub const TABLE_NAME: &str = "proof_of_coverage";

#[derive(Debug, Clone, Trino, Serialize, Deserialize, PartialEq)]
pub struct IcebergRadioReward {
    hotspot_key: String,
    cbsd_id: String,
    base_coverage_points_sum: f64,
    boosted_coverage_points_sum: f64,
    base_reward_shares: f64,
    boosted_reward_shares: f64,
    base_poc_reward: i64,
    boosted_poc_reward: i64,
    seniority_timestamp: DateTime<FixedOffset>,
    coverage_object: String,
    location_trust_score_multiplier: f64,
    speedtest_multiplier: f64,
    sp_boosted_hex_status: String,
    oracle_boosted_hex_status: String,
    speedtest_avg_upload_speed_bps: i64,
    speedtest_avg_download_speed_bps: i64,
    speedtest_avg_latency_ms: i64,
    start_period: DateTime<FixedOffset>,
    end_period: DateTime<FixedOffset>,
}

pub fn table_definition() -> helium_iceberg::Result<TableDefinition> {
    TableDefinition::builder(REWARDS_NAMESPACE, TABLE_NAME)
        .with_fields([
            FieldDefinition::required_string("hotspot_key"),
            FieldDefinition::required_string("cbsd_id"),
            FieldDefinition::required_double("base_coverage_points_sum"),
            FieldDefinition::required_double("boosted_coverage_points_sum"),
            FieldDefinition::required_double("base_reward_shares"),
            FieldDefinition::required_double("boosted_reward_shares"),
            FieldDefinition::required_long("base_poc_reward"),
            FieldDefinition::required_long("boosted_poc_reward"),
            FieldDefinition::required_timestamptz("seniority_timestamp"),
            FieldDefinition::required_string("coverage_object"),
            FieldDefinition::required_double("location_trust_score_multiplier"),
            FieldDefinition::required_double("speedtest_multiplier"),
            FieldDefinition::required_string("sp_boosted_hex_status"),
            FieldDefinition::required_string("oracle_boosted_hex_status"),
            FieldDefinition::required_long("speedtest_avg_upload_speed_bps"),
            FieldDefinition::required_long("speedtest_avg_download_speed_bps"),
            FieldDefinition::required_long("speedtest_avg_latency_ms"),
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

impl From<&MobileRewardShare> for IcebergRadioReward {
    fn from(share: &MobileRewardShare) -> Self {
        let reward = match &share.reward {
            Some(ProtoReward::RadioRewardV2(r)) => r,
            other => panic!("expected RadioRewardV2, got {other:?}"),
        };

        let (avg_upload, avg_download, avg_latency) =
            reward.speedtest_average.as_ref().map_or((0, 0, 0), |s| {
                (
                    s.upload_speed_bps as i64,
                    s.download_speed_bps as i64,
                    s.latency_ms as i64,
                )
            });

        Self {
            hotspot_key: PublicKeyBinary::from(reward.hotspot_key.as_slice()).to_string(),
            cbsd_id: reward.cbsd_id.clone(),
            base_coverage_points_sum: proto_decimal_to_f64(&reward.base_coverage_points_sum),
            boosted_coverage_points_sum: proto_decimal_to_f64(&reward.boosted_coverage_points_sum),
            base_reward_shares: proto_decimal_to_f64(&reward.base_reward_shares),
            boosted_reward_shares: proto_decimal_to_f64(&reward.boosted_reward_shares),
            base_poc_reward: reward.base_poc_reward as i64,
            boosted_poc_reward: reward.boosted_poc_reward as i64,
            seniority_timestamp: timestamp_to_dt(reward.seniority_timestamp),
            coverage_object: Uuid::from_slice(&reward.coverage_object)
                .map(|u| u.to_string())
                .unwrap_or_default(),
            location_trust_score_multiplier: proto_decimal_to_f64(
                &reward.location_trust_score_multiplier,
            ),
            speedtest_multiplier: proto_decimal_to_f64(&reward.speedtest_multiplier),
            sp_boosted_hex_status: proto::SpBoostedHexStatus::try_from(
                reward.sp_boosted_hex_status,
            )
            .unwrap_or(proto::SpBoostedHexStatus::Eligible)
            .as_str_name()
            .to_string(),
            oracle_boosted_hex_status: proto::OracleBoostedHexStatus::try_from(
                reward.oracle_boosted_hex_status,
            )
            .unwrap_or(proto::OracleBoostedHexStatus::Eligible)
            .as_str_name()
            .to_string(),
            speedtest_avg_upload_speed_bps: avg_upload,
            speedtest_avg_download_speed_bps: avg_download,
            speedtest_avg_latency_ms: avg_latency,
            start_period: timestamp_to_dt(share.start_period),
            end_period: timestamp_to_dt(share.end_period),
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
    fn convert_radio_reward_v2() {
        let coverage_uuid = Uuid::new_v4();
        let share = MobileRewardShare {
            start_period: 1_700_000_000,
            end_period: 1_700_086_400,
            reward: Some(ProtoReward::RadioRewardV2(proto::RadioRewardV2 {
                hotspot_key: vec![1, 2, 3],
                cbsd_id: "test-cbsd".to_string(),
                base_coverage_points_sum: Some(helium_proto::Decimal {
                    value: "100.5".to_string(),
                }),
                boosted_coverage_points_sum: Some(helium_proto::Decimal {
                    value: "50.25".to_string(),
                }),
                base_reward_shares: Some(helium_proto::Decimal {
                    value: "75.0".to_string(),
                }),
                boosted_reward_shares: Some(helium_proto::Decimal {
                    value: "25.0".to_string(),
                }),
                base_poc_reward: 1000,
                boosted_poc_reward: 500,
                seniority_timestamp: 1_699_900_000,
                coverage_object: coverage_uuid.as_bytes().to_vec(),
                location_trust_score_multiplier: Some(helium_proto::Decimal {
                    value: "0.9".to_string(),
                }),
                speedtest_multiplier: Some(helium_proto::Decimal {
                    value: "0.8".to_string(),
                }),
                sp_boosted_hex_status: proto::SpBoostedHexStatus::Eligible as i32,
                oracle_boosted_hex_status: proto::OracleBoostedHexStatus::Eligible as i32,
                speedtest_average: Some(proto::Speedtest {
                    upload_speed_bps: 10_000_000,
                    download_speed_bps: 100_000_000,
                    latency_ms: 25,
                    timestamp: 0,
                }),
                ..Default::default()
            })),
        };

        let iceberg = IcebergRadioReward::from(&share);

        assert_eq!(iceberg.cbsd_id, "test-cbsd");
        assert!((iceberg.base_coverage_points_sum - 100.5).abs() < f64::EPSILON);
        assert!((iceberg.boosted_coverage_points_sum - 50.25).abs() < f64::EPSILON);
        assert!((iceberg.base_reward_shares - 75.0).abs() < f64::EPSILON);
        assert!((iceberg.boosted_reward_shares - 25.0).abs() < f64::EPSILON);
        assert_eq!(iceberg.base_poc_reward, 1000);
        assert_eq!(iceberg.boosted_poc_reward, 500);
        assert_eq!(iceberg.coverage_object, coverage_uuid.to_string());
        assert!((iceberg.location_trust_score_multiplier - 0.9).abs() < f64::EPSILON);
        assert!((iceberg.speedtest_multiplier - 0.8).abs() < f64::EPSILON);
        assert_eq!(iceberg.speedtest_avg_upload_speed_bps, 10_000_000);
        assert_eq!(iceberg.speedtest_avg_download_speed_bps, 100_000_000);
        assert_eq!(iceberg.speedtest_avg_latency_ms, 25);
    }

    #[test]
    fn convert_radio_reward_v2_with_no_speedtest_average() {
        let share = MobileRewardShare {
            start_period: 1_700_000_000,
            end_period: 1_700_086_400,
            reward: Some(ProtoReward::RadioRewardV2(proto::RadioRewardV2 {
                hotspot_key: vec![1, 2, 3],
                speedtest_average: None,
                ..Default::default()
            })),
        };

        let iceberg = IcebergRadioReward::from(&share);

        assert_eq!(iceberg.speedtest_avg_upload_speed_bps, 0);
        assert_eq!(iceberg.speedtest_avg_download_speed_bps, 0);
        assert_eq!(iceberg.speedtest_avg_latency_ms, 0);
    }
}
