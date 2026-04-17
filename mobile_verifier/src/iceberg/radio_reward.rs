use chrono::{DateTime, FixedOffset, Utc};
use file_store::traits::TimestampDecode;
use helium_crypto::PublicKeyBinary;
use helium_iceberg::{FieldDefinition, PartitionDefinition, SortFieldDefinition, TableDefinition};
use serde::{Deserialize, Serialize};
use trino_rust_client::Trino;
use uuid::Uuid;

mod proto {
    pub use helium_proto::services::poc_mobile::RadioRewardV2;
}

use crate::FromProtoDecimal;

use super::REWARDS_NAMESPACE;

pub const TABLE_NAME: &str = "proof_of_coverage";

#[derive(Debug, Clone, Trino, Serialize, Deserialize, PartialEq)]
pub struct IcebergRadioReward {
    hotspot_key: String,
    cbsd_id: String,
    base_coverage_points_sum: f64,
    boosted_coverage_points_sum: f64,
    base_reward_shares: f64,
    boosted_reward_shares: f64,
    base_poc_reward: u64,
    boosted_poc_reward: u64,
    seniority_timestamp: DateTime<FixedOffset>,
    coverage_object: String,
    location_trust_score_multiplier: f64,
    speedtest_multiplier: f64,
    sp_boosted_hex_status: String,
    oracle_boosted_hex_status: String,
    speedtest_avg_upload_speed_bps: u64,
    speedtest_avg_download_speed_bps: u64,
    speedtest_avg_latency_ms: u32,
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
        .build()
}

impl IcebergRadioReward {
    pub fn from_radio_reward(
        reward: &proto::RadioRewardV2,
        start_period: DateTime<Utc>,
        end_period: DateTime<Utc>,
    ) -> Self {
        let (avg_upload, avg_download, avg_latency) =
            reward.speedtest_average.as_ref().map_or((0, 0, 0), |s| {
                (s.upload_speed_bps, s.download_speed_bps, s.latency_ms)
            });

        let seniority_timestamp = reward
            .seniority_timestamp
            .to_timestamp()
            .unwrap_or_default()
            .fixed_offset();

        let coverage_object = Uuid::from_slice(&reward.coverage_object)
            .map(|u| u.to_string())
            .unwrap_or_default();

        let sp_boosted_hex_status = reward.sp_boosted_hex_status().as_str_name().to_string();

        let oracle_boosted_hex_status =
            reward.oracle_boosted_hex_status().as_str_name().to_string();

        Self {
            hotspot_key: PublicKeyBinary::from(reward.hotspot_key.as_slice()).to_string(),
            cbsd_id: reward.cbsd_id.clone(),
            base_coverage_points_sum: reward.base_coverage_points_sum.to_f64(),
            boosted_coverage_points_sum: reward.boosted_coverage_points_sum.to_f64(),
            base_reward_shares: reward.base_reward_shares.to_f64(),
            boosted_reward_shares: reward.boosted_reward_shares.to_f64(),
            base_poc_reward: reward.base_poc_reward,
            boosted_poc_reward: reward.boosted_poc_reward,
            seniority_timestamp,
            coverage_object,
            location_trust_score_multiplier: reward.location_trust_score_multiplier.to_f64(),
            speedtest_multiplier: reward.speedtest_multiplier.to_f64(),
            sp_boosted_hex_status,
            oracle_boosted_hex_status,
            speedtest_avg_upload_speed_bps: avg_upload,
            speedtest_avg_download_speed_bps: avg_download,
            speedtest_avg_latency_ms: avg_latency,
            start_period: start_period.into(),
            end_period: end_period.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::ToProtoDecimal;

    use super::*;

    use helium_proto::services::poc_mobile::{
        OracleBoostedHexStatus, SpBoostedHexStatus, Speedtest,
    };

    use rust_decimal::dec;

    #[test]
    fn table_definition_builds_successfully() {
        let def = table_definition().expect("table definition should build");
        assert_eq!(def.name(), TABLE_NAME);
        assert_eq!(def.namespace(), REWARDS_NAMESPACE);
    }

    #[test]
    fn convert_radio_reward_v2() {
        let coverage_uuid = Uuid::new_v4();
        let reward = proto::RadioRewardV2 {
            hotspot_key: vec![1, 2, 3],
            cbsd_id: "test-cbsd".to_string(),
            base_coverage_points_sum: Some(dec!(100.5).proto_decimal()),
            boosted_coverage_points_sum: Some(dec!(50.25).proto_decimal()),
            base_reward_shares: Some(dec!(75.0).proto_decimal()),
            boosted_reward_shares: Some(dec!(25.0).proto_decimal()),
            base_poc_reward: 1000,
            boosted_poc_reward: 500,
            seniority_timestamp: 1_699_900_000,
            coverage_object: coverage_uuid.as_bytes().to_vec(),
            location_trust_score_multiplier: Some(dec!(0.9).proto_decimal()),
            speedtest_multiplier: Some(dec!(0.8).proto_decimal()),
            sp_boosted_hex_status: SpBoostedHexStatus::Eligible as i32,
            oracle_boosted_hex_status: OracleBoostedHexStatus::Eligible as i32,
            speedtest_average: Some(Speedtest {
                upload_speed_bps: 10_000_000,
                download_speed_bps: 100_000_000,
                latency_ms: 25,
                timestamp: 0,
            }),
            ..Default::default()
        };

        let iceberg = IcebergRadioReward::from_radio_reward(&reward, Utc::now(), Utc::now());

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
        let reward = proto::RadioRewardV2 {
            hotspot_key: vec![1, 2, 3],
            speedtest_average: None,
            ..Default::default()
        };

        let iceberg = IcebergRadioReward::from_radio_reward(&reward, Utc::now(), Utc::now());

        assert_eq!(iceberg.speedtest_avg_upload_speed_bps, 0);
        assert_eq!(iceberg.speedtest_avg_download_speed_bps, 0);
        assert_eq!(iceberg.speedtest_avg_latency_ms, 0);
    }
}
