use file_store::traits::TimestampEncode;
use helium_proto::services::poc_mobile::{
    radio_reward_v2::{CoveredHex, LocationTrustScore},
    BoostedHexStatus, Speedtest,
};
use rust_decimal::prelude::ToPrimitive;

pub trait ToProtoDecimal {
    fn proto_decimal(&self) -> helium_proto::Decimal;
}

impl ToProtoDecimal for rust_decimal::Decimal {
    fn proto_decimal(&self) -> helium_proto::Decimal {
        helium_proto::Decimal {
            value: self.to_string(),
        }
    }
}
pub trait RadioRewardV2Ext {
    fn proto_location_trust_scores(&self) -> Vec<LocationTrustScore>;
    fn proto_speedtests(&self) -> Vec<Speedtest>;
    fn proto_speedtest_avg(&self) -> Speedtest;
    fn proto_boosted_hex_status(&self) -> BoostedHexStatus;
    fn proto_covered_hexes(&self) -> Vec<CoveredHex>;
}

impl RadioRewardV2Ext for coverage_point_calculator::CoveragePoints {
    fn proto_location_trust_scores(&self) -> Vec<LocationTrustScore> {
        self.location_trust_scores
            .iter()
            .map(|lt| LocationTrustScore {
                meters_to_asserted: lt.meters_to_asserted.into(),
                trust_score: Some(lt.trust_score.proto_decimal()),
            })
            .collect()
    }

    fn proto_speedtest_avg(&self) -> Speedtest {
        let st = self.speedtest_avg;
        helium_proto::services::poc_mobile::Speedtest {
            upload_speed_bps: st.upload_speed.as_bps(),
            download_speed_bps: st.download_speed.as_bps(),
            latency_ms: st.latency_millis,
            timestamp: st.timestamp.encode_timestamp(),
        }
    }

    fn proto_speedtests(&self) -> Vec<Speedtest> {
        self.speedtests
            .iter()
            .map(|st| helium_proto::services::poc_mobile::Speedtest {
                upload_speed_bps: st.upload_speed.as_bps(),
                download_speed_bps: st.download_speed.as_bps(),
                latency_ms: st.latency_millis,
                timestamp: st.timestamp.encode_timestamp(),
            })
            .collect()
    }

    fn proto_boosted_hex_status(&self) -> BoostedHexStatus {
        match self.boosted_hex_eligibility {
            coverage_point_calculator::SpBoostedHexStatus::Eligible => BoostedHexStatus::Eligible,
            coverage_point_calculator::SpBoostedHexStatus::WifiLocationScoreBelowThreshold(_) => {
                BoostedHexStatus::LocationScoreBelowThreshold
            }
            coverage_point_calculator::SpBoostedHexStatus::RadioThresholdNotMet => {
                BoostedHexStatus::RadioThresholdNotMet
            }
            coverage_point_calculator::SpBoostedHexStatus::ServiceProviderBanned => {
                BoostedHexStatus::ServiceProviderBan
            }
            coverage_point_calculator::SpBoostedHexStatus::AverageAssertedDistanceOverLimit(_) => {
                BoostedHexStatus::AverageAssertedDistanceOverLimit
            }
        }
    }

    fn proto_covered_hexes(&self) -> Vec<CoveredHex> {
        self.covered_hexes
            .iter()
            .map(|covered_hex| CoveredHex {
                location: covered_hex.hex.into_raw(),
                base_coverage_points: Some(covered_hex.points.base.proto_decimal()),
                boosted_coverage_points: Some(covered_hex.points.boosted.proto_decimal()),
                urbanized: covered_hex.assignments.urbanized.into(),
                footfall: covered_hex.assignments.footfall.into(),
                landtype: covered_hex.assignments.landtype.into(),
                assignment_multiplier: Some(covered_hex.assignment_multiplier.proto_decimal()),
                rank: covered_hex.rank as u32,
                rank_multiplier: Some(covered_hex.rank_multiplier.proto_decimal()),
                boosted_multiplier: covered_hex
                    .boosted_multiplier
                    .and_then(|x| x.to_u32())
                    .unwrap_or_default(),
            })
            .collect()
    }
}
