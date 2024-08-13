//!
//! Many changes to the rewards algorithm are contained in and across many HIPs.
//! The blog post [MOBILE Proof of Coverage][mobile-poc-blog] contains a more
//! thorough explanation of many of them. It is not exhaustive, but a great
//! place to start.
//!
//! ## Important Fields
//! - [CoveredHex::points]
//!   - [HIP-74][modeled-coverage]
//!   - reduced cbrs radio coverage points [HIP-113][cbrs-experimental]
//!
//! - [CoveredHex::assignment_multiplier]
//!   - [HIP-103][oracle-boosting]
//!     - provider boosted hexes increase oracle boosting to 1x
//!
//! - [CoveredHex::rank]
//!   - [HIP-105][hex-limits]
//!
//! - [CoveredHex::boosted_multiplier]
//!   - must meet minimum subscriber thresholds [HIP-84][provider-boosting]
//!   - Wifi Location trust score >0.75 for boosted hex eligibility [HIP-93][wifi-aps]
//!
//! - [CoveragePoints::location_trust_multiplier]
//!   - [HIP-98][qos-score]
//!   - states 30m requirement for boosted hexes [HIP-107][prevent-gaming]
//!   - increase Boosted hex restriction, 30m -> 50m [Pull Request][boosted-hex-restriction]
//!   - Maximum Asserted Distance Difference [HIP-119][location-gaming]
//!
//! - [CoveragePoints::speedtest_multiplier]
//!   - [HIP-74][modeled-coverage]
//!   - added "Good" speedtest tier [HIP-98][qos-score]
//!     - latency is explicitly under limit in HIP <https://github.com/helium/oracles/pull/737>
//!
//! ## Notable Conditions:
//! - [LocationTrust]
//!   - The average distance to asserted must be <=50m to be eligible for boosted rewards.
//!   - CBRS Radio's location is always trusted because of GPS.
//!
//! - [Speedtest]
//!   - The latest 6 speedtests will be used.
//!   - There must be more than 2 speedtests.
//!
//! - [CoveredHex]
//!   - If a Radio is not [BoostedHexStatus::Eligible], boost values are removed before calculations.
//!   - If a Hex is boosted by a Provider, the Oracle Assignment multiplier is automatically 1x.
//!
//! - [ServiceProviderBoostedRewardEligibility]
//!   - Radio must pass at least 1mb of data from 3 unique phones [HIP-84][provider-boosting]
//!   - Service Provider can invalidate boosted rewards of a hotspot [HIP-125][provider-banning]
//!
//! [modeled-coverage]:        https://github.com/helium/HIP/blob/main/0074-mobile-poc-modeled-coverage-rewards.md#outdoor-radios
//! [provider-boosting]:       https://github.com/helium/HIP/blob/main/0084-service-provider-hex-boosting.md
//! [wifi-aps]:                https://github.com/helium/HIP/blob/main/0093-addition-of-wifi-aps-to-mobile-subdao.md
//! [qos-score]:               https://github.com/helium/HIP/blob/main/0098-mobile-subdao-quality-of-service-requirements.md
//! [oracle-boosting]:         https://github.com/helium/HIP/blob/main/0103-oracle-hex-boosting.md
//! [hex-limits]:              https://github.com/helium/HIP/blob/main/0105-modification-of-mobile-subdao-hex-limits.md
//! [prevent-gaming]:          https://github.com/helium/HIP/blob/main/0107-preventing-gaming-within-the-mobile-network.md
//! [cbrs-experimental]:       https://github.com/helium/HIP/blob/main/0113-reward-cbrs-as-experimental.md
//! [mobile-poc-blog]:         https://docs.helium.com/mobile/proof-of-coverage
//! [boosted-hex-restriction]: https://github.com/helium/oracles/pull/808
//! [location-gaming]:         https://github.com/helium/HIP/blob/main/0119-closing-gaming-loopholes-within-the-mobile-network.md
//! [provider-banning]:        https://github.com/helium/HIP/blob/main/0125-temporary-anti-gaming-measures-for-boosted-hexes.md
//!
pub use crate::{
    hexes::{CoveredHex, HexPoints},
    location::{asserted_distance_to_trust_multiplier, LocationTrust},
    service_provider_boosting::SPBoostedRewardEligibility,
    speedtest::{BytesPs, Speedtest, SpeedtestTier},
};
use coverage_map::SignalLevel;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use service_provider_boosting::{MAX_AVERAGE_DISTANCE, MIN_WIFI_TRUST_MULTIPLIER};

mod hexes;
mod location;
mod service_provider_boosting;
pub mod speedtest;

pub type Result<T = ()> = std::result::Result<T, Error>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("signal level {0:?} not allowed for {1:?}")]
    InvalidSignalLevel(SignalLevel, RadioType),
}

/// Output of calculating coverage points for a Radio.
///
/// The data in this struct may be different from the input data, but
/// it contains the values used for calculating coverage points.
///
/// - If more than the allowed speedtests were provided, only the speedtests
///   considered are included here.
///
/// - When a radio covers boosted hexes, [CoveragePoints::location_trust_scores] will contain a
///   trust score _after_ the boosted hex restriction has been applied.
///
/// - When a radio is not eligible for boosted hex rewards, [CoveragePoints::covered_hexes] will
///   have no boosted_multiplier values.
///
/// #### Terminology
///
/// *Points*
///
/// The value provided from covering hexes. These include hex related modifiers.
/// - Rank
/// - Oracle boosting.
///
/// *Multipliers*
///
/// Values relating to a radio that modify it's points.
/// - Location Trust
/// - Speedtest
///
/// *Shares*
///
/// The result of multiplying Points and Multipliers together.
///
#[derive(Debug, Clone)]
pub struct CoveragePoints {
    /// Breakdown of coverage points by source
    pub coverage_points: HexPoints,
    /// Location Trust Multiplier, maximum of 1
    ///
    /// Coverage trust of a Radio
    pub location_trust_multiplier: Decimal,
    /// Speedtest Mulitplier, maximum of 1
    ///
    /// Backhaul of a Radio
    pub speedtest_multiplier: Decimal,
    /// Input Radio Type
    pub radio_type: RadioType,
    /// Input ServiceProviderBoostedRewardEligibility
    pub service_provider_boosted_reward_eligibility: SPBoostedRewardEligibility,
    /// Derived Eligibility for Boosted Hex Rewards
    pub boosted_hex_eligibility: BoostedHexStatus,
    /// Speedtests used in calculcation
    pub speedtests: Vec<Speedtest>,
    /// Location Trust Scores used in calculation
    pub location_trust_scores: Vec<LocationTrust>,
    /// Covered Hexes used in calculation
    pub covered_hexes: Vec<CoveredHex>,
}

impl CoveragePoints {
    pub fn new(
        radio_type: RadioType,
        service_provider_boosted_reward_eligibility: SPBoostedRewardEligibility,
        speedtests: Vec<Speedtest>,
        location_trust_scores: Vec<LocationTrust>,
        ranked_coverage: Vec<coverage_map::RankedCoverage>,
    ) -> Result<CoveragePoints> {
        let location_trust_multiplier = location::multiplier(radio_type, &location_trust_scores);

        let boost_eligibility = BoostedHexStatus::new(
            radio_type,
            location_trust_multiplier,
            &location_trust_scores,
            service_provider_boosted_reward_eligibility,
        );

        let covered_hexes =
            hexes::clean_covered_hexes(radio_type, boost_eligibility, ranked_coverage)?;
        let hex_coverage_points = hexes::calculated_coverage_points(&covered_hexes);

        let speedtests = speedtest::clean_speedtests(speedtests);
        let speedtest_multiplier = speedtest::multiplier(&speedtests);

        Ok(CoveragePoints {
            coverage_points: hex_coverage_points,
            location_trust_multiplier,
            speedtest_multiplier,
            radio_type,
            service_provider_boosted_reward_eligibility,
            boosted_hex_eligibility: boost_eligibility,
            speedtests,
            location_trust_scores,
            covered_hexes,
        })
    }

    /// Accumulated points related only to coverage.
    /// (Hex * Rank * Assignment) * Location Trust
    /// Used for reporting.
    ///
    /// NOTE:
    /// Coverage Points includes Location Trust multiplier. In a future version,
    /// coverage points will refer only to points received by covering a hex,
    /// and multipliers like location trust will be applied later to reach a
    /// value referred to as "shares".
    ///
    /// Ref:
    /// <https://github.com/helium/proto/blob/master/src/service/poc_mobile.proto>
    /// `message radio_reward`
    pub fn coverage_points_v1(&self) -> Decimal {
        let total_coverage_points = self.coverage_points.base + self.boosted_points();
        total_coverage_points * self.location_trust_multiplier
    }

    /// Accumulated points related to entire radio.
    /// coverage points * speedtest
    /// Used in calculating rewards
    pub fn total_shares(&self) -> Decimal {
        self.total_base_shares() + self.total_boosted_shares()
    }

    /// Useful for grabbing only base points when calculating reward shares
    pub fn total_base_shares(&self) -> Decimal {
        self.coverage_points.base * self.speedtest_multiplier * self.location_trust_multiplier
    }

    /// Useful for grabbing only boost points when calculating reward shares
    pub fn total_boosted_shares(&self) -> Decimal {
        self.boosted_points() * self.speedtest_multiplier * self.location_trust_multiplier
    }

    fn boosted_points(&self) -> Decimal {
        match self.boosted_hex_eligibility {
            BoostedHexStatus::Eligible => self.coverage_points.boosted,
            BoostedHexStatus::WifiLocationScoreBelowThreshold(_) => dec!(0),
            BoostedHexStatus::AverageAssertedDistanceOverLimit(_) => dec!(0),
            BoostedHexStatus::RadioThresholdNotMet => dec!(0),
            BoostedHexStatus::ServiceProviderBanned => dec!(0),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BoostedHexStatus {
    Eligible,
    WifiLocationScoreBelowThreshold(Decimal),
    AverageAssertedDistanceOverLimit(Decimal),
    RadioThresholdNotMet,
    ServiceProviderBanned,
}

impl BoostedHexStatus {
    fn new(
        radio_type: RadioType,
        location_trust_multiplier: Decimal,
        location_trust_scores: &[LocationTrust],
        service_provider_boosted_reward_eligibility: SPBoostedRewardEligibility,
    ) -> Self {
        match service_provider_boosted_reward_eligibility {
            // hip-125: if radio has been banned by service provider, no boosting
            SPBoostedRewardEligibility::ServiceProviderBanned => Self::ServiceProviderBanned,
            // hip-84: if radio has not met minimum data and subscriber thresholds, no boosting
            SPBoostedRewardEligibility::RadioThresholdNotMet => Self::RadioThresholdNotMet,
            SPBoostedRewardEligibility::Eligible => {
                // hip-93: if radio is wifi & location_trust score multiplier < 0.75, no boosting
                if radio_type.is_wifi() && location_trust_multiplier < MIN_WIFI_TRUST_MULTIPLIER {
                    return Self::WifiLocationScoreBelowThreshold(location_trust_multiplier);
                }

                // hip-119: if the average distance to asserted is beyond 50m, no boosting
                let average_distance =
                    location::average_distance(radio_type, location_trust_scores);
                if average_distance > MAX_AVERAGE_DISTANCE {
                    return Self::AverageAssertedDistanceOverLimit(average_distance);
                }

                Self::Eligible
            }
        }
    }

    fn is_eligible(&self) -> bool {
        matches!(self, Self::Eligible)
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum RadioType {
    IndoorWifi,
    OutdoorWifi,
    IndoorCbrs,
    OutdoorCbrs,
}

impl RadioType {
    fn base_coverage_points(&self, signal_level: &SignalLevel) -> Result<Decimal> {
        let mult = match self {
            RadioType::IndoorWifi => match signal_level {
                SignalLevel::High => dec!(400),
                SignalLevel::Low => dec!(100),
                other => return Err(Error::InvalidSignalLevel(*other, *self)),
            },
            RadioType::OutdoorWifi => match signal_level {
                SignalLevel::High => dec!(16),
                SignalLevel::Medium => dec!(8),
                SignalLevel::Low => dec!(4),
                SignalLevel::None => dec!(0),
            },
            RadioType::IndoorCbrs => match signal_level {
                SignalLevel::High => dec!(100),
                SignalLevel::Low => dec!(25),
                other => return Err(Error::InvalidSignalLevel(*other, *self)),
            },
            RadioType::OutdoorCbrs => match signal_level {
                SignalLevel::High => dec!(4),
                SignalLevel::Medium => dec!(2),
                SignalLevel::Low => dec!(1),
                SignalLevel::None => dec!(0),
            },
        };
        Ok(mult)
    }

    fn rank_multiplier(&self, rank: usize) -> Decimal {
        match (self, rank) {
            // Indoors Radios
            (RadioType::IndoorWifi, 1) => dec!(1),
            (RadioType::IndoorCbrs, 1) => dec!(1),
            // Outdoor Wifi
            (RadioType::OutdoorWifi, 1) => dec!(1),
            (RadioType::OutdoorWifi, 2) => dec!(0.5),
            (RadioType::OutdoorWifi, 3) => dec!(0.25),
            // Outdoor Cbrs
            (RadioType::OutdoorCbrs, 1) => dec!(1),
            (RadioType::OutdoorCbrs, 2) => dec!(0.5),
            (RadioType::OutdoorCbrs, 3) => dec!(0.25),
            // Radios outside acceptable rank in a hex do not get points for that hex.
            _ => dec!(0),
        }
    }

    pub fn is_wifi(&self) -> bool {
        matches!(self, Self::IndoorWifi | Self::OutdoorWifi)
    }

    pub fn is_cbrs(&self) -> bool {
        matches!(self, Self::IndoorCbrs | Self::OutdoorCbrs)
    }
}

#[cfg(test)]
mod tests {

    use rstest::rstest;
    use speedtest::SpeedtestTier;

    use std::num::NonZeroU32;

    use super::*;
    use chrono::Utc;
    use coverage_map::RankedCoverage;
    use hex_assignments::{assignment::HexAssignments, Assignment};
    use rust_decimal_macros::dec;

    #[rstest]
    #[case::unboosted(0, dec!(0))]
    #[case::minimum_boosted(1, dec!(400))]
    #[case::boosted(5, dec!(2000))]
    fn hip_103_provider_boost_can_raise_oracle_boost(
        #[case] boost_multiplier: u32,
        #[case] expected_points: Decimal,
    ) {
        let wifi = CoveragePoints::new(
            RadioType::IndoorWifi,
            SPBoostedRewardEligibility::Eligible,
            speedtest_maximum(),
            location_trust_maximum(),
            vec![RankedCoverage {
                hotspot_key: pubkey(),
                cbsd_id: None,
                hex: hex_location(),
                rank: 1,
                signal_level: SignalLevel::High,
                assignments: assignments_from(Assignment::C),
                boosted: NonZeroU32::new(boost_multiplier),
            }],
        )
        .unwrap();

        // A Hex with the worst possible oracle boosting assignment.
        // The boosting assignment multiplier will be 1x when the hex is provider boosted.
        assert_eq!(expected_points, wifi.coverage_points_v1());
    }

    #[test]
    fn hip_84_radio_meets_minimum_subscriber_threshold_for_boosted_hexes() {
        let calculate_wifi = |eligibility: SPBoostedRewardEligibility| {
            CoveragePoints::new(
                RadioType::IndoorWifi,
                eligibility,
                speedtest_maximum(),
                location_trust_maximum(),
                vec![RankedCoverage {
                    hotspot_key: pubkey(),
                    cbsd_id: None,
                    hex: hex_location(),
                    rank: 1,
                    signal_level: SignalLevel::High,
                    assignments: assignments_maximum(),
                    boosted: NonZeroU32::new(5),
                }],
            )
            .expect("indoor wifi with location scores")
        };

        let base_points = RadioType::IndoorWifi
            .base_coverage_points(&SignalLevel::High)
            .unwrap();

        // Radio meeting the threshold is eligible for boosted hexes.
        // Boosted hex provides radio with more than base_points.
        let verified_wifi = calculate_wifi(SPBoostedRewardEligibility::Eligible);
        assert_eq!(base_points * dec!(5), verified_wifi.coverage_points_v1());

        // Radio not meeting the threshold is not eligible for boosted hexes.
        // Boost from hex is not applied, radio receives base points.
        let unverified_wifi = calculate_wifi(SPBoostedRewardEligibility::RadioThresholdNotMet);
        assert_eq!(base_points, unverified_wifi.coverage_points_v1());
    }

    #[test]
    fn hip_93_wifi_with_low_location_score_receives_no_boosted_hexes() {
        let calculate_wifi = |location_trust_scores: Vec<LocationTrust>| {
            CoveragePoints::new(
                RadioType::IndoorWifi,
                SPBoostedRewardEligibility::Eligible,
                speedtest_maximum(),
                location_trust_scores,
                vec![RankedCoverage {
                    hotspot_key: pubkey(),
                    cbsd_id: None,
                    hex: hex_location(),
                    rank: 1,
                    signal_level: SignalLevel::High,
                    assignments: assignments_maximum(),
                    boosted: NonZeroU32::new(5),
                }],
            )
            .expect("indoor wifi with location scores")
        };

        let base_points = RadioType::IndoorWifi
            .base_coverage_points(&SignalLevel::High)
            .unwrap();

        // Radio with good trust score is eligible for boosted hexes.
        // Boosted hex provides radio with more than base_points.
        let trusted_wifi = calculate_wifi(location_trust_with_scores(&[dec!(1), dec!(1)]));
        assert!(trusted_wifi.location_trust_multiplier > dec!(0.75));
        assert!(trusted_wifi.coverage_points_v1() > base_points);

        // Radio with poor trust score is not eligible for boosted hexes.
        // Boost from hex is not applied, and points are further lowered by poor trust score.
        let untrusted_wifi = calculate_wifi(location_trust_with_scores(&[dec!(0.1), dec!(0.2)]));
        assert!(untrusted_wifi.location_trust_multiplier < dec!(0.75));
        assert!(untrusted_wifi.coverage_points_v1() < base_points);
    }

    #[test]
    fn hip_119_radio_with_past_50m_from_asserted_receives_no_boosted_hexes() {
        let calculate_wifi = |location_trust_scores: Vec<LocationTrust>| {
            CoveragePoints::new(
                RadioType::IndoorWifi,
                SPBoostedRewardEligibility::Eligible,
                speedtest_maximum(),
                location_trust_scores,
                vec![RankedCoverage {
                    hotspot_key: pubkey(),
                    cbsd_id: None,
                    hex: hex_location(),
                    rank: 1,
                    signal_level: SignalLevel::High,
                    assignments: assignments_maximum(),
                    boosted: NonZeroU32::new(5),
                }],
            )
            .expect("indoor wifi with location scores")
        };

        let base_points = RadioType::IndoorWifi
            .base_coverage_points(&SignalLevel::High)
            .unwrap();

        // Radio with distance to asserted under the limit is eligible for boosted hexes.
        // Boosted hex provides radio with more than base_points.
        let trusted_wifi = calculate_wifi(location_trust_with_asserted_distance(&[0, 49]));
        assert!(trusted_wifi.total_shares() > base_points);

        // Radio with distance to asserted over the limit is not eligible for boosted hexes.
        // Boost from hex is not applied.
        let untrusted_wifi = calculate_wifi(location_trust_with_asserted_distance(&[50, 51]));
        assert_eq!(untrusted_wifi.total_shares(), base_points);
    }

    #[test]
    fn speedtests_effect_reward_shares() {
        let calculate_indoor_cbrs = |speedtests: Vec<Speedtest>| {
            CoveragePoints::new(
                RadioType::IndoorCbrs,
                SPBoostedRewardEligibility::Eligible,
                speedtests,
                location_trust_maximum(),
                vec![RankedCoverage {
                    hotspot_key: pubkey(),
                    cbsd_id: Some("serial".to_string()),
                    hex: hex_location(),
                    rank: 1,
                    signal_level: SignalLevel::High,
                    assignments: assignments_maximum(),
                    boosted: None,
                }],
            )
            .expect("indoor cbrs with speedtests")
        };

        let base_coverage_points = RadioType::IndoorCbrs
            .base_coverage_points(&SignalLevel::High)
            .unwrap();

        let indoor_cbrs = calculate_indoor_cbrs(speedtest_maximum());
        assert_eq!(
            base_coverage_points * SpeedtestTier::Good.multiplier(),
            indoor_cbrs.total_shares()
        );

        let indoor_cbrs = calculate_indoor_cbrs(vec![
            speedtest_with_download(BytesPs::mbps(88)),
            speedtest_with_download(BytesPs::mbps(88)),
        ]);
        assert_eq!(
            base_coverage_points * SpeedtestTier::Acceptable.multiplier(),
            indoor_cbrs.total_shares()
        );

        let indoor_cbrs = calculate_indoor_cbrs(vec![
            speedtest_with_download(BytesPs::mbps(62)),
            speedtest_with_download(BytesPs::mbps(62)),
        ]);
        assert_eq!(
            base_coverage_points * SpeedtestTier::Degraded.multiplier(),
            indoor_cbrs.total_shares()
        );

        let indoor_cbrs = calculate_indoor_cbrs(vec![
            speedtest_with_download(BytesPs::mbps(42)),
            speedtest_with_download(BytesPs::mbps(42)),
        ]);
        assert_eq!(
            base_coverage_points * SpeedtestTier::Poor.multiplier(),
            indoor_cbrs.total_shares()
        );

        let indoor_cbrs = calculate_indoor_cbrs(vec![
            speedtest_with_download(BytesPs::mbps(25)),
            speedtest_with_download(BytesPs::mbps(25)),
        ]);
        assert_eq!(
            base_coverage_points * SpeedtestTier::Fail.multiplier(),
            indoor_cbrs.total_shares()
        );
    }

    #[test]
    fn oracle_boosting_assignments_apply_per_hex() {
        fn ranked_coverage(
            footfall: Assignment,
            landtype: Assignment,
            urbanized: Assignment,
        ) -> RankedCoverage {
            RankedCoverage {
                hotspot_key: pubkey(),
                cbsd_id: Some("serial".to_string()),
                hex: hex_location(),
                rank: 1,
                signal_level: SignalLevel::High,
                assignments: HexAssignments {
                    footfall,
                    landtype,
                    urbanized,
                },
                boosted: None,
            }
        }

        use Assignment::*;
        let indoor_cbrs = CoveragePoints::new(
            RadioType::IndoorCbrs,
            SPBoostedRewardEligibility::Eligible,
            speedtest_maximum(),
            location_trust_maximum(),
            vec![
                // yellow - POI ≥ 1 Urbanized
                ranked_coverage(A, A, A), // 100
                ranked_coverage(A, B, A), // 100
                ranked_coverage(A, C, A), // 100
                // orange - POI ≥ 1 Not Urbanized
                ranked_coverage(A, A, B), // 100
                ranked_coverage(A, B, B), // 100
                ranked_coverage(A, C, B), // 100
                // light green - Point of Interest Urbanized
                ranked_coverage(B, A, A), // 70
                ranked_coverage(B, B, A), // 70
                ranked_coverage(B, C, A), // 70
                // dark green - Point of Interest Not Urbanized
                ranked_coverage(B, A, B), // 50
                ranked_coverage(B, B, B), // 50
                ranked_coverage(B, C, B), // 50
                // light blue - No POI Urbanized
                ranked_coverage(C, A, A), // 40
                ranked_coverage(C, B, A), // 30
                ranked_coverage(C, C, A), // 5
                // dark blue - No POI Not Urbanized
                ranked_coverage(C, A, B), // 20
                ranked_coverage(C, B, B), // 15
                ranked_coverage(C, C, B), // 3
                // gray - Outside of USA
                ranked_coverage(A, A, C), // 0
                ranked_coverage(A, B, C), // 0
                ranked_coverage(A, C, C), // 0
                ranked_coverage(B, A, C), // 0
                ranked_coverage(B, B, C), // 0
                ranked_coverage(B, C, C), // 0
                ranked_coverage(C, A, C), // 0
                ranked_coverage(C, B, C), // 0
                ranked_coverage(C, C, C), // 0
            ],
        )
        .expect("indoor cbrs");

        assert_eq!(dec!(1073), indoor_cbrs.coverage_points_v1());
    }

    #[rstest]
    #[case(RadioType::OutdoorWifi, 1, dec!(16))]
    #[case(RadioType::OutdoorWifi, 2, dec!(8))]
    #[case(RadioType::OutdoorWifi, 3, dec!(4))]
    #[case(RadioType::OutdoorWifi, 42, dec!(0))]
    fn outdoor_radios_consider_top_3_ranked_hexes(
        #[case] radio_type: RadioType,
        #[case] rank: usize,
        #[case] expected_points: Decimal,
    ) {
        let outdoor_wifi = CoveragePoints::new(
            radio_type,
            SPBoostedRewardEligibility::Eligible,
            speedtest_maximum(),
            location_trust_maximum(),
            vec![RankedCoverage {
                hotspot_key: pubkey(),
                cbsd_id: None,
                hex: hex_location(),
                rank,
                signal_level: SignalLevel::High,
                assignments: assignments_maximum(),
                boosted: None,
            }],
        )
        .expect("outdoor wifi");

        assert_eq!(expected_points, outdoor_wifi.coverage_points_v1());
    }

    #[rstest]
    #[case(RadioType::IndoorWifi, 1, dec!(400))]
    #[case(RadioType::IndoorWifi, 2, dec!(0))]
    #[case(RadioType::IndoorWifi, 42, dec!(0))]
    fn indoor_radios_only_consider_first_ranked_hexes(
        #[case] radio_type: RadioType,
        #[case] rank: usize,
        #[case] expected_points: Decimal,
    ) {
        let indoor_wifi = CoveragePoints::new(
            radio_type,
            SPBoostedRewardEligibility::Eligible,
            speedtest_maximum(),
            location_trust_maximum(),
            vec![
                RankedCoverage {
                    hotspot_key: pubkey(),
                    cbsd_id: None,
                    hex: hex_location(),
                    rank,
                    signal_level: SignalLevel::High,
                    assignments: assignments_maximum(),
                    boosted: None,
                },
                RankedCoverage {
                    hotspot_key: pubkey(),
                    cbsd_id: None,
                    hex: hex_location(),
                    rank: 2,
                    signal_level: SignalLevel::High,
                    assignments: assignments_maximum(),
                    boosted: None,
                },
                RankedCoverage {
                    hotspot_key: pubkey(),
                    cbsd_id: None,
                    hex: hex_location(),
                    rank: 42,
                    signal_level: SignalLevel::High,
                    assignments: assignments_maximum(),
                    boosted: None,
                },
            ],
        )
        .expect("indoor wifi");

        assert_eq!(expected_points, indoor_wifi.coverage_points_v1());
    }

    #[test]
    fn location_trust_score_multiplier() {
        // Location scores are averaged together
        let indoor_wifi = CoveragePoints::new(
            RadioType::IndoorWifi,
            SPBoostedRewardEligibility::Eligible,
            speedtest_maximum(),
            location_trust_with_scores(&[dec!(0.1), dec!(0.2), dec!(0.3), dec!(0.4)]),
            vec![RankedCoverage {
                hotspot_key: pubkey(),
                cbsd_id: None,
                hex: hex_location(),
                rank: 1,
                signal_level: SignalLevel::High,
                assignments: assignments_maximum(),
                boosted: None,
            }],
        )
        .expect("indoor wifi");

        // Location trust scores is 1/4
        // (0.1 + 0.2 + 0.3 + 0.4) / 4
        assert_eq!(dec!(100), indoor_wifi.coverage_points_v1());
    }

    #[test]
    fn boosted_hex() {
        let covered_hexes = vec![
            RankedCoverage {
                hotspot_key: pubkey(),
                cbsd_id: None,
                hex: hex_location(),
                rank: 1,
                signal_level: SignalLevel::High,
                assignments: assignments_maximum(),
                boosted: None,
            },
            RankedCoverage {
                hotspot_key: pubkey(),
                cbsd_id: None,
                hex: hex_location(),
                rank: 1,
                signal_level: SignalLevel::Low,
                assignments: assignments_maximum(),
                boosted: NonZeroU32::new(4),
            },
        ];
        let indoor_wifi = CoveragePoints::new(
            RadioType::IndoorWifi,
            SPBoostedRewardEligibility::Eligible,
            speedtest_maximum(),
            location_trust_maximum(),
            covered_hexes.clone(),
        )
        .expect("indoor wifi");

        // The hex with a low signal_level is boosted to the same level as a
        // signal_level of High.
        assert_eq!(dec!(800), indoor_wifi.coverage_points_v1());
    }

    #[rstest]
    #[case(SignalLevel::High, dec!(4))]
    #[case(SignalLevel::Medium, dec!(2))]
    #[case(SignalLevel::Low, dec!(1))]
    #[case(SignalLevel::None, dec!(0))]
    fn outdoor_cbrs_base_coverage_points(
        #[case] signal_level: SignalLevel,
        #[case] expected: Decimal,
    ) {
        let outdoor_cbrs = CoveragePoints::new(
            RadioType::OutdoorCbrs,
            SPBoostedRewardEligibility::Eligible,
            speedtest_maximum(),
            location_trust_maximum(),
            vec![RankedCoverage {
                hotspot_key: pubkey(),
                cbsd_id: Some("serial".to_string()),
                hex: hex_location(),
                rank: 1,
                signal_level,
                assignments: assignments_maximum(),
                boosted: None,
            }],
        )
        .expect("outdoor cbrs");

        assert_eq!(expected, outdoor_cbrs.coverage_points_v1());
    }

    #[rstest]
    #[case(SignalLevel::High, dec!(100))]
    #[case(SignalLevel::Low, dec!(25))]
    fn indoor_cbrs_base_coverage_points(
        #[case] signal_level: SignalLevel,
        #[case] expected: Decimal,
    ) {
        let indoor_cbrs = CoveragePoints::new(
            RadioType::IndoorCbrs,
            SPBoostedRewardEligibility::Eligible,
            speedtest_maximum(),
            location_trust_maximum(),
            vec![RankedCoverage {
                hotspot_key: pubkey(),
                cbsd_id: Some("serial".to_string()),
                hex: hex_location(),
                rank: 1,
                signal_level,
                assignments: assignments_maximum(),
                boosted: None,
            }],
        )
        .expect("indoor cbrs");

        assert_eq!(expected, indoor_cbrs.coverage_points_v1());
    }

    #[rstest]
    #[case(SignalLevel::High, dec!(16))]
    #[case(SignalLevel::Medium, dec!(8))]
    #[case(SignalLevel::Low, dec!(4))]
    #[case(SignalLevel::None, dec!(0))]
    fn outdoor_wifi_base_coverage_points(
        #[case] signal_level: SignalLevel,
        #[case] expected: Decimal,
    ) {
        let outdoor_wifi = CoveragePoints::new(
            RadioType::OutdoorWifi,
            SPBoostedRewardEligibility::Eligible,
            speedtest_maximum(),
            location_trust_maximum(),
            vec![RankedCoverage {
                hotspot_key: pubkey(),
                cbsd_id: Some("serial".to_string()),
                hex: hex_location(),
                rank: 1,
                signal_level,
                assignments: assignments_maximum(),
                boosted: None,
            }],
        )
        .expect("indoor cbrs");

        assert_eq!(expected, outdoor_wifi.coverage_points_v1());
    }

    #[rstest]
    #[case(SignalLevel::High, dec!(400))]
    #[case(SignalLevel::Low, dec!(100))]
    fn indoor_wifi_base_coverage_points(
        #[case] signal_level: SignalLevel,
        #[case] expected: Decimal,
    ) {
        let indoor_wifi = CoveragePoints::new(
            RadioType::IndoorWifi,
            SPBoostedRewardEligibility::Eligible,
            speedtest_maximum(),
            location_trust_maximum(),
            vec![RankedCoverage {
                hotspot_key: pubkey(),
                cbsd_id: None,
                hex: hex_location(),
                rank: 1,
                signal_level,
                assignments: assignments_maximum(),
                boosted: None,
            }],
        )
        .expect("indoor wifi");

        assert_eq!(expected, indoor_wifi.coverage_points_v1());
    }

    #[test]
    fn wifi_with_bad_location_boosted_hex_status_prioritizes_service_provider_statuses() {
        let bad_location = vec![LocationTrust {
            meters_to_asserted: 100,
            trust_score: dec!(0.0),
        }];

        let wifi_bad_trust_score = |sp_status: SPBoostedRewardEligibility| {
            BoostedHexStatus::new(
                RadioType::IndoorWifi,
                location::multiplier(RadioType::IndoorWifi, &bad_location),
                &bad_location,
                sp_status,
            )
        };

        assert_eq!(
            wifi_bad_trust_score(SPBoostedRewardEligibility::Eligible),
            BoostedHexStatus::WifiLocationScoreBelowThreshold(dec!(0)),
        );
        assert_eq!(
            wifi_bad_trust_score(SPBoostedRewardEligibility::ServiceProviderBanned),
            BoostedHexStatus::ServiceProviderBanned
        );
        assert_eq!(
            wifi_bad_trust_score(SPBoostedRewardEligibility::RadioThresholdNotMet),
            BoostedHexStatus::RadioThresholdNotMet
        );
    }

    fn hex_location() -> hextree::Cell {
        hextree::Cell::from_raw(0x8c2681a3064edff).unwrap()
    }

    fn assignments_maximum() -> HexAssignments {
        HexAssignments {
            footfall: Assignment::A,
            landtype: Assignment::A,
            urbanized: Assignment::A,
        }
    }

    fn assignments_from(assignment: Assignment) -> HexAssignments {
        HexAssignments {
            footfall: assignment,
            landtype: assignment,
            urbanized: assignment,
        }
    }

    fn speedtest_maximum() -> Vec<Speedtest> {
        vec![
            Speedtest {
                upload_speed: BytesPs::mbps(15),
                download_speed: BytesPs::mbps(150),
                latency_millis: 15,
                timestamp: Utc::now(),
            },
            Speedtest {
                upload_speed: BytesPs::mbps(15),
                download_speed: BytesPs::mbps(150),
                latency_millis: 15,
                timestamp: Utc::now(),
            },
        ]
    }

    fn speedtest_with_download(download: BytesPs) -> Speedtest {
        Speedtest {
            upload_speed: BytesPs::mbps(15),
            download_speed: download,
            latency_millis: 15,
            timestamp: Utc::now(),
        }
    }

    fn location_trust_maximum() -> Vec<LocationTrust> {
        vec![LocationTrust {
            meters_to_asserted: 1,
            trust_score: dec!(1.0),
        }]
    }

    fn location_trust_with_scores(trust_scores: &[Decimal]) -> Vec<LocationTrust> {
        trust_scores
            .to_owned()
            .iter()
            .copied()
            .map(|trust_score| LocationTrust {
                meters_to_asserted: 1,
                trust_score,
            })
            .collect()
    }

    fn location_trust_with_asserted_distance(distances_to_asserted: &[u32]) -> Vec<LocationTrust> {
        distances_to_asserted
            .to_owned()
            .iter()
            .copied()
            .map(|meters_to_asserted| LocationTrust {
                meters_to_asserted,
                trust_score: dec!(1.0),
            })
            .collect()
    }

    fn pubkey() -> Vec<u8> {
        vec![1]
    }
}
