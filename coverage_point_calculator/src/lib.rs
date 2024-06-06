//!
//! Many changes to the rewards algorithm are contained in and across many HIPs.
//! The blog post [MOBILE Proof of Coverage][mobile-poc-blog] contains a more
//! thorough explanation of many of them. It is not exhaustive, but a great
//! place to start.
//!
//! ## Fields:
//! - modeled_coverage_points
//!   - [HIP-74][modeled-coverage]
//!   - reduced cbrs radio coverage points [HIP-113][cbrs-experimental]
//!
//! - assignment_multiplier
//!   - [HIP-103][oracle-boosting]
//!
//! - rank
//!   - [HIP-105][hex-limits]
//!
//! - hex_boost_multiplier  
//!   - must meet minimum subscriber thresholds [HIP-84][provider-boosting]
//!   - Wifi Location trust score >0.75 for boosted hex eligibility [HIP-93][wifi-aps]
//!
//! - location_trust_score_multiplier
//!   - [HIP-98][qos-score]
//!   - states 30m requirement for boosted hexes [HIP-107][prevent-gaming]
//!   - increase Boosted hex restriction, 30m -> 50m [Pull Request][boosted-hex-restriction]
//!
//! - speedtest_multiplier
//!   - [HIP-74][modeled-coverage]
//!   - added "Good" speedtest tier [HIP-98][qos-score]
//!     - latency is explicitly under limit in HIP <https://github.com/helium/oracles/pull/737>
//!
//! ## Notable Conditions:
//! - Location
//!   - If a Radio covers any boosted hexes, [LocationTrust] scores must meet distance requirements, or be degraded.
//!   - CBRS Radio's location is always trusted because of GPS.
//!
//! - Speedtests
//!   - The latest 6 speedtests will be used.
//!   - There must be more than 2 speedtests.
//!
//! - Covered Hexes
//!   - If a Radio is not [CoveragePoints::boosted_hex_eligibility], boost values are removed before calculations. [CoveredHexes::new_without_boosts]
//!
//! ## References:
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
//!
use crate::{
    hexes::CoveredHexes,
    location::{LocationTrust, LocationTrustScores},
    speedtest::Speedtest,
};
use coverage_map::{RankedCoverage, SignalLevel};
use rust_decimal::{Decimal, RoundingStrategy};
use rust_decimal_macros::dec;
use speedtest::Speedtests;

pub mod hexes;
pub mod location;
pub mod speedtest;

pub type Result<T = ()> = std::result::Result<T, Error>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("signal level {0:?} not allowed for {1:?}")]
    InvalidSignalLevel(SignalLevel, RadioType),
}

/// Necessary checks for calculating coverage points is done during [RewardableRadio::new].
#[derive(Debug, Clone)]
pub struct RewardableRadio {
    pub radio_type: RadioType,
    pub radio_threshold: RadioThreshold,
    pub boosted_hex_eligibility: BoostedHexStatus,
    speedtests: Speedtests,
    location_trust_scores: LocationTrustScores,
    covered_hexes: CoveredHexes,
}

/// Output of calculating coverage points for a [RewardableRadio].
///
/// The only data included was used for calculating coverage points.
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
#[derive(Debug)]
pub struct CoveragePoints {
    /// Value used when calculating poc_reward
    pub total_coverage_points: Decimal,
    /// Coverage Points collected from each Covered Hex
    pub hex_coverage_points: Decimal,
    /// Location Trust Multiplier, maximum of 1
    pub location_trust_multiplier: Decimal,
    /// Speedtest Mulitplier, maximum of 1
    pub speedtest_multiplier: Decimal,
}

pub fn calculate_coverage_points(radio: &RewardableRadio) -> CoveragePoints {
    let hex_coverage_points = radio.covered_hexes.calculated_coverage_points();
    let location_trust_multiplier = radio.location_trust_scores.multiplier;
    let speedtest_multiplier = radio.speedtests.multiplier;

    let coverage_points = hex_coverage_points * location_trust_multiplier * speedtest_multiplier;
    let total_coverage_points = coverage_points.round_dp_with_strategy(2, RoundingStrategy::ToZero);

    CoveragePoints {
        total_coverage_points,
        hex_coverage_points,
        location_trust_multiplier,
        speedtest_multiplier,
    }
}

impl RewardableRadio {
    pub fn new(
        radio_type: RadioType,
        speedtests: Vec<Speedtest>,
        location_trust_scores: Vec<LocationTrust>,
        radio_threshold: RadioThreshold,
        ranked_coverage: Vec<RankedCoverage>,
    ) -> Result<Self> {
        let location_trust_scores =
            LocationTrustScores::new(radio_type, location_trust_scores, &ranked_coverage);

        let boosted_hex_status = BoostedHexStatus::new(
            &radio_type,
            location_trust_scores.multiplier,
            &radio_threshold,
        );

        let covered_hexes = CoveredHexes::new(radio_type, ranked_coverage, boosted_hex_status)?;

        Ok(Self {
            radio_type,
            speedtests: Speedtests::new(speedtests),
            location_trust_scores,
            radio_threshold,
            covered_hexes,
            boosted_hex_eligibility: boosted_hex_status,
        })
    }
}

#[derive(Debug, Clone, Copy)]
pub enum BoostedHexStatus {
    Eligible,
    WifiLocationScoreBelowThreshold(Decimal),
    RadioThresholdNotMet,
}

impl BoostedHexStatus {
    fn new(
        radio_type: &RadioType,
        location_trust_score: Decimal,
        radio_threshold: &RadioThreshold,
    ) -> Self {
        // hip93: if radio is wifi & location_trust score multiplier < 0.75, no boosting
        if radio_type.is_wifi() && location_trust_score < dec!(0.75) {
            return Self::WifiLocationScoreBelowThreshold(location_trust_score);
        }

        // hip84: if radio has not met minimum data and subscriber thresholds, no boosting
        if !radio_threshold.is_met() {
            return Self::RadioThresholdNotMet;
        }

        Self::Eligible
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

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum RadioThreshold {
    Verified,
    Unverified,
}

impl RadioThreshold {
    fn is_met(&self) -> bool {
        matches!(self, Self::Verified)
    }
}

#[cfg(test)]
mod tests {

    use rstest::rstest;
    use speedtest::SpeedtestTier;

    use std::num::NonZeroU32;

    use crate::{location::Meters, speedtest::BytesPs};

    use super::*;
    use chrono::Utc;
    use hex_assignments::{assignment::HexAssignments, Assignment};
    use rust_decimal_macros::dec;

    #[test]
    fn hip_84_radio_meets_minimum_subscriber_threshold_for_boosted_hexes() {
        let make_wifi = |radio_verified: RadioThreshold| {
            RewardableRadio::new(
                RadioType::IndoorWifi,
                speedtest_maximum(),
                location_trust_maximum(),
                radio_verified,
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
        let verified_wifi = make_wifi(RadioThreshold::Verified);
        assert_eq!(
            base_points * dec!(5),
            calculate_coverage_points(verified_wifi.clone()).total_coverage_points
        );

        // Radio not meeting the threshold is not eligible for boosted hexes.
        // Boost from hex is not applied, radio receives base points.
        let unverified_wifi = make_wifi(RadioThreshold::Unverified);
        assert_eq!(
            base_points,
            calculate_coverage_points(unverified_wifi).total_coverage_points
        );
    }

    #[test]
    fn hip_93_wifi_with_low_location_score_receives_no_boosted_hexes() {
        let make_wifi = |location_trust_scores: Vec<LocationTrust>| {
            RewardableRadio::new(
                RadioType::IndoorWifi,
                speedtest_maximum(),
                location_trust_scores,
                RadioThreshold::Verified,
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
        let trusted_wifi = make_wifi(location_trust_with_scores(&[dec!(1), dec!(1)]));
        assert!(trusted_wifi.location_trust_scores.multiplier > dec!(0.75));
        assert!(
            calculate_coverage_points(trusted_wifi.clone()).total_coverage_points > base_points
        );

        // Radio with poor trust score is not eligible for boosted hexes.
        // Boost from hex is not applied, and points are further lowered by poor trust score.
        let untrusted_wifi = make_wifi(location_trust_with_scores(&[dec!(0.1), dec!(0.2)]));
        assert!(untrusted_wifi.location_trust_scores.multiplier < dec!(0.75));
        assert!(calculate_coverage_points(untrusted_wifi).total_coverage_points < base_points);
    }

    #[test]
    fn speedtest() {
        let make_indoor_cbrs = |speedtests: Vec<Speedtest>| {
            RewardableRadio::new(
                RadioType::IndoorCbrs,
                speedtests,
                location_trust_maximum(),
                RadioThreshold::Verified,
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

        let indoor_cbrs = make_indoor_cbrs(speedtest_maximum());
        assert_eq!(
            base_coverage_points * SpeedtestTier::Good.multiplier(),
            calculate_coverage_points(indoor_cbrs).total_coverage_points
        );

        let indoor_cbrs = make_indoor_cbrs(vec![
            speedtest_with_download(BytesPs::mbps(88)),
            speedtest_with_download(BytesPs::mbps(88)),
        ]);
        assert_eq!(
            base_coverage_points * SpeedtestTier::Acceptable.multiplier(),
            calculate_coverage_points(indoor_cbrs.clone()).total_coverage_points
        );

        let indoor_cbrs = make_indoor_cbrs(vec![
            speedtest_with_download(BytesPs::mbps(62)),
            speedtest_with_download(BytesPs::mbps(62)),
        ]);
        assert_eq!(
            base_coverage_points * SpeedtestTier::Degraded.multiplier(),
            calculate_coverage_points(indoor_cbrs).total_coverage_points
        );

        let indoor_cbrs = make_indoor_cbrs(vec![
            speedtest_with_download(BytesPs::mbps(42)),
            speedtest_with_download(BytesPs::mbps(42)),
        ]);
        assert_eq!(
            base_coverage_points * SpeedtestTier::Poor.multiplier(),
            calculate_coverage_points(indoor_cbrs).total_coverage_points
        );

        let indoor_cbrs = make_indoor_cbrs(vec![
            speedtest_with_download(BytesPs::mbps(25)),
            speedtest_with_download(BytesPs::mbps(25)),
        ]);
        assert_eq!(
            base_coverage_points * SpeedtestTier::Fail.multiplier(),
            calculate_coverage_points(indoor_cbrs).total_coverage_points
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
        let indoor_cbrs = RewardableRadio::new(
            RadioType::IndoorCbrs,
            speedtest_maximum(),
            location_trust_maximum(),
            RadioThreshold::Verified,
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

        assert_eq!(
            dec!(1073),
            calculate_coverage_points(indoor_cbrs).total_coverage_points
        );
    }

    #[test]
    fn outdoor_radios_consider_top_3_ranked_hexes() {
        let outdoor_wifi = RewardableRadio::new(
            RadioType::OutdoorWifi,
            speedtest_maximum(),
            location_trust_maximum(),
            RadioThreshold::Verified,
            vec![
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
                    rank: 2,
                    signal_level: SignalLevel::High,
                    assignments: assignments_maximum(),
                    boosted: None,
                },
                RankedCoverage {
                    hotspot_key: pubkey(),
                    cbsd_id: None,
                    hex: hex_location(),
                    rank: 3,
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
        .expect("outdoor wifi");

        // rank 1  :: 1.00 * 16 == 16
        // rank 2  :: 0.50 * 16 == 8
        // rank 3  :: 0.25 * 16 == 4
        // rank 42 :: 0.00 * 16 == 0
        assert_eq!(
            dec!(28),
            calculate_coverage_points(outdoor_wifi).total_coverage_points
        );
    }

    #[test]
    fn indoor_radios_only_consider_first_ranked_hexes() {
        let indoor_wifi = RewardableRadio::new(
            RadioType::IndoorWifi,
            speedtest_maximum(),
            location_trust_maximum(),
            RadioThreshold::Verified,
            vec![
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

        assert_eq!(
            dec!(400),
            calculate_coverage_points(indoor_wifi).total_coverage_points
        );
    }

    #[test]
    fn location_trust_score_multiplier() {
        // Location scores are averaged together
        let indoor_wifi = RewardableRadio::new(
            RadioType::IndoorWifi,
            speedtest_maximum(),
            location_trust_with_scores(&[dec!(0.1), dec!(0.2), dec!(0.3), dec!(0.4)]),
            RadioThreshold::Verified,
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
        assert_eq!(
            dec!(100),
            calculate_coverage_points(indoor_wifi).total_coverage_points
        );
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
        let indoor_wifi = RewardableRadio::new(
            RadioType::IndoorWifi,
            speedtest_maximum(),
            location_trust_maximum(),
            RadioThreshold::Verified,
            covered_hexes.clone(),
        )
        .expect("indoor wifi");

        // The hex with a low signal_level is boosted to the same level as a
        // signal_level of High.
        assert_eq!(
            dec!(800),
            calculate_coverage_points(indoor_wifi.clone()).total_coverage_points
        );
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
        let outdoor_cbrs = RewardableRadio::new(
            RadioType::OutdoorCbrs,
            speedtest_maximum(),
            location_trust_maximum(),
            RadioThreshold::Verified,
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

        assert_eq!(
            expected,
            calculate_coverage_points(outdoor_cbrs).total_coverage_points
        );
    }

    #[rstest]
    #[case(SignalLevel::High, dec!(100))]
    #[case(SignalLevel::Low, dec!(25))]
    fn indoor_cbrs_base_coverage_points(
        #[case] signal_level: SignalLevel,
        #[case] expected: Decimal,
    ) {
        let indoor_cbrs = RewardableRadio::new(
            RadioType::IndoorCbrs,
            speedtest_maximum(),
            location_trust_maximum(),
            RadioThreshold::Verified,
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

        assert_eq!(
            expected,
            calculate_coverage_points(indoor_cbrs).total_coverage_points
        );
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
        let outdoor_wifi = RewardableRadio::new(
            RadioType::IndoorCbrs,
            speedtest_maximum(),
            location_trust_maximum(),
            RadioThreshold::Verified,
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

        assert_eq!(
            expected,
            calculate_coverage_points(outdoor_wifi).total_coverage_points
        );
    }

    #[rstest]
    #[case(SignalLevel::High, dec!(400))]
    #[case(SignalLevel::Low, dec!(100))]
    fn indoor_wifi_base_coverage_points(
        #[case] signal_level: SignalLevel,
        #[case] expected: Decimal,
    ) {
        let indoor_wifi = RewardableRadio::new(
            RadioType::IndoorWifi,
            speedtest_maximum(),
            location_trust_maximum(),
            RadioThreshold::Verified,
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

        assert_eq!(
            expected,
            calculate_coverage_points(indoor_wifi).total_coverage_points
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

    fn speedtest_maximum() -> Vec<Speedtest> {
        vec![
            Speedtest {
                upload_speed: BytesPs::mbps(15),
                download_speed: BytesPs::mbps(150),
                latency: Millis::new(15),
                timestamp: Utc::now(),
            },
            Speedtest {
                upload_speed: BytesPs::mbps(15),
                download_speed: BytesPs::mbps(150),
                latency: Millis::new(15),
                timestamp: Utc::now(),
            },
        ]
    }

    fn speedtest_with_download(download: BytesPs) -> Speedtest {
        Speedtest {
            upload_speed: BytesPs::mbps(15),
            download_speed: download,
            latency: Millis::new(15),
            timestamp: Utc::now(),
        }
    }

    fn location_trust_maximum() -> Vec<LocationTrust> {
        vec![LocationTrust {
            distance_to_asserted: Meters::new(1),
            trust_score: dec!(1.0),
        }]
    }

    fn location_trust_with_scores(trust_scores: &[Decimal]) -> Vec<LocationTrust> {
        trust_scores
            .to_owned()
            .iter()
            .copied()
            .map(|trust_score| LocationTrust {
                distance_to_asserted: Meters::new(1),
                trust_score,
            })
            .collect()
    }

    fn pubkey() -> helium_crypto::PublicKeyBinary {
        helium_crypto::PublicKeyBinary::from_str(
            "112NqN2WWMwtK29PMzRby62fDydBJfsCLkCAf392stdok48ovNT6",
        )
        .expect("failed owner parse")
    }
}
