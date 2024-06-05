///
/// Many changes to the rewards algorithm are contained in and across many HIPs.
/// The blog post [MOBILE Proof of Coverage][mobile-poc-blog] contains a more
/// thorough explanation of many of them. It is not exhaustive, but a great
/// place to start.
///
/// ## Fields:
/// - modeled_coverage_points
///   - [HIP-74][modeled-coverage]
///   - reduced cbrs radio coverage points [HIP-113][cbrs-experimental]
///
/// - assignment_multiplier
///   - [HIP-103][oracle-boosting]
///
/// - rank
///   - [HIP-105][hex-limits]
///
/// - hex_boost_multiplier  
///   - must meet minimum subscriber thresholds [HIP-84][provider-boosting]
///   - Wifi Location trust score >0.75 for boosted hex eligibility [HIP-93][wifi-aps]
///
/// - location_trust_score_multiplier
///   - [HIP-98][qos-score]
///   - states 30m requirement for boosted hexes [HIP-107][prevent-gaming]
///   - increase Boosted hex restriction, 30m -> 50m [Pull Request][boosted-hex-restriction]
///
/// - speedtest_multiplier
///   - [HIP-74][modeled-coverage]
///   - added "Good" speedtest tier [HIP-98][qos-score]
///     - latency is explicitly under limit in HIP https://github.com/helium/oracles/pull/737
///
/// ## Notable Conditions:
/// - Location
///   - If a Radio covers any boosted hexes, [LocationTrust] scores must meet distance requirements, or be degraded.
///   - CBRS Radio's location is always trusted because of GPS.
///
/// - Speedtests
///   - The latest 6 speedtests will be used.
///   - There must be more than 2 speedtests.
///
/// - Covered Hexes
///   - If a Radio is not [eligible_for_boosted_hexes], boost values are removed before calculations. [CoveredHexes::new_without_boosts]
///
/// ## References:
/// [modeled-coverage]:        https://github.com/helium/HIP/blob/main/0074-mobile-poc-modeled-coverage-rewards.md#outdoor-radios
/// [provider-boosting]:       https://github.com/helium/HIP/blob/main/0084-service-provider-hex-boosting.md
/// [wifi-aps]:                https://github.com/helium/HIP/blob/main/0093-addition-of-wifi-aps-to-mobile-subdao.md
/// [qos-score]:               https://github.com/helium/HIP/blob/main/0098-mobile-subdao-quality-of-service-requirements.md
/// [oracle-boosting]:         https://github.com/helium/HIP/blob/main/0103-oracle-hex-boosting.md
/// [hex-limits]:              https://github.com/helium/HIP/blob/main/0105-modification-of-mobile-subdao-hex-limits.md
/// [prevent-gaming]:          https://github.com/helium/HIP/blob/main/0107-preventing-gaming-within-the-mobile-network.md
/// [cbrs-experimental]:       https://github.com/helium/HIP/blob/main/0113-reward-cbrs-as-experimental.md
/// [mobile-poc-blog]:         https://docs.helium.com/mobile/proof-of-coverage
/// [boosted-hex-restriction]: https://github.com/helium/oracles/pull/808
///
use crate::{
    hexes::{CoveredHex, CoveredHexes},
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
pub type MaxOneMultplier = Decimal;

/// Input Radio to calculation
#[derive(Debug, Clone)]
pub struct RewardableRadio {
    radio_type: RadioType,
    speedtests: Speedtests,
    location_trust_scores: LocationTrustScores,
    radio_threshold: RadioThreshold,
    covered_hexes: CoveredHexes,
    eligible_for_boosted_hexes: bool,
}

#[derive(Debug)]
pub struct CoveragePoints {
    /// Value used when calculating poc_reward
    pub total_coverage_points: Decimal,
    /// Coverage Points collected from each Covered Hex
    /// vvv turn into function call
    pub hex_coverage_points: Decimal,
    /// Location Trust Multiplier, maximum of 1
    pub location_trust_multiplier: Decimal,
    /// Speedtest Mulitplier, maximum of 1
    pub speedtest_multiplier: Decimal,
    // ---
    pub radio_type: RadioType,
    pub radio_threshold: RadioThreshold,
    pub speedtests: Vec<Speedtest>,
    pub location_trust_scores: Vec<LocationTrust>,
    pub covered_hexes: Vec<CoveredHex>,
    pub eligible_for_boosted_hexes: bool,
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("signal level {0:?} not allowed for {1:?}")]
    InvalidSignalLevel(SignalLevel, RadioType),
}

pub fn calculate_coverage_points(radio: RewardableRadio) -> CoveragePoints {
    let hex_coverage_points = radio.covered_hexes.accumulate_calculated_coverage_points();
    let location_trust_multiplier = radio.location_trust_scores.multiplier;
    let speedtest_multiplier = radio.speedtests.multiplier;

    let coverage_points = hex_coverage_points * location_trust_multiplier * speedtest_multiplier;
    let total_coverage_points = coverage_points.round_dp_with_strategy(2, RoundingStrategy::ToZero);

    CoveragePoints {
        total_coverage_points,
        hex_coverage_points,
        location_trust_multiplier,
        speedtest_multiplier,
        // Radio information
        radio_type: radio.radio_type,
        radio_threshold: radio.radio_threshold,
        speedtests: radio.speedtests.speedtests,
        location_trust_scores: radio.location_trust_scores.trust_scores,
        covered_hexes: radio.covered_hexes.hexes,
        eligible_for_boosted_hexes: radio.eligible_for_boosted_hexes,
    }
}

impl CoveragePoints {
    pub fn iter_boosted_hexes(&self) -> impl Iterator<Item = CoveredHex> {
        let eligible = self.eligible_for_boosted_hexes;

        self.covered_hexes
            .clone()
            .into_iter()
            .filter(move |_| eligible)
            .filter(|hex| hex.boosted_multiplier.is_some())
    }
}

impl RewardableRadio {
    pub fn new(
        radio_type: RadioType,
        speedtests: Vec<Speedtest>,
        location_trust_scores: Vec<LocationTrust>,
        radio_threshold: RadioThreshold,
        covered_hexes: Vec<RankedCoverage>,
    ) -> Result<Self> {
        // QUESTION: we need to know about boosted hexes to determine location multiplier.
        // The location multiplier is then used to determine if they are eligible for boosted hexes.
        // In the case where they cannot use boosted hexes, should the location mulitiplier be restored?

        let any_boosted_hexes = covered_hexes.iter().any(|hex| hex.boosted.is_some());
        let location_trust_scores = if any_boosted_hexes {
            LocationTrustScores::new_with_boosted_hexes(&radio_type, location_trust_scores)
        } else {
            LocationTrustScores::new(&radio_type, location_trust_scores)
        };

        let eligible_for_boosted_hexes = eligible_for_boosted_hexes(
            &radio_type,
            location_trust_scores.multiplier,
            &radio_threshold,
        );

        let covered_hexes = if eligible_for_boosted_hexes {
            CoveredHexes::new(&radio_type, covered_hexes)?
        } else {
            CoveredHexes::new_without_boosts(&radio_type, covered_hexes)?
        };

        Ok(Self {
            radio_type,
            speedtests: Speedtests::new(speedtests),
            location_trust_scores,
            radio_threshold,
            covered_hexes,
            eligible_for_boosted_hexes,
        })
    }

    pub fn radio_type(&self) -> RadioType {
        self.radio_type
    }
}

fn eligible_for_boosted_hexes(
    radio_type: &RadioType,
    location_trust_score: Decimal,
    radio_threshold: &RadioThreshold,
) -> bool {
    // hip93: if radio is wifi & location_trust score multiplier < 0.75, no boosting
    if radio_type.is_wifi() && location_trust_score < dec!(0.75) {
        return false;
    }

    // hip84: if radio has not met minimum data and subscriber thresholds, no boosting
    if !radio_threshold.threshold_met() {
        return false;
    }

    true
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

    fn rank_multipliers(&self) -> Vec<Decimal> {
        match self {
            RadioType::IndoorWifi => vec![dec!(1)],
            RadioType::IndoorCbrs => vec![dec!(1)],
            RadioType::OutdoorWifi => vec![dec!(1), dec!(0.5), dec!(0.25)],
            RadioType::OutdoorCbrs => vec![dec!(1), dec!(0.5), dec!(0.25)],
        }
    }

    pub fn is_wifi(&self) -> bool {
        matches!(self, Self::IndoorWifi | Self::OutdoorWifi)
    }

    pub fn is_cbrs(&self) -> bool {
        matches!(self, Self::IndoorCbrs | Self::OutdoorCbrs)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum RadioThreshold {
    Verified,
    UnVerified,
}

impl RadioThreshold {
    fn threshold_met(&self) -> bool {
        matches!(self, Self::Verified)
    }
}

#[cfg(test)]
mod tests {

    use std::{num::NonZeroU32, str::FromStr};

    use crate::{
        location::Meters,
        speedtest::{BytesPs, Millis},
    };

    use super::*;
    use chrono::Utc;
    use hex_assignments::{assignment::HexAssignments, Assignment};
    use rust_decimal_macros::dec;

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

    #[test]
    fn hip_84_radio_meets_minimum_subscriber_threshold_for_boosted_hexes() {
        let make_wifi = |location_trust_scores: Vec<LocationTrust>| {
            RewardableRadio::new(
                RadioType::IndoorWifi,
                Speedtest::maximum(),
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
        // Boosted Hex get's radio over the base_points
        let trusted_location = LocationTrust::with_trust_scores(&[dec!(1), dec!(1)]);
        let trusted_wifi = make_wifi(trusted_location);
        assert!(trusted_wifi.location_trust_scores.multiplier > dec!(0.75));
        assert!(
            calculate_coverage_points(trusted_wifi.clone()).total_coverage_points > base_points
        );

        // degraded location score get's radio under base_points
        let untrusted_location = LocationTrust::with_trust_scores(&[dec!(0.1), dec!(0.2)]);
        let untrusted_wifi = make_wifi(untrusted_location);
        assert!(untrusted_wifi.location_trust_scores.multiplier < dec!(0.75));
        assert!(calculate_coverage_points(untrusted_wifi).total_coverage_points < base_points);
    }

    #[test]
    fn hip_93_wifi_with_low_location_score_receives_no_boosted_hexes() {
        let make_wifi = |location_trust_scores: Vec<LocationTrust>| {
            RewardableRadio::new(
                RadioType::IndoorWifi,
                Speedtest::maximum(),
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
        // Boosted Hex get's radio over the base_points
        let trusted_location = LocationTrust::with_trust_scores(&[dec!(1), dec!(1)]);
        let trusted_wifi = make_wifi(trusted_location);
        assert!(trusted_wifi.location_trust_scores.multiplier > dec!(0.75));
        assert!(
            calculate_coverage_points(trusted_wifi.clone()).total_coverage_points > base_points
        );

        // degraded location score get's radio under base_points
        let untrusted_location = LocationTrust::with_trust_scores(&[dec!(0.1), dec!(0.2)]);
        let untrusted_wifi = make_wifi(untrusted_location);
        assert!(untrusted_wifi.location_trust_scores.multiplier < dec!(0.75));
        assert!(calculate_coverage_points(untrusted_wifi).total_coverage_points < base_points);
    }

    #[test]
    fn speedtest() {
        let make_indoor_cbrs = |speedtests: Vec<Speedtest>| {
            RewardableRadio::new(
                RadioType::IndoorCbrs,
                speedtests,
                LocationTrust::maximum(),
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

        let indoor_cbrs = make_indoor_cbrs(Speedtest::maximum());
        assert_eq!(
            dec!(100),
            calculate_coverage_points(indoor_cbrs).total_coverage_points
        );

        let indoor_cbrs = make_indoor_cbrs(vec![
            Speedtest::download(BytesPs::mbps(88)),
            Speedtest::download(BytesPs::mbps(88)),
        ]);
        assert_eq!(
            dec!(75),
            calculate_coverage_points(indoor_cbrs.clone()).total_coverage_points
        );

        let indoor_cbrs = make_indoor_cbrs(vec![
            Speedtest::download(BytesPs::mbps(62)),
            Speedtest::download(BytesPs::mbps(62)),
        ]);
        assert_eq!(
            dec!(50),
            calculate_coverage_points(indoor_cbrs).total_coverage_points
        );

        let indoor_cbrs = make_indoor_cbrs(vec![
            Speedtest::download(BytesPs::mbps(42)),
            Speedtest::download(BytesPs::mbps(42)),
        ]);
        assert_eq!(
            dec!(25),
            calculate_coverage_points(indoor_cbrs).total_coverage_points
        );

        let indoor_cbrs = make_indoor_cbrs(vec![
            Speedtest::download(BytesPs::mbps(25)),
            Speedtest::download(BytesPs::mbps(25)),
        ]);
        assert_eq!(
            dec!(0),
            calculate_coverage_points(indoor_cbrs).total_coverage_points
        );
    }

    #[test]
    fn oracle_boosting_assignments_apply_per_hex() {
        fn local_hex(
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
            Speedtest::maximum(),
            LocationTrust::maximum(),
            RadioThreshold::Verified,
            vec![
                // yellow - POI ≥ 1 Urbanized
                local_hex(A, A, A), // 100
                local_hex(A, B, A), // 100
                local_hex(A, C, A), // 100
                // orange - POI ≥ 1 Not Urbanized
                local_hex(A, A, B), // 100
                local_hex(A, B, B), // 100
                local_hex(A, C, B), // 100
                // light green - Point of Interest Urbanized
                local_hex(B, A, A), // 70
                local_hex(B, B, A), // 70
                local_hex(B, C, A), // 70
                // dark green - Point of Interest Not Urbanized
                local_hex(B, A, B), // 50
                local_hex(B, B, B), // 50
                local_hex(B, C, B), // 50
                // light blue - No POI Urbanized
                local_hex(C, A, A), // 40
                local_hex(C, B, A), // 30
                local_hex(C, C, A), // 5
                // dark blue - No POI Not Urbanized
                local_hex(C, A, B), // 20
                local_hex(C, B, B), // 15
                local_hex(C, C, B), // 3
                // gray - Outside of USA
                local_hex(A, A, C), // 0
                local_hex(A, B, C), // 0
                local_hex(A, C, C), // 0
                local_hex(B, A, C), // 0
                local_hex(B, B, C), // 0
                local_hex(B, C, C), // 0
                local_hex(C, A, C), // 0
                local_hex(C, B, C), // 0
                local_hex(C, C, C), // 0
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
            Speedtest::maximum(),
            LocationTrust::maximum(),
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
            Speedtest::maximum(),
            LocationTrust::maximum(),
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
            Speedtest::maximum(),
            LocationTrust::with_trust_scores(&[dec!(0.1), dec!(0.2), dec!(0.3), dec!(0.4)]),
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
            Speedtest::maximum(),
            LocationTrust::maximum(),
            RadioThreshold::Verified,
            covered_hexes.clone(),
        )
        .expect("verified indoor wifi");
        // The hex with a low signal_level is boosted to the same level as a
        // signal_level of High.
        assert_eq!(
            dec!(800),
            calculate_coverage_points(indoor_wifi.clone()).total_coverage_points
        );

        // When the radio is not verified for boosted rewards, the boost has no effect.
        let indoor_wifi = RewardableRadio::new(
            RadioType::IndoorWifi,
            Speedtest::maximum(),
            LocationTrust::maximum(),
            RadioThreshold::UnVerified,
            covered_hexes,
        )
        .expect("unverified indoor wifi");
        assert_eq!(
            dec!(500),
            calculate_coverage_points(indoor_wifi).total_coverage_points
        );
    }

    #[test]
    fn base_radio_coverage_points() {
        let outdoor_cbrs = RewardableRadio::new(
            RadioType::OutdoorCbrs,
            Speedtest::maximum(),
            LocationTrust::maximum(),
            RadioThreshold::Verified,
            vec![
                RankedCoverage {
                    hotspot_key: pubkey(),
                    cbsd_id: Some("serial".to_string()),
                    hex: hex_location(),
                    rank: 1,
                    signal_level: SignalLevel::High,
                    assignments: assignments_maximum(),
                    boosted: None,
                },
                RankedCoverage {
                    hotspot_key: pubkey(),
                    cbsd_id: Some("serial".to_string()),
                    hex: hex_location(),
                    rank: 1,
                    signal_level: SignalLevel::Medium,
                    assignments: assignments_maximum(),
                    boosted: None,
                },
                RankedCoverage {
                    hotspot_key: pubkey(),
                    cbsd_id: Some("serial".to_string()),
                    hex: hex_location(),
                    rank: 1,
                    signal_level: SignalLevel::Low,
                    assignments: assignments_maximum(),
                    boosted: None,
                },
                RankedCoverage {
                    hotspot_key: pubkey(),
                    cbsd_id: Some("serial".to_string()),
                    hex: hex_location(),
                    rank: 1,
                    signal_level: SignalLevel::None,
                    assignments: assignments_maximum(),
                    boosted: None,
                },
            ],
        )
        .expect("outdoor cbrs");

        let indoor_cbrs = RewardableRadio::new(
            RadioType::IndoorCbrs,
            Speedtest::maximum(),
            LocationTrust::maximum(),
            RadioThreshold::Verified,
            vec![
                RankedCoverage {
                    hotspot_key: pubkey(),
                    cbsd_id: Some("serial".to_string()),
                    hex: hex_location(),
                    rank: 1,
                    signal_level: SignalLevel::High,
                    assignments: assignments_maximum(),
                    boosted: None,
                },
                RankedCoverage {
                    hotspot_key: pubkey(),
                    cbsd_id: Some("serial".to_string()),
                    hex: hex_location(),
                    rank: 1,
                    signal_level: SignalLevel::Low,
                    assignments: assignments_maximum(),
                    boosted: None,
                },
            ],
        )
        .expect("indoor cbrs");

        let outdoor_wifi = RewardableRadio::new(
            RadioType::OutdoorWifi,
            Speedtest::maximum(),
            LocationTrust::maximum(),
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
                    rank: 1,
                    signal_level: SignalLevel::Medium,
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
                    boosted: None,
                },
                RankedCoverage {
                    hotspot_key: pubkey(),
                    cbsd_id: None,
                    hex: hex_location(),
                    rank: 1,
                    signal_level: SignalLevel::None,
                    assignments: assignments_maximum(),
                    boosted: None,
                },
            ],
        )
        .expect("outdoor wifi");

        let indoor_wifi = RewardableRadio::new(
            RadioType::IndoorWifi,
            Speedtest::maximum(),
            LocationTrust::maximum(),
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
                    rank: 1,
                    signal_level: SignalLevel::Low,
                    assignments: assignments_maximum(),
                    boosted: None,
                },
            ],
        )
        .expect("indoor wifi");

        // When each radio contains a hex of every applicable signal_level, and
        // multipliers are break even. These are the accumulated coverage points.
        assert_eq!(
            dec!(7),
            calculate_coverage_points(outdoor_cbrs).total_coverage_points
        );
        assert_eq!(
            dec!(125),
            calculate_coverage_points(indoor_cbrs).total_coverage_points
        );
        assert_eq!(
            dec!(28),
            calculate_coverage_points(outdoor_wifi).total_coverage_points
        );
        assert_eq!(
            dec!(500),
            calculate_coverage_points(indoor_wifi).total_coverage_points
        );
    }

    impl Speedtest {
        fn maximum() -> Vec<Self> {
            vec![
                Self {
                    upload_speed: BytesPs::mbps(15),
                    download_speed: BytesPs::mbps(150),
                    latency: Millis::new(15),
                    timestamp: Utc::now(),
                },
                Self {
                    upload_speed: BytesPs::mbps(15),
                    download_speed: BytesPs::mbps(150),
                    latency: Millis::new(15),
                    timestamp: Utc::now(),
                },
            ]
        }

        fn download(download: BytesPs) -> Self {
            Self {
                upload_speed: BytesPs::mbps(15),
                download_speed: download,
                latency: Millis::new(15),
                timestamp: Utc::now(),
            }
        }
    }

    impl LocationTrust {
        fn maximum() -> Vec<LocationTrust> {
            vec![LocationTrust {
                distance_to_asserted: Meters::new(1),
                trust_score: dec!(1.0),
            }]
        }

        fn with_trust_scores(trust_scores: &[Decimal]) -> Vec<LocationTrust> {
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
    }

    fn pubkey() -> helium_crypto::PublicKeyBinary {
        helium_crypto::PublicKeyBinary::from_str(
            "112NqN2WWMwtK29PMzRby62fDydBJfsCLkCAf392stdok48ovNT6",
        )
        .expect("failed owner parse")
    }
}
