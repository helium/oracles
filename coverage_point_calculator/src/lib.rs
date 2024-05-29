///
/// Many changes to the rewards algorithm are contained in and across many HIPs.
/// The blog post [MOBILE Proof of Coverage][mobile-poc-blog] contains a more
/// thorough explanation of many of them. It is not exhaustive, but a great
/// place to start.
///
/// ## Fields:
/// - estimated_coverage_points
///   - [HIP-74][modeled-coverage]
///   - reduced cbrs radio coverage points [HIP-113][cbrs-experimental]
/// - assignment_multiplier
///   - [HIP-103][oracle-boosting]
/// - rank
///   - [HIP-105][hex-limits]
/// - hex_boost_multiplier  
///   - must meet minimum subscriber thresholds [HIP-84][provider-boosting]
///   - Wifi Location trust score >0.75 for boosted hex eligibility [HIP-93][wifi-aps]
/// - location_trust_score_multiplier
///   - [HIP-98][qos-score]
///   - increase Boosted hex restriction, 30m -> 50m [HIP-93][boosted-hex-restriction]
/// - speedtest_multiplier
///   - [HIP-74][modeled-coverage]
///   - added "Good" speedtest tier [HIP-98][qos-score]
///     - latency is explicitly under limit in HIP https://github.com/helium/oracles/pull/737
///
/// ## References:
/// [modeled-coverage]:        https://github.com/helium/HIP/blob/main/0074-mobile-poc-modeled-coverage-rewards.md#outdoor-radios
/// [cbrs-experimental]:       https://github.com/helium/HIP/blob/main/0113-reward-cbrs-as-experimental.md
/// [oracle-boosting]:         https://github.com/helium/HIP/blob/main/0103-oracle-hex-boosting.md
/// [hex-limits]:              https://github.com/helium/HIP/blob/main/0105-modification-of-mobile-subdao-hex-limits.md
/// [provider-boosting]:       https://github.com/helium/HIP/blob/main/0084-service-provider-hex-boosting.md
/// [qos-score]:               https://github.com/helium/HIP/blob/main/0098-mobile-subdao-quality-of-service-requirements.md
/// [wifi-aps]:                https://github.com/helium/HIP/blob/main/0093-addition-of-wifi-aps-to-mobile-subdao.md
/// [mobile-poc-blog]:         https://docs.helium.com/mobile/proof-of-coverage
/// [boosted-hex-restriction]: https://github.com/helium/oracles/pull/808
///
/// To Integrate in Docs:
///
/// Some verbiage about ranks.
/// https://github.com/helium/HIP/blob/main/0105-modification-of-mobile-subdao-hex-limits.md
///
/// Has something to say about 30meters from asserted location wrt poc rewards
/// for boosted hexes.
/// https://github.com/helium/HIP/blob/8b1e814afa61a714b5ba63d3265e5897ab4c5116/0107-preventing-gaming-within-the-mobile-network.md
///
use crate::{
    location::{LocationTrust, LocationTrustScores},
    speedtest::{Speedtest, SpeedtestTier},
};
use rust_decimal::{Decimal, RoundingStrategy};
use rust_decimal_macros::dec;

pub mod location;
pub mod speedtest;

pub type Rank = std::num::NonZeroUsize;
pub type Multiplier = std::num::NonZeroU32;
pub type MaxOneMultplier = Decimal;
type Points = Decimal;

pub trait Radio<Key> {
    fn key(&self) -> Key;
    fn radio_type(&self) -> RadioType;
    fn speedtests(&self) -> Vec<Speedtest>;
    fn location_trust_scores(&self) -> Vec<LocationTrust>;
    fn verified_radio_threshold(&self) -> SubscriberThreshold;
}

pub trait CoverageMap<Key> {
    fn hexes(&self, radio: &impl Radio<Key>) -> Vec<CoveredHex>;
}

pub fn calculate_coverage_points(radio: RewardableRadio) -> CoveragePoints {
    let radio_type = &radio.radio_type;

    let rank_multipliers = radio_type.rank_multipliers();
    let max_rank = rank_multipliers.len();

    let hex_points = radio
        .covered_hexes
        .hexes
        .iter()
        .filter(|hex| hex.rank.get() <= max_rank)
        .map(|hex| {
            let base_coverage_points = radio_type.base_coverage_points(&hex.signal_level);
            let assignments_multiplier = hex.assignments.multiplier();
            let rank_multiplier = rank_multipliers[hex.rank.get() - 1];
            let hex_boost_multiplier = radio.hex_boosting_multiplier(hex);

            base_coverage_points * assignments_multiplier * rank_multiplier * hex_boost_multiplier
        });

    let base_points = hex_points.sum::<Decimal>();
    let location_score = radio.location_trust_multiplier();
    let speedtest = radio.speedtest_multiplier();

    let coverage_points = base_points * location_score * speedtest;
    let coverage_points = coverage_points.round_dp_with_strategy(2, RoundingStrategy::ToZero);

    CoveragePoints {
        coverage_points,
        radio,
    }
}

pub fn make_rewardable_radios<'a, K>(
    radios: &'a [impl Radio<K>],
    coverage_map: &'a impl CoverageMap<K>,
) -> impl Iterator<Item = RewardableRadio> + 'a {
    radios
        .iter()
        .map(|radio| make_rewardable_radio(radio, coverage_map))
}

pub fn make_rewardable_radio<K>(
    radio: &impl Radio<K>,
    coverage_map: &impl CoverageMap<K>,
) -> RewardableRadio {
    RewardableRadio {
        radio_type: radio.radio_type(),
        speedtests: radio.speedtests(),
        location_trust_scores: LocationTrustScores::new(radio.location_trust_scores()),
        verified_radio_threshold: radio.verified_radio_threshold(),
        covered_hexes: CoveredHexes::new(coverage_map.hexes(radio)),
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
    fn base_coverage_points(&self, signal_level: &SignalLevel) -> Points {
        match self {
            RadioType::IndoorWifi => match signal_level {
                SignalLevel::High => dec!(400),
                SignalLevel::Low => dec!(100),
                other => panic!("indoor wifi radios cannot have {other:?} signal levels"),
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
                other => panic!("indoor cbrs radios cannot have {other:?} signal levels"),
            },
            RadioType::OutdoorCbrs => match signal_level {
                SignalLevel::High => dec!(4),
                SignalLevel::Medium => dec!(2),
                SignalLevel::Low => dec!(1),
                SignalLevel::None => dec!(0),
            },
        }
    }

    fn rank_multipliers(&self) -> Vec<Decimal> {
        match self {
            RadioType::IndoorWifi => vec![dec!(1)],
            RadioType::IndoorCbrs => vec![dec!(1)],
            RadioType::OutdoorWifi => vec![dec!(1), dec!(0.5), dec!(0.25)],
            RadioType::OutdoorCbrs => vec![dec!(1), dec!(0.5), dec!(0.25)],
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SignalLevel {
    High,
    Medium,
    Low,
    None,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Assignments {
    pub footfall: Assignment,
    pub landtype: Assignment,
    pub urbanized: Assignment,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Assignment {
    A,
    B,
    C,
}

impl Assignments {
    fn multiplier(&self) -> MaxOneMultplier {
        let Assignments {
            footfall,
            urbanized,
            landtype,
        } = self;

        use Assignment::*;
        match (footfall, landtype, urbanized) {
            // yellow - POI ≥ 1 Urbanized
            (A, A, A) => dec!(1.00),
            (A, B, A) => dec!(1.00),
            (A, C, A) => dec!(1.00),
            // orange - POI ≥ 1 Not Urbanized
            (A, A, B) => dec!(1.00),
            (A, B, B) => dec!(1.00),
            (A, C, B) => dec!(1.00),
            // light green - Point of Interest Urbanized
            (B, A, A) => dec!(0.70),
            (B, B, A) => dec!(0.70),
            (B, C, A) => dec!(0.70),
            // dark green - Point of Interest Not Urbanized
            (B, A, B) => dec!(0.50),
            (B, B, B) => dec!(0.50),
            (B, C, B) => dec!(0.50),
            // light blue - No POI Urbanized
            (C, A, A) => dec!(0.40),
            (C, B, A) => dec!(0.30),
            (C, C, A) => dec!(0.05),
            // dark blue - No POI Not Urbanized
            (C, A, B) => dec!(0.20),
            (C, B, B) => dec!(0.15),
            (C, C, B) => dec!(0.03),
            // gray - Outside of USA
            (_, _, C) => dec!(0.00),
        }
    }
}

#[derive(Debug)]
pub struct CoveragePoints {
    pub coverage_points: Decimal,
    pub radio: RewardableRadio,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SubscriberThreshold {
    Verified,
    UnVerified,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RewardableRadio {
    pub radio_type: RadioType,
    pub speedtests: Vec<Speedtest>,
    pub location_trust_scores: LocationTrustScores,
    pub verified_radio_threshold: SubscriberThreshold,
    pub covered_hexes: CoveredHexes,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CoveredHexes {
    any_boosted: bool,
    hexes: Vec<CoveredHex>,
}

impl CoveredHexes {
    fn new(covered_hexes: Vec<CoveredHex>) -> Self {
        let any_boosted = covered_hexes.iter().any(|hex| hex.boosted.is_some());
        Self {
            any_boosted,
            hexes: covered_hexes,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct CoveredHex {
    pub rank: Rank,
    pub signal_level: SignalLevel,
    pub assignments: Assignments,
    pub boosted: Option<Multiplier>,
}

impl RewardableRadio {
    fn location_trust_multiplier(&self) -> Decimal {
        // CBRS radios are always trusted because they have internal GPS
        if self.is_cbrs() {
            return dec!(1);
        }

        if self.any_hexes_boosted() {
            self.location_trust_scores.any_hex_boosted_multiplier
        } else {
            self.location_trust_scores.no_boosted_hex_multiplier
        }
    }

    fn hex_boosting_multiplier(&self, hex: &CoveredHex) -> MaxOneMultplier {
        // need to consider requirements from hip93 & hip84 before applying any boost
        // hip93: if radio is wifi & location_trust score multiplier < 0.75, no boosting
        if self.is_wifi() && self.location_trust_multiplier() < dec!(0.75) {
            return dec!(1);
        }
        // hip84: if radio has not met minimum data and subscriber thresholds, no boosting
        if !self.subscriber_threshold_met() {
            return dec!(1);
        }

        let boost = hex.boosted.map_or(1, |boost| boost.get());
        Decimal::from(boost)
    }

    fn speedtest_multiplier(&self) -> MaxOneMultplier {
        const MIN_REQUIRED_SPEEDTEST_SAMPLES: usize = 2;

        if self.speedtests.len() < MIN_REQUIRED_SPEEDTEST_SAMPLES {
            return SpeedtestTier::Fail.multiplier();
        }

        let speedtest_avg = Speedtest::avg(&self.speedtests);
        speedtest_avg.multiplier()
    }

    fn any_hexes_boosted(&self) -> bool {
        self.covered_hexes.any_boosted
    }

    fn is_wifi(&self) -> bool {
        matches!(
            self.radio_type,
            RadioType::IndoorWifi | RadioType::OutdoorWifi
        )
    }

    fn is_cbrs(&self) -> bool {
        matches!(
            self.radio_type,
            RadioType::IndoorCbrs | RadioType::OutdoorCbrs
        )
    }

    fn subscriber_threshold_met(&self) -> bool {
        matches!(self.verified_radio_threshold, SubscriberThreshold::Verified)
    }
}

#[cfg(test)]
mod tests {

    use crate::{
        location::Meters,
        speedtest::{BytesPs, Millis},
    };

    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn hip_84_radio_meets_minimum_subscriber_threshold_for_boosted_hexes() {
        let trusted_location = LocationTrustScores::with_trust_scores(&[dec!(1), dec!(1)]);
        let untrusted_location = LocationTrustScores::with_trust_scores(&[dec!(0.1), dec!(0.2)]);
        let mut wifi = RewardableRadio {
            radio_type: RadioType::IndoorWifi,
            speedtests: Speedtest::maximum(),
            location_trust_scores: trusted_location,
            verified_radio_threshold: SubscriberThreshold::Verified,
            covered_hexes: CoveredHexes::new(vec![CoveredHex {
                rank: Rank::new(1).unwrap(),
                signal_level: SignalLevel::High,
                assignments: Assignments::maximum(),
                boosted: Multiplier::new(5),
            }]),
        };

        let base_points = RadioType::IndoorWifi.base_coverage_points(&SignalLevel::High);
        // Boosted Hex get's radio over the base_points
        assert!(wifi.location_trust_multiplier() > dec!(0.75));
        assert!(calculate_coverage_points(wifi.clone()).coverage_points > base_points);

        // degraded location score get's radio under base_points
        wifi.location_trust_scores = untrusted_location;
        assert!(wifi.location_trust_multiplier() < dec!(0.75));
        assert!(calculate_coverage_points(wifi).coverage_points < base_points);
    }

    #[test]
    fn hip_93_wifi_with_low_location_score_receives_no_boosted_hexes() {
        let trusted_location = LocationTrustScores::with_trust_scores(&[dec!(1), dec!(1)]);
        let untrusted_location = LocationTrustScores::with_trust_scores(&[dec!(0.1), dec!(0.2)]);
        let mut wifi = RewardableRadio {
            radio_type: RadioType::IndoorWifi,
            speedtests: Speedtest::maximum(),
            location_trust_scores: trusted_location,
            verified_radio_threshold: SubscriberThreshold::Verified,
            covered_hexes: CoveredHexes::new(vec![CoveredHex {
                rank: Rank::new(1).unwrap(),
                signal_level: SignalLevel::High,
                assignments: Assignments::maximum(),
                boosted: Multiplier::new(5),
            }]),
        };

        let base_points = RadioType::IndoorWifi.base_coverage_points(&SignalLevel::High);
        // Boosted Hex get's radio over the base_points
        assert!(wifi.location_trust_multiplier() > dec!(0.75));
        assert!(calculate_coverage_points(wifi.clone()).coverage_points > base_points);

        // degraded location score get's radio under base_points
        wifi.location_trust_scores = untrusted_location;
        assert!(wifi.location_trust_multiplier() < dec!(0.75));
        assert!(calculate_coverage_points(wifi).coverage_points < base_points);
    }

    #[test]
    fn speedtest() {
        let mut indoor_cbrs = RewardableRadio {
            radio_type: RadioType::IndoorCbrs,
            speedtests: Speedtest::maximum(),
            location_trust_scores: LocationTrustScores::maximum(),
            verified_radio_threshold: SubscriberThreshold::Verified,
            covered_hexes: CoveredHexes::new(vec![CoveredHex {
                rank: Rank::new(1).unwrap(),
                signal_level: SignalLevel::High,
                assignments: Assignments::maximum(),
                boosted: None,
            }]),
        };

        assert_eq!(
            dec!(100),
            calculate_coverage_points(indoor_cbrs.clone()).coverage_points
        );

        indoor_cbrs.speedtests = vec![
            Speedtest::download(BytesPs::mbps(88)),
            Speedtest::download(BytesPs::mbps(88)),
        ];
        assert_eq!(
            dec!(75),
            calculate_coverage_points(indoor_cbrs.clone()).coverage_points
        );

        indoor_cbrs.speedtests = vec![
            Speedtest::download(BytesPs::mbps(62)),
            Speedtest::download(BytesPs::mbps(62)),
        ];
        assert_eq!(
            dec!(50),
            calculate_coverage_points(indoor_cbrs.clone()).coverage_points
        );

        indoor_cbrs.speedtests = vec![
            Speedtest::download(BytesPs::mbps(42)),
            Speedtest::download(BytesPs::mbps(42)),
        ];
        assert_eq!(
            dec!(25),
            calculate_coverage_points(indoor_cbrs.clone()).coverage_points
        );

        indoor_cbrs.speedtests = vec![
            Speedtest::download(BytesPs::mbps(25)),
            Speedtest::download(BytesPs::mbps(25)),
        ];
        assert_eq!(
            dec!(0),
            calculate_coverage_points(indoor_cbrs).coverage_points
        );
    }

    #[test]
    fn oracle_boosting_assignments_apply_per_hex() {
        fn local_hex(
            footfall: Assignment,
            landtype: Assignment,
            urbanized: Assignment,
        ) -> CoveredHex {
            CoveredHex {
                rank: Rank::new(1).unwrap(),
                signal_level: SignalLevel::High,
                assignments: Assignments {
                    footfall,
                    landtype,
                    urbanized,
                },
                boosted: None,
            }
        }

        use Assignment::*;
        let indoor_cbrs = RewardableRadio {
            radio_type: RadioType::IndoorCbrs,
            speedtests: Speedtest::maximum(),
            location_trust_scores: LocationTrustScores::maximum(),
            verified_radio_threshold: SubscriberThreshold::Verified,
            covered_hexes: CoveredHexes::new(vec![
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
            ]),
        };

        assert_eq!(
            dec!(1073),
            calculate_coverage_points(indoor_cbrs).coverage_points
        );
    }

    #[test]
    fn outdoor_radios_consider_top_3_ranked_hexes() {
        let outdoor_wifi = RewardableRadio {
            radio_type: RadioType::OutdoorWifi,
            speedtests: Speedtest::maximum(),
            location_trust_scores: LocationTrustScores::maximum(),
            verified_radio_threshold: SubscriberThreshold::Verified,
            covered_hexes: CoveredHexes::new(vec![
                CoveredHex {
                    rank: Rank::new(1).unwrap(),
                    signal_level: SignalLevel::High,
                    assignments: Assignments::maximum(),
                    boosted: None,
                },
                CoveredHex {
                    rank: Rank::new(2).unwrap(),
                    signal_level: SignalLevel::High,
                    assignments: Assignments::maximum(),
                    boosted: None,
                },
                CoveredHex {
                    rank: Rank::new(3).unwrap(),
                    signal_level: SignalLevel::High,
                    assignments: Assignments::maximum(),
                    boosted: None,
                },
                CoveredHex {
                    rank: Rank::new(42).unwrap(),
                    signal_level: SignalLevel::High,
                    assignments: Assignments::maximum(),
                    boosted: None,
                },
            ]),
        };

        // rank 1  :: 1.00 * 16 == 16
        // rank 2  :: 0.50 * 16 == 8
        // rank 3  :: 0.25 * 16 == 4
        // rank 42 :: 0.00 * 16 == 0
        assert_eq!(
            dec!(28),
            calculate_coverage_points(outdoor_wifi).coverage_points
        );
    }

    #[test]
    fn indoor_radios_only_consider_first_ranked_hexes() {
        let indoor_wifi = RewardableRadio {
            radio_type: RadioType::IndoorWifi,
            speedtests: Speedtest::maximum(),
            location_trust_scores: LocationTrustScores::maximum(),
            verified_radio_threshold: SubscriberThreshold::Verified,
            covered_hexes: CoveredHexes::new(vec![
                CoveredHex {
                    rank: Rank::new(1).unwrap(),
                    signal_level: SignalLevel::High,
                    assignments: Assignments::maximum(),
                    boosted: None,
                },
                CoveredHex {
                    rank: Rank::new(2).unwrap(),
                    signal_level: SignalLevel::High,
                    assignments: Assignments::maximum(),
                    boosted: None,
                },
                CoveredHex {
                    rank: Rank::new(42).unwrap(),
                    signal_level: SignalLevel::High,
                    assignments: Assignments::maximum(),
                    boosted: None,
                },
            ]),
        };

        assert_eq!(
            dec!(400),
            calculate_coverage_points(indoor_wifi).coverage_points
        );
    }

    #[test]
    fn location_trust_score_multiplier() {
        // Location scores are averaged together
        let indoor_wifi = RewardableRadio {
            radio_type: RadioType::IndoorWifi,
            speedtests: Speedtest::maximum(),
            location_trust_scores: LocationTrustScores::with_trust_scores(&[
                dec!(0.1),
                dec!(0.2),
                dec!(0.3),
                dec!(0.4),
            ]),
            verified_radio_threshold: SubscriberThreshold::Verified,
            covered_hexes: CoveredHexes::new(vec![CoveredHex {
                rank: Rank::new(1).unwrap(),
                signal_level: SignalLevel::High,
                assignments: Assignments::maximum(),
                boosted: None,
            }]),
        };

        // Location trust scores is 1/4
        assert_eq!(
            dec!(100),
            calculate_coverage_points(indoor_wifi).coverage_points
        );
    }

    #[test]
    fn boosted_hex() {
        let mut indoor_wifi = RewardableRadio {
            radio_type: RadioType::IndoorWifi,
            speedtests: Speedtest::maximum(),
            location_trust_scores: LocationTrustScores::maximum(),
            verified_radio_threshold: SubscriberThreshold::Verified,
            covered_hexes: CoveredHexes::new(vec![
                CoveredHex {
                    rank: Rank::new(1).unwrap(),
                    signal_level: SignalLevel::High,
                    assignments: Assignments::maximum(),
                    boosted: None,
                },
                CoveredHex {
                    rank: Rank::new(1).unwrap(),
                    signal_level: SignalLevel::Low,
                    assignments: Assignments::maximum(),
                    boosted: Multiplier::new(4),
                },
            ]),
        };
        // The hex with a low signal_level is boosted to the same level as a
        // signal_level of High.
        assert_eq!(
            dec!(800),
            calculate_coverage_points(indoor_wifi.clone()).coverage_points
        );

        // When the radio is not verified for boosted rewards, the boost has no effect.
        indoor_wifi.verified_radio_threshold = SubscriberThreshold::UnVerified;
        assert_eq!(
            dec!(500),
            calculate_coverage_points(indoor_wifi).coverage_points
        );
    }

    #[test]
    fn base_radio_coverage_points() {
        let outdoor_cbrs = RewardableRadio {
            radio_type: RadioType::OutdoorCbrs,
            speedtests: Speedtest::maximum(),
            location_trust_scores: LocationTrustScores::maximum(),
            verified_radio_threshold: SubscriberThreshold::Verified,
            covered_hexes: CoveredHexes::new(vec![
                CoveredHex {
                    rank: Rank::new(1).unwrap(),
                    signal_level: SignalLevel::High,
                    assignments: Assignments::maximum(),
                    boosted: None,
                },
                CoveredHex {
                    rank: Rank::new(1).unwrap(),
                    signal_level: SignalLevel::Medium,
                    assignments: Assignments::maximum(),
                    boosted: None,
                },
                CoveredHex {
                    rank: Rank::new(1).unwrap(),
                    signal_level: SignalLevel::Low,
                    assignments: Assignments::maximum(),
                    boosted: None,
                },
                CoveredHex {
                    rank: Rank::new(1).unwrap(),
                    signal_level: SignalLevel::None,
                    assignments: Assignments::maximum(),
                    boosted: None,
                },
            ]),
        };

        let indoor_cbrs = RewardableRadio {
            radio_type: RadioType::IndoorCbrs,
            speedtests: Speedtest::maximum(),
            location_trust_scores: LocationTrustScores::maximum(),
            verified_radio_threshold: SubscriberThreshold::Verified,
            covered_hexes: CoveredHexes::new(vec![
                CoveredHex {
                    rank: Rank::new(1).unwrap(),
                    signal_level: SignalLevel::High,
                    assignments: Assignments::maximum(),
                    boosted: None,
                },
                CoveredHex {
                    rank: Rank::new(1).unwrap(),
                    signal_level: SignalLevel::Low,
                    assignments: Assignments::maximum(),
                    boosted: None,
                },
            ]),
        };

        let outdoor_wifi = RewardableRadio {
            radio_type: RadioType::OutdoorWifi,
            speedtests: Speedtest::maximum(),
            location_trust_scores: LocationTrustScores::maximum(),
            verified_radio_threshold: SubscriberThreshold::Verified,
            covered_hexes: CoveredHexes::new(vec![
                CoveredHex {
                    rank: Rank::new(1).unwrap(),
                    signal_level: SignalLevel::High,
                    assignments: Assignments::maximum(),
                    boosted: None,
                },
                CoveredHex {
                    rank: Rank::new(1).unwrap(),
                    signal_level: SignalLevel::Medium,
                    assignments: Assignments::maximum(),
                    boosted: None,
                },
                CoveredHex {
                    rank: Rank::new(1).unwrap(),
                    signal_level: SignalLevel::Low,
                    assignments: Assignments::maximum(),
                    boosted: None,
                },
                CoveredHex {
                    rank: Rank::new(1).unwrap(),
                    signal_level: SignalLevel::None,
                    assignments: Assignments::maximum(),
                    boosted: None,
                },
            ]),
        };

        let indoor_wifi = RewardableRadio {
            radio_type: RadioType::IndoorWifi,
            speedtests: Speedtest::maximum(),
            location_trust_scores: LocationTrustScores::maximum(),
            verified_radio_threshold: SubscriberThreshold::Verified,
            covered_hexes: CoveredHexes::new(vec![
                CoveredHex {
                    rank: Rank::new(1).unwrap(),
                    signal_level: SignalLevel::High,
                    assignments: Assignments::maximum(),
                    boosted: None,
                },
                CoveredHex {
                    rank: Rank::new(1).unwrap(),
                    signal_level: SignalLevel::Low,
                    assignments: Assignments::maximum(),
                    boosted: None,
                },
            ]),
        };

        // When each radio contains a hex of every applicable signal_level, and
        // multipliers are break even. These are the accumulated coverage points.
        assert_eq!(
            dec!(7),
            calculate_coverage_points(outdoor_cbrs).coverage_points
        );
        assert_eq!(
            dec!(125),
            calculate_coverage_points(indoor_cbrs).coverage_points
        );
        assert_eq!(
            dec!(28),
            calculate_coverage_points(outdoor_wifi).coverage_points
        );
        assert_eq!(
            dec!(500),
            calculate_coverage_points(indoor_wifi).coverage_points
        );
    }

    impl Assignments {
        fn maximum() -> Self {
            Self {
                footfall: Assignment::A,
                landtype: Assignment::A,
                urbanized: Assignment::A,
            }
        }
    }

    impl Speedtest {
        fn maximum() -> Vec<Self> {
            vec![
                Self {
                    upload_speed: BytesPs::mbps(15),
                    download_speed: BytesPs::mbps(150),
                    latency: Millis::new(15),
                },
                Self {
                    upload_speed: BytesPs::mbps(15),
                    download_speed: BytesPs::mbps(150),
                    latency: Millis::new(15),
                },
            ]
        }

        fn download(download: BytesPs) -> Self {
            Self {
                upload_speed: BytesPs::mbps(15),
                download_speed: download,
                latency: Millis::new(15),
            }
        }
    }

    impl LocationTrustScores {
        fn maximum() -> Self {
            Self::new(vec![LocationTrust {
                distance_to_asserted: Meters::new(1),
                trust_score: dec!(1.0),
            }])
        }

        fn with_trust_scores(trust_scores: &[Decimal]) -> Self {
            Self::new(
                trust_scores
                    .to_owned()
                    .into_iter()
                    .map(|trust_score| LocationTrust {
                        distance_to_asserted: Meters::new(1),
                        trust_score,
                    })
                    .collect(),
            )
        }
    }
}
