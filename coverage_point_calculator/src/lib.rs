#![allow(unused)]
///
/// The coverage_points calculation in [`LocalRadio.coverage_points()`] are
/// comprised 5 top level fields.
///
/// - estimated_coverage_points
///   - [HIP-74][modeled-coverage]
///   - [HIP-113][cbrs-experimental]
/// - assignment_multiplier
///   - [HIP-103][oracle-boosting]
/// - rank
///   - [HIP-105][hex-limits]
/// - hex_boost_multiplier  
///   - [HIP-84][provider-boosting]
/// - location_trust_score_multiplier
///   - [HIP-98][qos-score]
/// - speedtest_multiplier
///   - [HIP-74][modeled-coverage]
///
/// [modeled-coverage]: https://github.com/helium/HIP/blob/main/0074-mobile-poc-modeled-coverage-rewards.md#outdoor-radios
/// [cbrs-experimental]: https://github.com/helium/HIP/blob/main/0113-reward-cbrs-as-experimental.md
/// [oracle-boosting]: https://github.com/helium/HIP/blob/main/0103-oracle-hex-boosting.md
/// [hex-limits]: https://github.com/helium/HIP/blob/main/0105-modification-of-mobile-subdao-hex-limits.md
/// [provider-boosting]: https://github.com/helium/HIP/blob/main/0084-service-provider-hex-boosting.md#mechanics-and-price-of-boosting-hexes
/// [qos-score]: https://github.com/helium/HIP/blob/main/0098-mobile-subdao-quality-of-service-requirements.md
///
use hextree::Cell;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

type Multiplier = std::num::NonZeroU32;
type MaxOneMultplier = Decimal;
type Points = Decimal;

#[derive(Debug, Clone, PartialEq)]
enum RadioType {
    IndoorWifi,
    OutdoorWifi,
    IndoorCbrs,
    OutdoorCbrs,
}

impl RadioType {
    fn estimated_coverage_points(&self, signal_level: &SignalLevel) -> Points {
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

    fn rank_multiplier(&self, hex: &LocalHex) -> Option<MaxOneMultplier> {
        let multipliers = match self {
            RadioType::IndoorWifi => vec![dec!(1)],
            RadioType::IndoorCbrs => vec![dec!(1)],
            RadioType::OutdoorWifi => vec![dec!(1), dec!(0.5), dec!(0.25)],
            RadioType::OutdoorCbrs => vec![dec!(1), dec!(0.5), dec!(0.25)],
        };

        // TODO: decide if rank should be 0-indexed
        multipliers.get(hex.rank - 1).cloned()
    }
}

#[derive(Debug, Clone, PartialEq)]
enum SignalLevel {
    High,
    Medium,
    Low,
    None,
}

#[derive(Debug, Clone, PartialEq)]
struct Assignments {
    footfall: Assignment,
    landtype: Assignment,
    urbanized: Assignment,
}

#[derive(Debug, Clone, PartialEq)]
enum Assignment {
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

#[derive(Debug, Clone, PartialEq)]
enum Speedtest {
    Good,
    Acceptable,
    Degraded,
    Poor,
    Fail,
}

impl Speedtest {
    fn multiplier(&self) -> MaxOneMultplier {
        match self {
            Speedtest::Good => dec!(1.00),
            Speedtest::Acceptable => dec!(0.75),
            Speedtest::Degraded => dec!(0.50),
            Speedtest::Poor => dec!(0.25),
            Speedtest::Fail => dec!(0),
        }
    }
}

trait Coverage {
    fn radio_type(&self) -> RadioType;
    fn signal_level(&self) -> SignalLevel;
}

trait CoverageMap<C: Coverage> {
    fn get(&self, cell: Cell) -> Vec<C>;
}

trait RewardableRadio {
    fn hex(&self) -> Cell;
    fn radio_type(&self) -> RadioType;
    fn location_trust_scores(&self) -> Vec<MaxOneMultplier>;
    fn verified_radio_threshold(&self) -> bool;
}

#[derive(Debug, PartialEq)]
struct LocalRadio {
    radio_type: RadioType,
    speedtest: Speedtest,
    location_trust_scores: Vec<MaxOneMultplier>,
    verified_radio_threshold: bool,
    hexes: Vec<LocalHex>,
}

#[derive(Debug, PartialEq)]
struct LocalHex {
    rank: usize,
    signal_level: SignalLevel,
    assignment: Assignments,
    boosted: Option<Multiplier>,
}

fn calculate<C: Coverage>(
    radio: impl RewardableRadio,
    coverage_map: impl CoverageMap<C>,
) -> LocalRadio {
    todo!()
}

impl LocalRadio {
    pub fn coverage_points(&self) -> Points {
        let mut points = vec![];
        let location_trust_score_multiplier = self.location_trust_multiplier();

        for hex in self.hexes.iter() {
            let Some(rank) = self.radio_type.rank_multiplier(hex) else {
                // Rank falls outside what is allowed, skip as early as possible
                continue;
            };

            let estimated_coverage_points =
                self.radio_type.estimated_coverage_points(&hex.signal_level);
            let assignments_multiplier = hex.assignment.multiplier();
            let hex_boost_multiplier = self.hex_boosting_multiplier(&hex);

            let coverage_points = estimated_coverage_points
                * assignments_multiplier
                * rank
                * hex_boost_multiplier
                * location_trust_score_multiplier;

            points.push(coverage_points)
        }

        let mut coverage_points = points.iter().sum::<Decimal>();
        coverage_points *= self.speedtest.multiplier();
        coverage_points.round_dp(2)
    }

    fn location_trust_multiplier(&self) -> Decimal {
        let trust_score_count = Decimal::from(self.location_trust_scores.len());
        let trust_score_sum = self.location_trust_scores.iter().sum::<Decimal>();
        trust_score_sum / trust_score_count
    }

    fn hex_boosting_multiplier(&self, hex: &LocalHex) -> MaxOneMultplier {
        let maybe_boost = if self.verified_radio_threshold {
            hex.boosted.map_or(1, |boost| boost.get())
        } else {
            1
        };
        Decimal::from(maybe_boost)
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use rust_decimal_macros::dec;

    impl Assignments {
        fn best() -> Self {
            Self {
                footfall: Assignment::A,
                landtype: Assignment::A,
                urbanized: Assignment::A,
            }
        }
    }

    #[test]
    fn speedtest() {
        let mut indoor_cbrs = LocalRadio {
            radio_type: RadioType::IndoorCbrs,
            speedtest: Speedtest::Good,
            location_trust_scores: vec![MaxOneMultplier::from_f32_retain(1.0).unwrap()],
            verified_radio_threshold: true,
            hexes: vec![LocalHex {
                rank: 1,
                signal_level: SignalLevel::High,
                assignment: Assignments::best(),
                boosted: None,
            }],
        };
        assert_eq!(dec!(100), indoor_cbrs.coverage_points());

        indoor_cbrs.speedtest = Speedtest::Acceptable;
        assert_eq!(dec!(75), indoor_cbrs.coverage_points());

        indoor_cbrs.speedtest = Speedtest::Degraded;
        assert_eq!(dec!(50), indoor_cbrs.coverage_points());

        indoor_cbrs.speedtest = Speedtest::Poor;
        assert_eq!(dec!(25), indoor_cbrs.coverage_points());

        indoor_cbrs.speedtest = Speedtest::Fail;
        assert_eq!(dec!(0), indoor_cbrs.coverage_points());
    }

    #[test]
    fn oracle_boosting_assignments_apply_per_hex() {
        fn local_hex(
            footfall: Assignment,
            landtype: Assignment,
            urbanized: Assignment,
        ) -> LocalHex {
            LocalHex {
                rank: 1,
                signal_level: SignalLevel::High,
                assignment: Assignments {
                    footfall,
                    landtype,
                    urbanized,
                },
                boosted: None,
            }
        }

        use Assignment::*;
        let indoor_cbrs = LocalRadio {
            radio_type: RadioType::IndoorCbrs,
            speedtest: Speedtest::Good,
            location_trust_scores: vec![MaxOneMultplier::from_f32_retain(1.0).unwrap()],
            verified_radio_threshold: true,
            hexes: vec![
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
        };

        assert_eq!(dec!(1073), indoor_cbrs.coverage_points());
    }

    #[test]
    fn outdoor_radios_consider_top_3_ranked_hexes() {
        let outdoor_wifi = LocalRadio {
            radio_type: RadioType::OutdoorWifi,
            speedtest: Speedtest::Good,
            location_trust_scores: vec![MaxOneMultplier::from_f32_retain(1.0).unwrap()],
            verified_radio_threshold: true,
            hexes: vec![
                LocalHex {
                    rank: 1,
                    signal_level: SignalLevel::High,
                    assignment: Assignments::best(),
                    boosted: None,
                },
                LocalHex {
                    rank: 2,
                    signal_level: SignalLevel::High,
                    assignment: Assignments::best(),
                    boosted: None,
                },
                LocalHex {
                    rank: 3,
                    signal_level: SignalLevel::High,
                    assignment: Assignments::best(),
                    boosted: None,
                },
                LocalHex {
                    rank: 42,
                    signal_level: SignalLevel::High,
                    assignment: Assignments::best(),
                    boosted: None,
                },
            ],
        };

        // rank 1  :: 1.00 * 16 == 16
        // rank 2  :: 0.50 * 16 == 8
        // rank 3  :: 0.25 * 16 == 4
        // rank 42 :: 0.00 * 16 == 0
        assert_eq!(dec!(28), outdoor_wifi.coverage_points());
    }

    #[test]
    fn indoor_radios_only_consider_first_ranked_hexes() {
        let indoor_wifi = LocalRadio {
            radio_type: RadioType::IndoorWifi,
            speedtest: Speedtest::Good,
            location_trust_scores: vec![MaxOneMultplier::from_f32_retain(1.0).unwrap()],
            verified_radio_threshold: true,
            hexes: vec![
                LocalHex {
                    rank: 1,
                    signal_level: SignalLevel::High,
                    assignment: Assignments::best(),
                    boosted: None,
                },
                LocalHex {
                    rank: 2,
                    signal_level: SignalLevel::High,
                    assignment: Assignments::best(),
                    boosted: None,
                },
                LocalHex {
                    rank: 42,
                    signal_level: SignalLevel::High,
                    assignment: Assignments::best(),
                    boosted: None,
                },
            ],
        };

        assert_eq!(dec!(400), indoor_wifi.coverage_points());
    }

    #[test]
    fn location_trust_score_multiplier() {
        // Location scores are averaged together
        let indoor_wifi = LocalRadio {
            radio_type: RadioType::IndoorWifi,
            speedtest: Speedtest::Good,
            location_trust_scores: vec![
                MaxOneMultplier::from_f32_retain(0.1).unwrap(),
                MaxOneMultplier::from_f32_retain(0.2).unwrap(),
                MaxOneMultplier::from_f32_retain(0.3).unwrap(),
                MaxOneMultplier::from_f32_retain(0.4).unwrap(),
            ],
            verified_radio_threshold: true,
            hexes: vec![LocalHex {
                rank: 1,
                signal_level: SignalLevel::High,
                assignment: Assignments::best(),
                boosted: None,
            }],
        };

        // Location trust scores is 1/4
        assert_eq!(dec!(100), indoor_wifi.coverage_points());
    }

    #[test]
    fn boosted_hex() {
        let mut indoor_wifi = LocalRadio {
            radio_type: RadioType::IndoorWifi,
            speedtest: Speedtest::Good,
            location_trust_scores: vec![MaxOneMultplier::from_f32_retain(1.0).unwrap()],
            verified_radio_threshold: true,
            hexes: vec![
                LocalHex {
                    rank: 1,
                    signal_level: SignalLevel::High,
                    assignment: Assignments::best(),
                    boosted: None,
                },
                LocalHex {
                    rank: 1,
                    signal_level: SignalLevel::Low,
                    assignment: Assignments::best(),
                    boosted: Multiplier::new(4),
                },
            ],
        };
        // The hex with a low signal_level is boosted to the same level as a
        // signal_level of High.
        assert_eq!(dec!(800), indoor_wifi.coverage_points());

        // When the radio is not verified for boosted rewards, the boost has no effect.
        indoor_wifi.verified_radio_threshold = false;
        assert_eq!(dec!(500), indoor_wifi.coverage_points());
    }

    #[test]
    fn base_radio_coverage_points() {
        let outdoor_cbrs = LocalRadio {
            radio_type: RadioType::OutdoorCbrs,
            speedtest: Speedtest::Good,
            location_trust_scores: vec![MaxOneMultplier::from_f32_retain(1.0).unwrap()],
            verified_radio_threshold: true,
            hexes: vec![
                LocalHex {
                    rank: 1,
                    signal_level: SignalLevel::High,
                    assignment: Assignments::best(),
                    boosted: None,
                },
                LocalHex {
                    rank: 1,
                    signal_level: SignalLevel::Medium,
                    assignment: Assignments::best(),
                    boosted: None,
                },
                LocalHex {
                    rank: 1,
                    signal_level: SignalLevel::Low,
                    assignment: Assignments::best(),
                    boosted: None,
                },
                LocalHex {
                    rank: 1,
                    signal_level: SignalLevel::None,
                    assignment: Assignments::best(),
                    boosted: None,
                },
            ],
        };

        let indoor_cbrs = LocalRadio {
            radio_type: RadioType::IndoorCbrs,
            speedtest: Speedtest::Good,
            location_trust_scores: vec![MaxOneMultplier::from_f32_retain(1.0).unwrap()],
            verified_radio_threshold: true,
            hexes: vec![
                LocalHex {
                    rank: 1,
                    signal_level: SignalLevel::High,
                    assignment: Assignments::best(),
                    boosted: None,
                },
                LocalHex {
                    rank: 1,
                    signal_level: SignalLevel::Low,
                    assignment: Assignments::best(),
                    boosted: None,
                },
            ],
        };

        let outdoor_wifi = LocalRadio {
            radio_type: RadioType::OutdoorWifi,
            speedtest: Speedtest::Good,
            location_trust_scores: vec![MaxOneMultplier::from_f32_retain(1.0).unwrap()],
            verified_radio_threshold: true,
            hexes: vec![
                LocalHex {
                    rank: 1,
                    signal_level: SignalLevel::High,
                    assignment: Assignments::best(),
                    boosted: None,
                },
                LocalHex {
                    rank: 1,
                    signal_level: SignalLevel::Medium,
                    assignment: Assignments::best(),
                    boosted: None,
                },
                LocalHex {
                    rank: 1,
                    signal_level: SignalLevel::Low,
                    assignment: Assignments::best(),
                    boosted: None,
                },
                LocalHex {
                    rank: 1,
                    signal_level: SignalLevel::None,
                    assignment: Assignments::best(),
                    boosted: None,
                },
            ],
        };

        let indoor_wifi = LocalRadio {
            radio_type: RadioType::IndoorWifi,
            speedtest: Speedtest::Good,
            location_trust_scores: vec![MaxOneMultplier::from_f32_retain(1.0).unwrap()],
            verified_radio_threshold: true,
            hexes: vec![
                LocalHex {
                    rank: 1,
                    signal_level: SignalLevel::High,
                    assignment: Assignments::best(),
                    boosted: None,
                },
                LocalHex {
                    rank: 1,
                    signal_level: SignalLevel::Low,
                    assignment: Assignments::best(),
                    boosted: None,
                },
            ],
        };

        // When each radio contains a hex of every applicable signal_level, and
        // multipliers are break even. These are the accumulated coverage points.
        assert_eq!(dec!(7), outdoor_cbrs.coverage_points());
        assert_eq!(dec!(125), indoor_cbrs.coverage_points());
        assert_eq!(dec!(28), outdoor_wifi.coverage_points());
        assert_eq!(dec!(500), indoor_wifi.coverage_points());
    }
}
