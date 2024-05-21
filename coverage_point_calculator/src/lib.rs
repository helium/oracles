#![allow(unused)]

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
    fn coverage_points(&self, signal_level: &SignalLevel) -> Points {
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
    fn location_trust_scores(&self) -> Vec<Multiplier>;
    fn verified_radio_threshold(&self) -> bool;
}

#[derive(Debug, PartialEq)]
struct LocalRadio {
    radio_type: RadioType,
    speedtest_multiplier: Multiplier,
    location_trust_scores: Vec<MaxOneMultplier>,
    verified_radio_threshold: bool,
    hexes: Vec<LocalHex>,
}

#[derive(Debug, PartialEq)]
struct LocalHex {
    rank: usize,
    signal_level: SignalLevel,
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
            let base_coverage_points = self.radio_type.coverage_points(&hex.signal_level);
            let oracle_multiplier = dec!(1);
            let Some(rank) = self.radio_type.rank_multiplier(hex) else {
                // Rank falls outside what is allowed, hex is skipped
                continue;
            };
            let hex_boost_multiplier = self.hex_boosting_multiplier(&hex);

            // https://www.notion.so/nova-labs/POC-reward-formula-7d1f62b638b5447fbfe37a11c0a3d3c8
            let coverage_points = base_coverage_points
                * oracle_multiplier
                * rank
                * hex_boost_multiplier
                * location_trust_score_multiplier;
            points.push(coverage_points)
        }

        points.iter().sum::<Decimal>().round_dp(2)
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

    #[test]
    fn outdoor_radios_consider_top_3_ranked_hexes() {
        let outdoor_wifi = LocalRadio {
            radio_type: RadioType::OutdoorWifi,
            speedtest_multiplier: Multiplier::new(1).unwrap(),
            location_trust_scores: vec![MaxOneMultplier::from_f32_retain(1.0).unwrap()],
            verified_radio_threshold: true,
            hexes: vec![
                LocalHex {
                    rank: 1,
                    signal_level: SignalLevel::High,
                    boosted: None,
                },
                LocalHex {
                    rank: 2,
                    signal_level: SignalLevel::High,
                    boosted: None,
                },
                LocalHex {
                    rank: 3,
                    signal_level: SignalLevel::High,
                    boosted: None,
                },
                LocalHex {
                    rank: 42,
                    signal_level: SignalLevel::High,
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
            speedtest_multiplier: Multiplier::new(1).unwrap(),
            location_trust_scores: vec![MaxOneMultplier::from_f32_retain(1.0).unwrap()],
            verified_radio_threshold: true,
            hexes: vec![
                LocalHex {
                    rank: 1,
                    signal_level: SignalLevel::High,
                    boosted: None,
                },
                LocalHex {
                    rank: 2,
                    signal_level: SignalLevel::High,
                    boosted: None,
                },
                LocalHex {
                    rank: 42,
                    signal_level: SignalLevel::High,
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
            speedtest_multiplier: Multiplier::new(1).unwrap(),
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
            speedtest_multiplier: Multiplier::new(1).unwrap(),
            location_trust_scores: vec![MaxOneMultplier::from_f32_retain(1.0).unwrap()],
            verified_radio_threshold: true,
            hexes: vec![
                LocalHex {
                    rank: 1,
                    signal_level: SignalLevel::High,
                    boosted: None,
                },
                LocalHex {
                    rank: 1,
                    signal_level: SignalLevel::Low,
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
            speedtest_multiplier: Multiplier::new(1).unwrap(),
            location_trust_scores: vec![MaxOneMultplier::from_f32_retain(1.0).unwrap()],
            verified_radio_threshold: true,
            hexes: vec![
                LocalHex {
                    rank: 1,
                    signal_level: SignalLevel::High,
                    boosted: None,
                },
                LocalHex {
                    rank: 1,
                    signal_level: SignalLevel::Medium,
                    boosted: None,
                },
                LocalHex {
                    rank: 1,
                    signal_level: SignalLevel::Low,
                    boosted: None,
                },
                LocalHex {
                    rank: 1,
                    signal_level: SignalLevel::None,
                    boosted: None,
                },
            ],
        };

        let indoor_cbrs = LocalRadio {
            radio_type: RadioType::IndoorCbrs,
            speedtest_multiplier: Multiplier::new(1).unwrap(),
            location_trust_scores: vec![MaxOneMultplier::from_f32_retain(1.0).unwrap()],
            verified_radio_threshold: true,
            hexes: vec![
                LocalHex {
                    rank: 1,
                    signal_level: SignalLevel::High,
                    boosted: None,
                },
                LocalHex {
                    rank: 1,
                    signal_level: SignalLevel::Low,
                    boosted: None,
                },
            ],
        };

        let outdoor_wifi = LocalRadio {
            radio_type: RadioType::OutdoorWifi,
            speedtest_multiplier: Multiplier::new(1).unwrap(),
            location_trust_scores: vec![MaxOneMultplier::from_f32_retain(1.0).unwrap()],
            verified_radio_threshold: true,
            hexes: vec![
                LocalHex {
                    rank: 1,
                    signal_level: SignalLevel::High,
                    boosted: None,
                },
                LocalHex {
                    rank: 1,
                    signal_level: SignalLevel::Medium,
                    boosted: None,
                },
                LocalHex {
                    rank: 1,
                    signal_level: SignalLevel::Low,
                    boosted: None,
                },
                LocalHex {
                    rank: 1,
                    signal_level: SignalLevel::None,
                    boosted: None,
                },
            ],
        };

        let indoor_wifi = LocalRadio {
            radio_type: RadioType::IndoorWifi,
            speedtest_multiplier: Multiplier::new(1).unwrap(),
            location_trust_scores: vec![MaxOneMultplier::from_f32_retain(1.0).unwrap()],
            verified_radio_threshold: true,
            hexes: vec![
                LocalHex {
                    rank: 1,
                    signal_level: SignalLevel::High,
                    boosted: None,
                },
                LocalHex {
                    rank: 1,
                    signal_level: SignalLevel::Low,
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
