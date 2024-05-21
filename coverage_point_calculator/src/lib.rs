#![allow(unused)]

use std::num::NonZeroU16;

use hextree::Cell;
use rust_decimal::Decimal;

type Multiplier = NonZeroU16;
type Points = u32;

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
                SignalLevel::High => 400,
                SignalLevel::Low => 100,
                other => panic!("indoor wifi radios cannot have {other:?} signal levels"),
            },
            RadioType::OutdoorWifi => match signal_level {
                SignalLevel::High => 16,
                SignalLevel::Medium => 8,
                SignalLevel::Low => 4,
                SignalLevel::None => 0,
            },
            RadioType::IndoorCbrs => match signal_level {
                SignalLevel::High => 100,
                SignalLevel::Low => 25,
                other => panic!("indoor cbrs radios cannot have {other:?} signal levels"),
            },
            RadioType::OutdoorCbrs => match signal_level {
                SignalLevel::High => 4,
                SignalLevel::Medium => 2,
                SignalLevel::Low => 1,
                SignalLevel::None => 0,
            },
        }
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
    location_trust_scores: Vec<Multiplier>,
    verified_radio_threshold: bool,
    hexes: Vec<LocalHex>,
}

#[derive(Debug, PartialEq)]
struct LocalHex {
    rank: u16,
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
        let mut points = 0;
        for hex in self.hexes.iter() {
            points += self.radio_type.coverage_points(&hex.signal_level);
        }
        points
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn base_radio_coverage_points() {
        let outdoor_cbrs = LocalRadio {
            radio_type: RadioType::OutdoorCbrs,
            speedtest_multiplier: NonZeroU16::new(1).unwrap(),
            location_trust_scores: vec![NonZeroU16::new(1).unwrap()],
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
            speedtest_multiplier: NonZeroU16::new(1).unwrap(),
            location_trust_scores: vec![NonZeroU16::new(1).unwrap()],
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
            speedtest_multiplier: NonZeroU16::new(1).unwrap(),
            location_trust_scores: vec![NonZeroU16::new(1).unwrap()],
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
            speedtest_multiplier: NonZeroU16::new(1).unwrap(),
            location_trust_scores: vec![NonZeroU16::new(1).unwrap()],
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
        assert_eq!(7, outdoor_cbrs.coverage_points());
        assert_eq!(125, indoor_cbrs.coverage_points());
        assert_eq!(28, outdoor_wifi.coverage_points());
        assert_eq!(500, indoor_wifi.coverage_points());
    }
}
