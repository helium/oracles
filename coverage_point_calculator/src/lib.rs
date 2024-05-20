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
            RadioType::IndoorCbrs => todo!(),
            RadioType::OutdoorCbrs => todo!(),
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
    fn outdoor_wifi_radio_coverage_points() {
        let local_radio = LocalRadio {
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
        assert_eq!(28, local_radio.coverage_points());
    }

    #[test]
    fn indoor_wifi_radio_coverage_points() {
        let local_radio = LocalRadio {
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
        assert_eq!(500, local_radio.coverage_points());
    }
}
