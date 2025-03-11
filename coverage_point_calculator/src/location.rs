use rust_decimal::Decimal;
use rust_decimal_macros::dec;

use crate::RadioType;

type Meters = u32;

#[derive(Debug, Clone, PartialEq)]
pub struct LocationTrust {
    pub meters_to_asserted: Meters,
    pub trust_score: Decimal,
}

/// Returns the trust multiplier for a given radio type and distance to it's asserted location.
///
/// [HIP-119: Gaming Loopholes][gaming-loopholes]
///
/// [gaming-loopholes]: https://github.com/helium/HIP/blob/main/0119-closing-gaming-loopholes-within-the-mobile-network.md#maximum-asserted-distance-difference
pub fn asserted_distance_to_trust_multiplier(
    radio_type: RadioType,
    meters_to_asserted: Meters,
) -> Decimal {
    match radio_type {
        RadioType::IndoorWifi => match meters_to_asserted {
            0..=200 => dec!(1.00),
            201..=300 => dec!(0.25),
            _ => dec!(0.00),
        },
        RadioType::OutdoorWifi => match meters_to_asserted {
            0..=75 => dec!(1.00),
            76..=100 => dec!(0.25),
            _ => dec!(0.00),
        },
        _ => dec!(0),
    }
}

pub(crate) fn average_distance(trust_scores: &[LocationTrust]) -> Decimal {
    // FIXME-K: if count = 0, division by zero happens
    let count = Decimal::from(trust_scores.len());
    let sum: Decimal = trust_scores
        .iter()
        .map(|l| Decimal::from(l.meters_to_asserted))
        .sum();

    sum / count
}

pub fn multiplier(trust_scores: &[LocationTrust]) -> Decimal {
    // FIXME-K: if count = 0, division by zero happens
    let count = Decimal::from(trust_scores.len());
    let scores: Decimal = trust_scores.iter().map(|l| l.trust_score).sum();

    scores / count
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn distance_does_not_effect_multiplier() {
        let trust_scores = vec![
            LocationTrust {
                meters_to_asserted: 0,
                trust_score: dec!(0.5),
            },
            LocationTrust {
                meters_to_asserted: 49,
                trust_score: dec!(0.5),
            },
            LocationTrust {
                meters_to_asserted: 50,
                trust_score: dec!(0.5),
            },
            LocationTrust {
                meters_to_asserted: 51,
                trust_score: dec!(0.5),
            },
            LocationTrust {
                meters_to_asserted: 100,
                trust_score: dec!(0.5),
            },
            LocationTrust {
                meters_to_asserted: 99999,
                trust_score: dec!(0.5),
            },
        ];

        assert_eq!(dec!(0.5), multiplier(&trust_scores));
    }
}
