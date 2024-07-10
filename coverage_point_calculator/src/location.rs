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
        RadioType::IndoorWifi | RadioType::IndoorCbrs => match meters_to_asserted {
            0..=200 => dec!(1.00),
            201..=300 => dec!(0.25),
            _ => dec!(0.00),
        },
        RadioType::OutdoorWifi | RadioType::OutdoorCbrs => match meters_to_asserted {
            0..=75 => dec!(1.00),
            76..=100 => dec!(0.25),
            _ => dec!(0.00),
        },
    }
}

pub(crate) fn average_distance(radio_type: RadioType, trust_scores: &[LocationTrust]) -> Decimal {
    // CBRS radios are always trusted because they have internal GPS
    if radio_type.is_cbrs() {
        return dec!(0);
    }

    let count = Decimal::from(trust_scores.len());
    let sum: Decimal = trust_scores
        .iter()
        .map(|l| Decimal::from(l.meters_to_asserted))
        .sum();

    sum / count
}

pub(crate) fn multiplier(radio_type: RadioType, trust_scores: &[LocationTrust]) -> Decimal {
    // CBRS radios are always trusted because they have internal GPS
    if radio_type.is_cbrs() {
        return dec!(1);
    }

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

        assert_eq!(dec!(0.5), multiplier(RadioType::IndoorWifi, &trust_scores));
    }

    #[test]
    fn cbrs_trust_score_bypassed_for_gps_trust() {
        // CBRS radios have GPS units in them, they are always trusted,
        // regardless of their score or distance provided.

        let trust_scores = vec![LocationTrust {
            meters_to_asserted: 99999,
            trust_score: dec!(0),
        }];

        assert_eq!(dec!(1), multiplier(RadioType::IndoorCbrs, &trust_scores));
    }
}
