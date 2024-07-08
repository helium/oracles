use coverage_map::RankedCoverage;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

use crate::RadioType;

/// When a Radio is covering any boosted hexes, it's trust score location must
/// be within this distance to it's asserted location. Otherwise the trust_score
/// will be capped at 0.25x.
const RESTRICTIVE_MAX_DISTANCE: Meters = 50;

type Meters = u32;

#[derive(Debug, Clone, PartialEq)]
pub struct LocationTrust {
    pub meters_to_asserted: Meters,
    pub trust_score: Decimal,
}

/// Returns the trust multiplier for a given radio type and distance to asserted.
///
/// Reference:
/// HIP-119
/// https://github.com/helium/HIP/blob/main/0119-closing-gaming-loopholes-within-the-mobile-network.md
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

pub(crate) fn clean_trust_scores(
    trust_scores: Vec<LocationTrust>,
    ranked_coverage: &[RankedCoverage],
) -> Vec<LocationTrust> {
    let any_boosted_hexes = ranked_coverage.iter().any(|hex| hex.boosted.is_some());

    if any_boosted_hexes {
        trust_scores
            .into_iter()
            .map(LocationTrust::into_boosted)
            .collect()
    } else {
        trust_scores
    }
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

impl LocationTrust {
    fn into_boosted(self) -> Self {
        // Cap multipliers to 0.25x when a radio covers _any_ boosted hex
        // and it's distance to asserted is above the threshold.
        let trust_score = if self.meters_to_asserted > RESTRICTIVE_MAX_DISTANCE {
            dec!(0.25).min(self.trust_score)
        } else {
            self.trust_score
        };

        LocationTrust {
            trust_score,
            meters_to_asserted: self.meters_to_asserted,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;

    use coverage_map::SignalLevel;
    use hex_assignments::{assignment::HexAssignments, Assignment};

    use super::*;

    #[test]
    fn all_locations_within_max_boosted_distance() {
        let trust_scores = vec![
            LocationTrust {
                meters_to_asserted: 49,
                trust_score: dec!(0.5),
            },
            LocationTrust {
                meters_to_asserted: 50,
                trust_score: dec!(0.5),
            },
        ];
        let boosted = clean_trust_scores(trust_scores.clone(), &boosted_ranked_coverage());
        let unboosted = clean_trust_scores(trust_scores, &[]);

        assert_eq!(dec!(0.5), multiplier(RadioType::IndoorWifi, &boosted));
        assert_eq!(dec!(0.5), multiplier(RadioType::IndoorWifi, &unboosted));
    }

    #[test]
    fn all_locations_past_max_boosted_distance() {
        let trust_scores = vec![
            LocationTrust {
                meters_to_asserted: 51,
                trust_score: dec!(0.5),
            },
            LocationTrust {
                meters_to_asserted: 100,
                trust_score: dec!(0.5),
            },
        ];

        let boosted = clean_trust_scores(trust_scores.clone(), &boosted_ranked_coverage());
        let unboosted = clean_trust_scores(trust_scores, &[]);

        assert_eq!(dec!(0.25), multiplier(RadioType::IndoorWifi, &boosted));
        assert_eq!(dec!(0.5), multiplier(RadioType::IndoorWifi, &unboosted));
    }

    #[test]
    fn locations_around_max_boosted_distance() {
        let trust_scores = vec![
            LocationTrust {
                meters_to_asserted: 50,
                trust_score: dec!(0.5),
            },
            LocationTrust {
                meters_to_asserted: 51,
                trust_score: dec!(0.5),
            },
        ];

        let boosted = clean_trust_scores(trust_scores.clone(), &boosted_ranked_coverage());
        let unboosted = clean_trust_scores(trust_scores, &[]);

        // location past distance limit trust score is degraded
        let degraded_mult = (dec!(0.5) + dec!(0.25)) / dec!(2);
        assert_eq!(degraded_mult, multiplier(RadioType::IndoorWifi, &boosted));
        // location past distance limit trust score is untouched
        assert_eq!(dec!(0.5), multiplier(RadioType::IndoorWifi, &unboosted));
    }

    #[test]
    fn cbrs_trust_score_bypassed_for_gps_trust() {
        // CBRS radios have GPS units in them, they are always trusted,
        // regardless of their score or distance provided.

        let trust_scores = vec![LocationTrust {
            meters_to_asserted: 99999,
            trust_score: dec!(0),
        }];

        let boosted = clean_trust_scores(trust_scores.clone(), &boosted_ranked_coverage());
        let unboosted = clean_trust_scores(trust_scores, &[]);

        assert_eq!(dec!(1), multiplier(RadioType::IndoorCbrs, &boosted));
        assert_eq!(dec!(1), multiplier(RadioType::IndoorCbrs, &unboosted));
    }

    fn boosted_ranked_coverage() -> Vec<RankedCoverage> {
        vec![RankedCoverage {
            hex: hextree::Cell::from_raw(0x8c2681a3064edff).unwrap(),
            rank: 1,
            hotspot_key: vec![],
            cbsd_id: None,
            signal_level: SignalLevel::High,
            assignments: HexAssignments {
                footfall: Assignment::A,
                landtype: Assignment::A,
                urbanized: Assignment::A,
            },
            boosted: NonZeroU32::new(5),
        }]
    }
}
