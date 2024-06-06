use coverage_map::RankedCoverage;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

use crate::RadioType;

const RESTRICTIVE_MAX_DISTANCE: Meters = Meters(50);

#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub struct Meters(u32);

impl Meters {
    pub fn new(meters: u32) -> Self {
        Self(meters)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct LocationTrustScores {
    pub multiplier: Decimal,
    pub trust_scores: Vec<LocationTrust>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct LocationTrust {
    pub distance_to_asserted: Meters,
    pub trust_score: Decimal,
}

impl LocationTrustScores {
    pub fn new(
        radio_type: RadioType,
        trust_scores: Vec<LocationTrust>,
        ranked_coverage: &[RankedCoverage],
    ) -> Self {
        let any_boosted_hexes = ranked_coverage.iter().any(|hex| hex.boosted.is_some());

        let cleaned_scores = if any_boosted_hexes {
            trust_scores
                .into_iter()
                .map(LocationTrust::into_boosted)
                .collect()
        } else {
            trust_scores
        };

        // CBRS radios are always trusted because they have internal GPS
        let multiplier = if radio_type.is_cbrs() {
            dec!(1)
        } else {
            multiplier(&cleaned_scores)
        };

        Self {
            multiplier,
            trust_scores: cleaned_scores,
        }
    }
}
impl LocationTrust {
    fn into_boosted(self) -> Self {
        // Cap multipliers to 0.25x when a radio covers _any_ boosted hex
        // and it's distance to asserted is above the threshold.
        let trust_score = if self.distance_to_asserted > RESTRICTIVE_MAX_DISTANCE {
            dec!(0.25).min(self.trust_score)
        } else {
            self.trust_score
        };

        LocationTrust {
            trust_score,
            distance_to_asserted: self.distance_to_asserted,
        }
    }
}

fn multiplier(trust_scores: &[LocationTrust]) -> Decimal {
    let count = Decimal::from(trust_scores.len());
    let scores: Decimal = trust_scores.iter().map(|l| l.trust_score).sum();

    scores / count
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
                distance_to_asserted: Meters(49),
                trust_score: dec!(0.5),
            },
            LocationTrust {
                distance_to_asserted: Meters(50),
                trust_score: dec!(0.5),
            },
        ];
        let boosted = LocationTrustScores::new(
            RadioType::IndoorWifi,
            trust_scores.clone(),
            &boosted_ranked_coverage(),
        );
        let unboosted = LocationTrustScores::new(RadioType::IndoorWifi, trust_scores, &[]);

        assert_eq!(dec!(0.5), boosted.multiplier);
        assert_eq!(dec!(0.5), unboosted.multiplier);
    }

    #[test]
    fn all_locations_past_max_boosted_distance() {
        let trust_scores = vec![
            LocationTrust {
                distance_to_asserted: Meters(51),
                trust_score: dec!(0.5),
            },
            LocationTrust {
                distance_to_asserted: Meters(100),
                trust_score: dec!(0.5),
            },
        ];

        let boosted = LocationTrustScores::new(
            RadioType::IndoorWifi,
            trust_scores.clone(),
            &boosted_ranked_coverage(),
        );
        let unboosted = LocationTrustScores::new(RadioType::IndoorWifi, trust_scores, &[]);

        assert_eq!(dec!(0.25), boosted.multiplier);
        assert_eq!(dec!(0.5), unboosted.multiplier);
    }

    #[test]
    fn locations_around_max_boosted_distance() {
        let trust_scores = vec![
            LocationTrust {
                distance_to_asserted: Meters(50),
                trust_score: dec!(0.5),
            },
            LocationTrust {
                distance_to_asserted: Meters(51),
                trust_score: dec!(0.5),
            },
        ];

        let boosted = LocationTrustScores::new(
            RadioType::IndoorWifi,
            trust_scores.clone(),
            &boosted_ranked_coverage(),
        );
        let unboosted = LocationTrustScores::new(RadioType::IndoorWifi, trust_scores, &[]);

        // location past distance limit trust score is degraded
        let degraded_mult = (dec!(0.5) + dec!(0.25)) / dec!(2);
        assert_eq!(degraded_mult, boosted.multiplier);
        // location past distance limit trust score is untouched
        assert_eq!(dec!(0.5), unboosted.multiplier);
    }

    #[test]
    fn cbrs_trust_score_bypassed_for_gps_trust() {
        // CBRS radios have GPS units in them, they are always trusted,
        // regardless of their score or distance provided.

        let trust_scores = vec![LocationTrust {
            distance_to_asserted: Meters(99999),
            trust_score: dec!(0),
        }];

        let boosted = LocationTrustScores::new(
            RadioType::IndoorCbrs,
            trust_scores.clone(),
            &boosted_ranked_coverage(),
        );
        let unboosted = LocationTrustScores::new(RadioType::IndoorCbrs, trust_scores, &[]);

        assert_eq!(dec!(1), boosted.multiplier);
        assert_eq!(dec!(1), unboosted.multiplier);
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
