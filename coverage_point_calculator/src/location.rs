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
    pub fn new(radio_type: &RadioType, trust_scores: Vec<LocationTrust>) -> Self {
        // CBRS radios are always trusted because they have internal GPS
        let multiplier = if radio_type.is_cbrs() {
            dec!(1)
        } else {
            multiplier(&trust_scores)
        };

        Self {
            multiplier,
            trust_scores,
        }
    }

    pub fn new_with_boosted_hexes(
        radio_type: &RadioType,
        trust_scores: Vec<LocationTrust>,
    ) -> Self {
        let trust_scores: Vec<_> = trust_scores
            .into_iter()
            .map(LocationTrust::into_boosted)
            .collect();

        Self::new(radio_type, trust_scores)
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
    use super::*;

    #[test]
    fn boosted_hexes_within_distance_retain_trust_score() {
        let lts = LocationTrustScores::new_with_boosted_hexes(
            &RadioType::IndoorWifi,
            vec![LocationTrust {
                distance_to_asserted: Meters(49),
                trust_score: dec!(1),
            }],
        );

        assert_eq!(
            LocationTrustScores {
                multiplier: dec!(1),
                trust_scores: vec![LocationTrust {
                    distance_to_asserted: Meters(49),
                    trust_score: dec!(1)
                }]
            },
            lts
        );
    }

    #[test]
    fn boosted_hexes_past_distance_reduce_trust_score() {
        let lts = LocationTrustScores::new_with_boosted_hexes(
            &RadioType::IndoorWifi,
            vec![LocationTrust {
                distance_to_asserted: Meters(51),
                trust_score: dec!(1),
            }],
        );

        assert_eq!(
            LocationTrustScores {
                multiplier: dec!(0.25),
                trust_scores: vec![LocationTrust {
                    distance_to_asserted: Meters(51),
                    trust_score: dec!(0.25)
                }]
            },
            lts
        );
    }

    #[test]
    fn multiplier_is_average_of_scores() {
        // All locations within max distance
        let boosted_trust_scores = LocationTrustScores::new_with_boosted_hexes(
            &RadioType::IndoorWifi,
            vec![
                LocationTrust {
                    distance_to_asserted: Meters(49),
                    trust_score: dec!(0.5),
                },
                LocationTrust {
                    distance_to_asserted: Meters(49),
                    trust_score: dec!(0.5),
                },
            ],
        );
        assert_eq!(dec!(0.5), boosted_trust_scores.multiplier);

        // 1 location within max distance, 1 location outside
        let boosted_over_limit_trust_scores = LocationTrustScores::new_with_boosted_hexes(
            &RadioType::IndoorWifi,
            vec![
                LocationTrust {
                    distance_to_asserted: Meters(49),
                    trust_score: dec!(0.5),
                },
                LocationTrust {
                    distance_to_asserted: Meters(51),
                    trust_score: dec!(0.5),
                },
            ],
        );
        let mult = (dec!(0.5) + dec!(0.25)) / dec!(2);
        assert_eq!(mult, boosted_over_limit_trust_scores.multiplier);

        // All locations outside boosted distance restriction, but no boosted hexes
        let unboosted_trust_scores = LocationTrustScores::new(
            &RadioType::IndoorWifi,
            vec![
                LocationTrust {
                    distance_to_asserted: Meters(100),
                    trust_score: dec!(0.5),
                },
                LocationTrust {
                    distance_to_asserted: Meters(100),
                    trust_score: dec!(0.5),
                },
            ],
        );
        assert_eq!(dec!(0.5), unboosted_trust_scores.multiplier);
    }
}
