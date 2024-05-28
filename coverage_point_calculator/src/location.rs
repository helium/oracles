use rust_decimal::Decimal;
use rust_decimal_macros::dec;

#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub struct Meters(u32);

impl Meters {
    pub fn new(meters: u32) -> Self {
        Self(meters)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct LocationTrustScores {
    pub any_hex_boosted_multiplier: Decimal,
    pub no_boosted_hex_multiplier: Decimal,
    trust_scores: Vec<LocationTrust>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct LocationTrust {
    pub distance_to_asserted: Meters,
    pub trust_score: Decimal,
}

impl LocationTrustScores {
    pub fn new(trust_scores: Vec<LocationTrust>) -> Self {
        let boosted_multiplier = boosted_multiplier(&trust_scores);
        let unboosted_multiplier = unboosted_multiplier(&trust_scores);
        Self {
            any_hex_boosted_multiplier: boosted_multiplier,
            no_boosted_hex_multiplier: unboosted_multiplier,
            trust_scores,
        }
    }
}

fn boosted_multiplier(trust_scores: &[LocationTrust]) -> Decimal {
    const RESTRICTIVE_MAX_DISTANCE: Meters = Meters(50);
    // Cap multipliers to 0.25x when a radio covers _any_ boosted hex
    // and it's distance to asserted is above the threshold.
    let count = Decimal::from(trust_scores.len());
    let scores: Decimal = trust_scores
        .iter()
        .map(|l| {
            if l.distance_to_asserted > RESTRICTIVE_MAX_DISTANCE {
                dec!(0.25).min(l.trust_score)
            } else {
                l.trust_score
            }
        })
        .sum();

    scores / count
}

fn unboosted_multiplier(trust_scores: &[LocationTrust]) -> Decimal {
    let count = Decimal::from(trust_scores.len());
    let scores: Decimal = trust_scores.iter().map(|l| l.trust_score).sum();

    scores / count
}
