use coverage_map::RankedCoverage;
use hex_assignments::assignment::HexAssignments;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

use crate::{BoostedHexStatus, RadioType, Result};

/// Breakdown of points for a hex.
///
/// Example:
///   Outdoor Wifi with 1-hex boosted 5x,
///   Rank 2, Assignment AAA:
///     SplitPoints {
///       modeled: 16,
///       base: 8,
///       boosted: 32
///     }
///
///   Rank 2 splits modeled points in half.
///   Boost at 5x adds 32 points ( (8 * 5) - 8 )
#[derive(Debug, Default, Clone)]
pub struct HexPoints {
    /// Default points received for hex
    ///
    /// (RadioType, SignalLevel) points
    modeled: Decimal,
    /// Points including Coverage affected multipliers
    ///
    /// modeled + (Rank * Assignment)
    base: Decimal,
    /// Points _over_ normal received from hex boosting.
    ///
    /// (base * Boost multiplier) - base
    boosted: Decimal,
}

impl HexPoints {
    pub(crate) fn total_coverage_points(&self) -> Decimal {
        self.base + self.boosted
    }
}

#[derive(Debug, Clone)]
pub struct CoveredHex {
    pub hex: hextree::Cell,
    /// Breakdown of points for a hex
    pub points: HexPoints,
    /// Oracle boosted Assignments
    pub assignments: HexAssignments,
    pub assignment_multiplier: Decimal,
    /// [RankedCoverage::rank] 1-based
    pub rank: usize,
    pub rank_multiplier: Decimal,
    /// Provider boosted multiplier. Will be None if the Radio does not qualify
    /// for boosted rewards.
    pub boosted_multiplier: Option<Decimal>,
}

pub(crate) fn clean_covered_hexes(
    radio_type: RadioType,
    boosted_hex_status: BoostedHexStatus,
    ranked_coverage: Vec<RankedCoverage>,
) -> Result<Vec<CoveredHex>> {
    // verify all hexes can obtain a base coverage point
    let covered_hexes = ranked_coverage
        .into_iter()
        .map(|ranked| {
            let modeled_coverage_points = radio_type.base_coverage_points(&ranked.signal_level)?;
            let rank_multiplier = radio_type.rank_multiplier(ranked.rank);

            let boosted_multiplier = if boosted_hex_status.is_eligible() {
                ranked.boosted.map(|boost| boost.get()).map(Decimal::from)
            } else {
                None
            };

            // hip-103: if a hex is boosted by a service provider >=1x, the oracle
            // multiplier will automatically be 1x, regardless of boosted_hex_status.
            let assignment_multiplier = if ranked.boosted.is_some() {
                dec!(1)
            } else {
                ranked.assignments.boosting_multiplier()
            };

            let base_coverage_points =
                modeled_coverage_points * assignment_multiplier * rank_multiplier;

            let calculated_coverage_points = modeled_coverage_points
                * assignment_multiplier
                * rank_multiplier
                * boosted_multiplier.unwrap_or(dec!(1));

            let boosted_coverage_points = calculated_coverage_points - base_coverage_points;

            Ok(CoveredHex {
                hex: ranked.hex,
                points: HexPoints {
                    modeled: modeled_coverage_points,
                    base: base_coverage_points,
                    boosted: boosted_coverage_points,
                },
                assignments: ranked.assignments,
                assignment_multiplier,
                rank: ranked.rank,
                rank_multiplier,
                boosted_multiplier,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(covered_hexes)
}

pub(crate) fn calculated_coverage_points(covered_hexes: &[CoveredHex]) -> HexPoints {
    covered_hexes
        .iter()
        .fold(HexPoints::default(), |acc, hex| HexPoints {
            modeled: acc.modeled + hex.points.modeled,
            base: acc.base + hex.points.base,
            boosted: acc.boosted + hex.points.boosted,
        })
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use std::num::NonZeroU32;

    use coverage_map::SignalLevel;
    use hex_assignments::Assignment;

    use super::*;

    #[rstest]
    #[case(BoostedHexStatus::Eligible)]
    #[case(BoostedHexStatus::WifiLocationScoreBelowThreshold(dec!(999)))]
    #[case(BoostedHexStatus::RadioThresholdNotMet)]
    fn hip_103_provider_boosted_hex_receives_maximum_oracle_boost(
        #[case] boost_status: BoostedHexStatus,
    ) {
        // Regardless of the radio's eligibility to receive provider boosted
        // rewards, a boosted hex increases the oracle assignment.
        let unboosted_coverage = RankedCoverage {
            hotspot_key: vec![1],
            cbsd_id: None,
            hex: hextree::Cell::from_raw(0x8c2681a3064edff).unwrap(),
            rank: 1,
            signal_level: SignalLevel::High,
            assignments: HexAssignments {
                footfall: Assignment::C,
                landtype: Assignment::C,
                urbanized: Assignment::C,
            },
            boosted: NonZeroU32::new(0),
        };
        let boosted_coverage = RankedCoverage {
            boosted: NonZeroU32::new(5),
            ..unboosted_coverage.clone()
        };

        let covered_hexes = clean_covered_hexes(
            RadioType::IndoorWifi,
            boost_status,
            vec![unboosted_coverage, boosted_coverage],
        )
        .unwrap();

        let unboosted = &covered_hexes[0];
        let boosted = &covered_hexes[1];

        // unboosted receives original multiplier
        assert_eq!(dec!(0), unboosted.assignment_multiplier);

        // provider boosted gets oracle assignment bumped to 1x
        assert_eq!(dec!(1), boosted.assignment_multiplier);
    }
}
