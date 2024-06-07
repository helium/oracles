use coverage_map::RankedCoverage;
use hex_assignments::assignment::HexAssignments;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

use crate::{BoostedHexStatus, RadioType, Result};

#[derive(Debug, Clone)]
pub struct CoveredHex {
    pub hex: hextree::Cell,
    /// Default points received from (RadioType, SignalLevel) pair.
    pub base_coverage_points: Decimal,
    /// Coverage points including assignment, rank, and boosted hex multipliers.
    pub calculated_coverage_points: Decimal,
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
    ranked_coverage: Vec<RankedCoverage>,
    boosted_hex_status: BoostedHexStatus,
) -> Result<Vec<CoveredHex>> {
    let ranked_coverage = if !boosted_hex_status.is_eligible() {
        ranked_coverage
            .into_iter()
            .map(|ranked| RankedCoverage {
                boosted: None,
                ..ranked
            })
            .collect()
    } else {
        ranked_coverage
    };

    // verify all hexes can obtain a base coverage point
    let covered_hexes = ranked_coverage
        .into_iter()
        .map(|ranked| {
            let base_coverage_points = radio_type.base_coverage_points(&ranked.signal_level)?;
            let rank_multiplier = radio_type.rank_multiplier(ranked.rank);
            let assignment_multiplier = ranked.assignments.boosting_multiplier();
            let boosted_multiplier = ranked.boosted.map(|boost| boost.get()).map(Decimal::from);

            let calculated_coverage_points = base_coverage_points
                * assignment_multiplier
                * rank_multiplier
                * boosted_multiplier.unwrap_or(dec!(1));

            Ok(CoveredHex {
                hex: ranked.hex,
                base_coverage_points,
                calculated_coverage_points,
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

pub(crate) fn calculated_coverage_points(covered_hexes: &[CoveredHex]) -> Decimal {
    covered_hexes
        .iter()
        .map(|hex| hex.calculated_coverage_points)
        .sum()
}
