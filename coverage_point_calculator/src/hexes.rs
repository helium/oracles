use coverage_map::RankedCoverage;
use hex_assignments::assignment::HexAssignments;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

use crate::{BoostedHexStatus, RadioType, Result};

#[derive(Debug, Clone)]
pub struct CoveredHexes {
    pub hexes: Vec<CoveredHex>,
}

#[derive(Debug, Clone)]
pub struct CoveredHex {
    pub hex: hextree::Cell,
    // --
    pub base_coverage_points: Decimal,
    pub calculated_coverage_points: Decimal,
    // oracle boosted
    pub assignments: HexAssignments,
    pub assignment_multiplier: Decimal,
    // --
    pub rank: usize,
    pub rank_multiplier: Decimal,
    // provider boosted
    pub boosted_multiplier: Option<Decimal>,
}

impl CoveredHexes {
    pub fn new(
        radio_type: RadioType,
        ranked_coverage: Vec<RankedCoverage>,
        boosted_hex_status: BoostedHexStatus,
    ) -> Result<Self> {
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
                let coverage_points = radio_type.base_coverage_points(&ranked.signal_level)?;
                let assignment_multiplier = ranked.assignments.boosting_multiplier();
                let rank_multiplier = rank_multipliers
                    .get(ranked.rank - 1)
                    .cloned()
                    .unwrap_or(dec!(0));

                let boosted_multiplier = ranked.boosted.map(|boost| boost.get()).map(Decimal::from);

                let calculated_coverage_points = coverage_points
                    * assignment_multiplier
                    * rank_multiplier
                    * boosted_multiplier.unwrap_or(dec!(1));

                Ok(CoveredHex {
                    hex: ranked.hex,
                    base_coverage_points: coverage_points,
                    calculated_coverage_points,
                    assignments: ranked.assignments,
                    assignment_multiplier,
                    rank: ranked.rank,
                    rank_multiplier,
                    boosted_multiplier,
                })
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(Self {
            hexes: covered_hexes,
        })
    }

    pub fn calculated_coverage_points(&self) -> Decimal {
        self.hexes
            .iter()
            .map(|hex| hex.calculated_coverage_points)
            .sum()
    }
}
