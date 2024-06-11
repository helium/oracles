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
            let boosted_multiplier = ranked.boosted.map(|boost| boost.get()).map(Decimal::from);

            // hip-103: if a hex is boosted by a service provider >1x, the oracle
            // multiplier will automatically be 1x, regardless of boosted_hex_status.
            let assignment_multiplier = if ranked.boosted.is_some() {
                dec!(1)
            } else {
                ranked.assignments.boosting_multiplier()
            };

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

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;

    use coverage_map::SignalLevel;
    use hex_assignments::Assignment;

    use super::*;

    #[test]
    fn hip_103_provider_boosted_hex_receives_maximum_oracle_boost() {
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
            vec![unboosted_coverage.clone(), boosted_coverage],
            BoostedHexStatus::Eligible,
        )
        .unwrap();

        let unboosted = &covered_hexes[0];
        let boosted = &covered_hexes[1];

        // unboosted receives original multiplier
        assert_eq!(dec!(0), unboosted.calculated_coverage_points);
        assert_eq!(
            unboosted_coverage.assignments.boosting_multiplier(),
            unboosted.assignment_multiplier
        );

        // provider boosted gets oracle assignment bumped to 1x
        assert_eq!(dec!(1), boosted.assignment_multiplier);
        assert_eq!(
            RadioType::IndoorWifi
                .base_coverage_points(&SignalLevel::High)
                .unwrap_or_default()
                * dec!(5),
            boosted.calculated_coverage_points
        );
    }
}
