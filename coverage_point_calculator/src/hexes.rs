use coverage_map::RankedCoverage;
use hex_assignments::assignment::HexAssignments;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

use crate::{OracleBoostingStatus, RadioType, Result};

/// Breakdown of points for a hex.
///
/// Example:
/// - Outdoor Wifi
/// - Rank 2
/// - Assignment: `AAA`
/// <pre>
/// SplitPoints {
///   modeled: 16,
///   base: 8,
/// }
/// </pre>
/// Rank 2 splits modeled points in half.
#[derive(Debug, Default, Clone)]
pub struct HexPoints {
    /// Default points received for hex
    ///
    /// (RadioType, SignalLevel) points
    ///
    /// This is a convenience field for debugging, hexes can reach similar
    /// values through different means, it helps to know the starting value.
    pub modeled: Decimal,
    /// Points including Coverage affected multipliers
    ///
    /// modeled + (Rank * Assignment)
    pub base: Decimal,
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
}

pub(crate) fn clean_covered_hexes(
    radio_type: RadioType,
    ranked_coverage: Vec<RankedCoverage>,
    oracle_boosting_status: OracleBoostingStatus,
) -> Result<Vec<CoveredHex>> {
    // verify all hexes can obtain a base coverage point
    let covered_hexes = ranked_coverage
        .into_iter()
        .map(|ranked| {
            let modeled_coverage_points = radio_type.base_coverage_points(&ranked.signal_level)?;
            let rank_multiplier = radio_type.rank_multiplier(ranked.rank);

            // hip-134: qualified radios earn full Oracle Boosting rewards
            let assignment_multiplier = match oracle_boosting_status {
                OracleBoostingStatus::Qualified => dec!(1),
                OracleBoostingStatus::Eligible => ranked.assignments.boosting_multiplier(),
            };

            let base_coverage_points =
                modeled_coverage_points * assignment_multiplier * rank_multiplier;

            Ok(CoveredHex {
                hex: ranked.hex,
                points: HexPoints {
                    modeled: modeled_coverage_points,
                    base: base_coverage_points,
                },
                assignments: ranked.assignments,
                assignment_multiplier,
                rank: ranked.rank,
                rank_multiplier,
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
        })
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use coverage_map::SignalLevel;
    use hex_assignments::Assignment;

    use super::*;

    #[rstest]
    fn hip134_qualified_radio(
        #[values(OracleBoostingStatus::Qualified, OracleBoostingStatus::Eligible)]
        boost_status: OracleBoostingStatus,
        #[values(RadioType::IndoorWifi, RadioType::OutdoorWifi)] radio_type: RadioType,
    ) {
        let coverage = RankedCoverage {
            hotspot_key: vec![1],
            hex: hextree::Cell::from_raw(0x8c2681a3064edff).unwrap(),
            rank: 1,
            signal_level: SignalLevel::High,
            assignments: HexAssignments {
                footfall: Assignment::C,
                landtype: Assignment::C,
                urbanized: Assignment::C,
                service_provider_override: Assignment::C,
            },
        };

        let covered_hexes = clean_covered_hexes(radio_type, vec![coverage], boost_status).unwrap();

        // Only Qualified WIFI radios should bypass bad assignment multiplier
        let expected_multiplier = match boost_status {
            OracleBoostingStatus::Qualified => dec!(1),
            OracleBoostingStatus::Eligible => dec!(0),
        };

        assert_eq!(expected_multiplier, covered_hexes[0].assignment_multiplier);
    }
}
