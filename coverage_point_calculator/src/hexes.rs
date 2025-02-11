use coverage_map::RankedCoverage;
use hex_assignments::assignment::HexAssignments;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

use crate::{OracleBoostingStatus, RadioType, Result, SpBoostedHexStatus};

/// Breakdown of points for a hex.
///
/// Example:
/// - Outdoor Wifi
/// - 1 hex boosted at `5x`
/// - Rank 2
/// - Assignment: `AAA`
/// <pre>
/// SplitPoints {
///   modeled: 16,
///   base: 8,
///   boosted: 32
/// }
/// </pre>
/// Rank 2 splits modeled points in half.
/// Boost at `5x` adds 32 points `( (8 * 5) - 8 )`
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
    /// Points _over_ normal received from hex boosting.
    ///
    /// (base * Boost multiplier) - base
    pub boosted: Decimal,
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
    boosted_hex_status: SpBoostedHexStatus,
    ranked_coverage: Vec<RankedCoverage>,
    oracle_boosting_status: OracleBoostingStatus,
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

            // hip-131: if the radio is banned, it automatically gets an assignment_multiplier of 0.0
            // hip-103: if a hex is boosted by a service provider >=1x, the oracle
            //   multiplier will automatically be 1x, regardless of boosted_hex_status.
            // hip-134: qualified radios earn full Oracle Boosting rewards
            let assignment_multiplier = match oracle_boosting_status {
                OracleBoostingStatus::Qualified if radio_type.is_wifi() => dec!(1),
                OracleBoostingStatus::Banned => dec!(0),
                OracleBoostingStatus::Qualified | OracleBoostingStatus::Eligible => {
                    if ranked.boosted.is_some() {
                        dec!(1)
                    } else {
                        ranked.assignments.boosting_multiplier()
                    }
                }
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
    #[case(SpBoostedHexStatus::Eligible)]
    #[case(SpBoostedHexStatus::WifiLocationScoreBelowThreshold(dec!(999)))]
    #[case(SpBoostedHexStatus::RadioThresholdNotMet)]
    fn hip_103_provider_boosted_hex_receives_maximum_oracle_boost(
        #[case] boost_status: SpBoostedHexStatus,
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
                service_provider_override: Assignment::C,
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
            OracleBoostingStatus::Eligible,
        )
        .unwrap();

        let unboosted = &covered_hexes[0];
        let boosted = &covered_hexes[1];

        // unboosted receives original multiplier
        assert_eq!(dec!(0), unboosted.assignment_multiplier);

        // provider boosted gets oracle assignment bumped to 1x
        assert_eq!(dec!(1), boosted.assignment_multiplier);
    }

    #[rstest]
    fn hip131_banned_radio() {
        let unboosted_coverage = RankedCoverage {
            hotspot_key: vec![1],
            cbsd_id: None,
            hex: hextree::Cell::from_raw(0x8c2681a3064edff).unwrap(),
            rank: 1,
            signal_level: SignalLevel::High,
            assignments: HexAssignments {
                footfall: Assignment::A,
                landtype: Assignment::A,
                urbanized: Assignment::A,
                service_provider_override: Assignment::C,
            },
            boosted: NonZeroU32::new(0),
        };

        let boosted_coverage = RankedCoverage {
            boosted: NonZeroU32::new(5),
            ..unboosted_coverage.clone()
        };

        let covered_hexes = clean_covered_hexes(
            RadioType::IndoorWifi,
            SpBoostedHexStatus::Eligible,
            vec![unboosted_coverage, boosted_coverage],
            OracleBoostingStatus::Banned,
        )
        .unwrap();

        assert_eq!(dec!(0), covered_hexes[0].assignment_multiplier);
        assert_eq!(dec!(0), covered_hexes[1].assignment_multiplier);
    }

    #[rstest]
    fn hip134_qualified_radio(
        #[values(
            OracleBoostingStatus::Qualified,
            OracleBoostingStatus::Eligible,
            OracleBoostingStatus::Banned
        )]
        boost_status: OracleBoostingStatus,
        #[values(
            RadioType::IndoorCbrs,
            RadioType::OutdoorCbrs,
            RadioType::IndoorWifi,
            RadioType::OutdoorWifi
        )]
        radio_type: RadioType,
    ) {
        let coverage = RankedCoverage {
            hotspot_key: vec![1],
            cbsd_id: None,
            hex: hextree::Cell::from_raw(0x8c2681a3064edff).unwrap(),
            rank: 1,
            signal_level: SignalLevel::High,
            assignments: HexAssignments {
                footfall: Assignment::C,
                landtype: Assignment::C,
                urbanized: Assignment::C,
                service_provider_override: Assignment::C,
            },
            boosted: NonZeroU32::new(0),
        };

        let covered_hexes = clean_covered_hexes(
            radio_type,
            SpBoostedHexStatus::Eligible,
            vec![coverage],
            boost_status,
        )
        .unwrap();

        // Only Qualified WIFI radios should bypass bad assignment multiplier
        let expected_multiplier = match boost_status {
            OracleBoostingStatus::Qualified if radio_type.is_wifi() => dec!(1),
            _ => dec!(0),
        };

        assert_eq!(expected_multiplier, covered_hexes[0].assignment_multiplier);
    }
}
