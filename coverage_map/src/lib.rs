use std::{collections::HashMap, num::NonZeroU32};

use chrono::{DateTime, Utc};
use hex_assignments::assignment::HexAssignments;
use hextree::Cell;

mod indoor;
mod outdoor;

use indoor::*;
use outdoor::*;

/// Data structure for keeping track of the ranking the coverage in each hex cell for indoor
/// and outdoor WiFi radios.
#[derive(Clone, Default, Debug)]
pub struct CoverageMapBuilder {
    indoor_wifi: IndoorCellTree,
    outdoor_wifi: OutdoorCellTree,
}

impl CoverageMapBuilder {
    /// Inserts a new coverage object into the builder.
    pub fn insert_coverage_object(&mut self, coverage_obj: CoverageObject) {
        match coverage_obj.indoor {
            true => insert_indoor_coverage_object(&mut self.indoor_wifi, coverage_obj),
            false => insert_outdoor_coverage_object(&mut self.outdoor_wifi, coverage_obj),
        }
    }

    /// Creates a submap from the current `CoverageMapBuilder` and the provided `coverage_objs`.
    ///
    /// A submap only contains the hexes that exist in the provided `coverage_objs` arguments. This
    /// allows for one to determine the potential ranking of new coverage objects without having
    /// to clone the entire CoverageMapBuilder.
    pub fn submap(&self, coverage_objs: Vec<CoverageObject>) -> Self {
        // A different way to implement this function would be to insert all of the coverage_objs into
        // the submap, and then reconstruct the coverage objs from only the relevant hexes and then
        // insert them into the new coverage object builder.
        let mut new_submap = Self::default();
        for coverage_obj in coverage_objs {
            // Clone each of the hexes in the current coverage from the old map into the new submap:
            match coverage_obj.indoor {
                true => clone_indoor_coverage_into_submap(
                    &mut new_submap.indoor_wifi,
                    &self.indoor_wifi,
                    &coverage_obj,
                ),
                false => clone_outdoor_coverage_into_submap(
                    &mut new_submap.outdoor_wifi,
                    &self.outdoor_wifi,
                    &coverage_obj,
                ),
            }
            // Now that we are sure that each of the hexes in this coverage object are in the new
            // submap, we can insert the new coverage obj.
            new_submap.insert_coverage_object(coverage_obj);
        }
        new_submap
    }

    /// Constructs a [CoverageMap] from the current `CoverageMapBuilder`
    pub fn build(
        self,
        boosted_hexes: &impl BoostedHexMap,
        epoch_start: DateTime<Utc>,
    ) -> CoverageMap {
        let mut wifi_hotspots = HashMap::<_, Vec<RankedCoverage>>::new();
        for coverage in
            into_indoor_coverage_map(self.indoor_wifi, boosted_hexes, epoch_start).chain(
                into_outdoor_coverage_map(self.outdoor_wifi, boosted_hexes, epoch_start),
            )
        {
            wifi_hotspots
                .entry(coverage.hotspot_key.clone())
                .or_default()
                .push(coverage);
        }
        CoverageMap { wifi_hotspots }
    }
}

/// Data structure from mapping radios to their ranked hex coverage
#[derive(Clone, Default, Debug)]
pub struct CoverageMap {
    wifi_hotspots: HashMap<Vec<u8>, Vec<RankedCoverage>>,
}

impl CoverageMap {
    /// Returns the hexes covered by the WiFi hotspot. The returned slice can be empty, indicating that
    /// the hotspot did not meet the criteria to be ranked in any hex.
    pub fn get_wifi_coverage(&self, wifi_hotspot: &[u8]) -> &[RankedCoverage] {
        self.wifi_hotspots
            .get(wifi_hotspot)
            .map(Vec::as_slice)
            .unwrap_or(&[])
    }
}

/// Coverage data given as input to the [CoverageMapBuilder]
#[derive(Clone, Debug)]
pub struct CoverageObject {
    pub indoor: bool,
    pub hotspot_key: Vec<u8>,
    // TODO-K remove
    pub cbsd_id: Option<String>,
    pub seniority_timestamp: DateTime<Utc>,
    pub coverage: Vec<UnrankedCoverage>,
}

/// Unranked hex coverage data given as input to the [CoverageMapBuilder]
#[derive(Clone, Debug)]
pub struct UnrankedCoverage {
    pub location: Cell,
    pub signal_power: i32,
    pub signal_level: SignalLevel,
    pub assignments: HexAssignments,
}

/// Ranked hex coverage given as output from the [CoverageMap]
#[derive(Clone, Debug)]
pub struct RankedCoverage {
    pub hex: Cell,
    pub rank: usize,
    pub hotspot_key: Vec<u8>,
    pub cbsd_id: Option<String>,
    pub assignments: HexAssignments,
    pub boosted: Option<NonZeroU32>,
    pub signal_level: SignalLevel,
}

#[derive(Copy, Clone, Debug, PartialOrd, Ord, PartialEq, Eq)]
pub enum SignalLevel {
    High,
    Medium,
    Low,
    None,
}

pub trait BoostedHexMap {
    fn get_current_multiplier(&self, cell: Cell, ts: DateTime<Utc>) -> Option<NonZeroU32>;
}

#[cfg(test)]
pub(crate) struct NoBoostedHexes;

#[cfg(test)]
impl BoostedHexMap for NoBoostedHexes {
    fn get_current_multiplier(&self, _cell: Cell, _ts: DateTime<Utc>) -> Option<NonZeroU32> {
        None
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use hex_assignments::Assignment;

    #[test]
    fn test_indoor_wifi_submap() {
        let radio1 = vec![1, 1, 1];
        let radio2 = vec![1, 1, 2];
        let radio3 = vec![1, 1, 3];

        let mut coverage_map_builder = CoverageMapBuilder::default();
        coverage_map_builder.insert_coverage_object(indoor_wifi_coverage(
            &radio1,
            0x8a1fb46622dffff,
            SignalLevel::High,
        ));
        coverage_map_builder.insert_coverage_object(indoor_wifi_coverage(
            &radio2,
            0x8c2681a3064d9ff,
            SignalLevel::Low,
        ));
        let submap_builder = coverage_map_builder.submap(vec![indoor_wifi_coverage(
            &radio3,
            0x8c2681a3064d9ff,
            SignalLevel::High,
        )]);
        let submap = submap_builder.build(&NoBoostedHexes, Utc::now());
        let cov_1 = submap.get_wifi_coverage(&radio1);
        assert_eq!(cov_1.len(), 0);
        let cov_2 = submap.get_wifi_coverage(&radio2);
        assert_eq!(cov_2.len(), 1);
        assert_eq!(cov_2[0].rank, 2);
        let cov_3 = submap.get_wifi_coverage(&radio3);
        assert_eq!(cov_3.len(), 1);
        assert_eq!(cov_3[0].rank, 1);
    }

    #[test]
    fn test_outdoor_wifi_submap() {
        let radio1 = vec![1, 1, 1];
        let radio2 = vec![1, 1, 2];
        let radio3 = vec![1, 1, 3];

        let mut coverage_map_builder = CoverageMapBuilder::default();
        coverage_map_builder.insert_coverage_object(outdoor_wifi_coverage(
            &radio1,
            0x8a1fb46622dffff,
            3,
        ));
        coverage_map_builder.insert_coverage_object(outdoor_wifi_coverage(
            &radio2,
            0x8c2681a3064d9ff,
            1,
        ));
        let submap_builder =
            coverage_map_builder.submap(vec![outdoor_wifi_coverage(&radio3, 0x8c2681a3064d9ff, 2)]);
        let submap = submap_builder.build(&NoBoostedHexes, Utc::now());
        let cov_1 = submap.get_wifi_coverage(&radio1);
        assert_eq!(cov_1.len(), 0);
        let cov_2 = submap.get_wifi_coverage(&radio2);
        assert_eq!(cov_2.len(), 1);
        assert_eq!(cov_2[0].rank, 2);
        let cov_3 = submap.get_wifi_coverage(&radio3);
        assert_eq!(cov_3.len(), 1);
        assert_eq!(cov_3[0].rank, 1);
    }

    fn hex_assignments_mock() -> HexAssignments {
        HexAssignments {
            footfall: Assignment::A,
            urbanized: Assignment::A,
            landtype: Assignment::A,
            service_provider_override: Assignment::C,
        }
    }

    fn indoor_wifi_coverage(owner: &[u8], hex: u64, signal_level: SignalLevel) -> CoverageObject {
        CoverageObject {
            indoor: true,
            hotspot_key: owner.to_vec(),
            seniority_timestamp: Utc::now(),
            cbsd_id: None,
            coverage: vec![UnrankedCoverage {
                location: Cell::from_raw(hex).expect("valid h3 cell"),
                signal_power: 0,
                signal_level,
                assignments: hex_assignments_mock(),
            }],
        }
    }

    fn outdoor_wifi_coverage(owner: &[u8], hex: u64, signal_power: i32) -> CoverageObject {
        CoverageObject {
            indoor: false,
            hotspot_key: owner.to_vec(),
            seniority_timestamp: Utc::now(),
            cbsd_id: None,
            coverage: vec![UnrankedCoverage {
                location: Cell::from_raw(hex).expect("valid h3 cell"),
                signal_power,
                signal_level: SignalLevel::None,
                assignments: hex_assignments_mock(),
            }],
        }
    }
}
