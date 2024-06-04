use std::{collections::HashMap, num::NonZeroU32};

use chrono::{DateTime, Utc};
use helium_crypto::PublicKeyBinary;
use hex_assignments::assignment::HexAssignments;
use hextree::Cell;

mod indoor;
mod outdoor;

use indoor::*;
use outdoor::*;

/// Data structure for keeping track of the ranking the coverage in each hex cell for indoor
/// and outdoor CBRS and WiFi radios.
#[derive(Clone, Default, Debug)]
pub struct CoverageMapBuilder {
    indoor_cbrs: IndoorCellTree,
    indoor_wifi: IndoorCellTree,
    outdoor_cbrs: OutdoorCellTree,
    outdoor_wifi: OutdoorCellTree,
}

impl CoverageMapBuilder {
    /// Inserts a new coverage object into the builder.
    pub fn insert_coverage_object(&mut self, coverage_obj: CoverageObject) {
        match (coverage_obj.indoor, coverage_obj.cbsd_id.is_some()) {
            (true, true) => insert_indoor_coverage_object(&mut self.indoor_cbrs, coverage_obj),
            (true, false) => insert_indoor_coverage_object(&mut self.indoor_wifi, coverage_obj),
            (false, true) => insert_outdoor_coverage_object(&mut self.outdoor_cbrs, coverage_obj),
            (false, false) => insert_outdoor_coverage_object(&mut self.outdoor_wifi, coverage_obj),
        }
    }

    /// Creates a submap from the current `CoverageMapBuilder` and the provided `coverage_objs`.
    ///
    /// A submap only contains the hexes that exist in the provided `coverage_objs` arguments. This
    /// allows for one to determine the potential ranking of new coverage objects without having
    /// to clone the entire CoverageMapBuilder.
    // TODO(map): Should this return a `CoverageMap` instead? I don't really see the purpose of
    // having this return a `CoverageMapBuilder` since it will probably always be converted instantly
    // to a `CoverageMap`.
    pub fn submap(&self, coverage_objs: Vec<CoverageObject>) -> Self {
        // A different way to implement this function would be to insert all of the coverage_objs into
        // the submap, and then reconstruct the coverage objs from only the relevant hexes and then
        // insert them into the new coverage object builder.
        let mut new_submap = Self::default();
        for coverage_obj in coverage_objs {
            // Clone each of the hexes in the current coverage from the old map into the new submap:
            match (coverage_obj.indoor, coverage_obj.cbsd_id.is_some()) {
                (true, true) => clone_indoor_coverage_into_submap(
                    &mut new_submap.indoor_cbrs,
                    &self.indoor_cbrs,
                    &coverage_obj,
                ),
                (true, false) => clone_indoor_coverage_into_submap(
                    &mut new_submap.indoor_wifi,
                    &self.indoor_wifi,
                    &coverage_obj,
                ),
                (false, true) => clone_outdoor_coverage_into_submap(
                    &mut new_submap.outdoor_cbrs,
                    &self.outdoor_cbrs,
                    &coverage_obj,
                ),
                (false, false) => clone_outdoor_coverage_into_submap(
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
        let mut cbrs_radios = HashMap::<_, Vec<RankedCoverage>>::new();
        for coverage in into_indoor_coverage_map(self.indoor_cbrs, boosted_hexes, epoch_start)
            .chain(into_indoor_coverage_map(
                self.indoor_wifi,
                boosted_hexes,
                epoch_start,
            ))
            .chain(into_outdoor_coverage_map(
                self.outdoor_cbrs,
                boosted_hexes,
                epoch_start,
            ))
            .chain(into_outdoor_coverage_map(
                self.outdoor_wifi,
                boosted_hexes,
                epoch_start,
            ))
        {
            if let Some(ref cbsd_id) = coverage.cbsd_id {
                cbrs_radios
                    .entry(cbsd_id.clone())
                    .or_default()
                    .push(coverage);
            } else {
                wifi_hotspots
                    .entry(coverage.hotspot_key.clone())
                    .or_default()
                    .push(coverage);
            }
        }
        CoverageMap {
            wifi_hotspots,
            cbrs_radios,
        }
    }
}

/// Data structure from mapping radios to their ranked hex coverage
#[derive(Clone, Default, Debug)]
pub struct CoverageMap {
    wifi_hotspots: HashMap<PublicKeyBinary, Vec<RankedCoverage>>,
    cbrs_radios: HashMap<String, Vec<RankedCoverage>>,
}

impl CoverageMap {
    /// Returns the hexes covered by the WiFi hotspot. The returned slice can be empty, indicating that
    /// the hotspot did not meet the criteria to be ranked in any hex.
    pub fn get_wifi_coverage(&self, wifi_hotspot: &PublicKeyBinary) -> &[RankedCoverage] {
        self.wifi_hotspots
            .get(wifi_hotspot)
            .map(Vec::as_slice)
            .unwrap_or(&[])
    }

    /// Returns the hexes covered by the CBRS radio. The returned slice can be empty, indicating that
    /// the radio did not meet the criteria to be ranked in any hex.
    pub fn get_cbrs_coverage(&self, cbrs_radio: &str) -> &[RankedCoverage] {
        self.cbrs_radios
            .get(cbrs_radio)
            .map(Vec::as_slice)
            .unwrap_or(&[])
    }
}

/// Coverage data given as input to the [CoverageMapBuilder]
#[derive(Clone, Debug)]
pub struct CoverageObject {
    pub indoor: bool,
    pub hotspot_key: PublicKeyBinary,
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
    pub hotspot_key: PublicKeyBinary,
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
    fn test_indoor_cbrs_submap() {
        let mut coverage_map_builder = CoverageMapBuilder::default();
        coverage_map_builder.insert_coverage_object(indoor_cbrs_coverage(
            "1",
            0x8a1fb46622dffff,
            SignalLevel::High,
        ));
        coverage_map_builder.insert_coverage_object(indoor_cbrs_coverage(
            "2",
            0x8c2681a3064d9ff,
            SignalLevel::Low,
        ));
        let submap_builder = coverage_map_builder.submap(vec![indoor_cbrs_coverage(
            "3",
            0x8c2681a3064d9ff,
            SignalLevel::High,
        )]);
        let submap = submap_builder.build(&NoBoostedHexes, Utc::now());
        let cov_1 = submap.get_cbrs_coverage("1");
        assert_eq!(cov_1.len(), 0);
        let cov_2 = submap.get_cbrs_coverage("2");
        assert_eq!(cov_2.len(), 1);
        assert_eq!(cov_2[0].rank, 2);
        let cov_3 = submap.get_cbrs_coverage("3");
        assert_eq!(cov_3.len(), 1);
        assert_eq!(cov_3[0].rank, 1);
    }

    #[test]
    fn test_indoor_wifi_submap() {
        let mut coverage_map_builder = CoverageMapBuilder::default();
        coverage_map_builder.insert_coverage_object(indoor_wifi_coverage(
            "11xtYwQYnvkFYnJ9iZ8kmnetYKwhdi87Mcr36e1pVLrhBMPLjV9",
            0x8a1fb46622dffff,
            SignalLevel::High,
        ));
        coverage_map_builder.insert_coverage_object(indoor_wifi_coverage(
            "11PGVtgW9aM9ynfvns5USUsynYQ7EsMpxVqWuDKqFogKQX7etkR",
            0x8c2681a3064d9ff,
            SignalLevel::Low,
        ));
        let submap_builder = coverage_map_builder.submap(vec![indoor_wifi_coverage(
            "11ibmJmQXTL6qMh4cq9pJ7tUtrpafWaVjjT6qhY7CNvjyvY9g1",
            0x8c2681a3064d9ff,
            SignalLevel::High,
        )]);
        let submap = submap_builder.build(&NoBoostedHexes, Utc::now());
        let cov_1 = submap.get_wifi_coverage(
            &"11xtYwQYnvkFYnJ9iZ8kmnetYKwhdi87Mcr36e1pVLrhBMPLjV9"
                .parse()
                .unwrap(),
        );
        assert_eq!(cov_1.len(), 0);
        let cov_2 = submap.get_wifi_coverage(
            &"11PGVtgW9aM9ynfvns5USUsynYQ7EsMpxVqWuDKqFogKQX7etkR"
                .parse()
                .unwrap(),
        );
        assert_eq!(cov_2.len(), 1);
        assert_eq!(cov_2[0].rank, 2);
        let cov_3 = submap.get_wifi_coverage(
            &"11ibmJmQXTL6qMh4cq9pJ7tUtrpafWaVjjT6qhY7CNvjyvY9g1"
                .parse()
                .unwrap(),
        );
        assert_eq!(cov_3.len(), 1);
        assert_eq!(cov_3[0].rank, 1);
    }

    #[test]
    fn test_outdoor_cbrs_submap() {
        let mut coverage_map_builder = CoverageMapBuilder::default();
        coverage_map_builder.insert_coverage_object(outdoor_cbrs_coverage(
            "1",
            0x8a1fb46622dffff,
            3,
        ));
        coverage_map_builder.insert_coverage_object(outdoor_cbrs_coverage(
            "2",
            0x8c2681a3064d9ff,
            1,
        ));
        let submap_builder =
            coverage_map_builder.submap(vec![outdoor_cbrs_coverage("3", 0x8c2681a3064d9ff, 2)]);
        let submap = submap_builder.build(&NoBoostedHexes, Utc::now());
        let cov_1 = submap.get_cbrs_coverage("1");
        assert_eq!(cov_1.len(), 0);
        let cov_2 = submap.get_cbrs_coverage("2");
        assert_eq!(cov_2.len(), 1);
        assert_eq!(cov_2[0].rank, 2);
        let cov_3 = submap.get_cbrs_coverage("3");
        assert_eq!(cov_3.len(), 1);
        assert_eq!(cov_3[0].rank, 1);
    }

    #[test]
    fn test_outdoor_wifi_submap() {
        let mut coverage_map_builder = CoverageMapBuilder::default();
        coverage_map_builder.insert_coverage_object(outdoor_wifi_coverage(
            "11xtYwQYnvkFYnJ9iZ8kmnetYKwhdi87Mcr36e1pVLrhBMPLjV9",
            0x8a1fb46622dffff,
            3,
        ));
        coverage_map_builder.insert_coverage_object(outdoor_wifi_coverage(
            "11PGVtgW9aM9ynfvns5USUsynYQ7EsMpxVqWuDKqFogKQX7etkR",
            0x8c2681a3064d9ff,
            1,
        ));
        let submap_builder = coverage_map_builder.submap(vec![outdoor_wifi_coverage(
            "11ibmJmQXTL6qMh4cq9pJ7tUtrpafWaVjjT6qhY7CNvjyvY9g1",
            0x8c2681a3064d9ff,
            2,
        )]);
        let submap = submap_builder.build(&NoBoostedHexes, Utc::now());
        let cov_1 = submap.get_wifi_coverage(
            &"11xtYwQYnvkFYnJ9iZ8kmnetYKwhdi87Mcr36e1pVLrhBMPLjV9"
                .parse()
                .unwrap(),
        );
        assert_eq!(cov_1.len(), 0);
        let cov_2 = submap.get_wifi_coverage(
            &"11PGVtgW9aM9ynfvns5USUsynYQ7EsMpxVqWuDKqFogKQX7etkR"
                .parse()
                .unwrap(),
        );
        assert_eq!(cov_2.len(), 1);
        assert_eq!(cov_2[0].rank, 2);
        let cov_3 = submap.get_wifi_coverage(
            &"11ibmJmQXTL6qMh4cq9pJ7tUtrpafWaVjjT6qhY7CNvjyvY9g1"
                .parse()
                .unwrap(),
        );
        assert_eq!(cov_3.len(), 1);
        assert_eq!(cov_3[0].rank, 1);
    }

    fn hex_assignments_mock() -> HexAssignments {
        HexAssignments {
            footfall: Assignment::A,
            urbanized: Assignment::A,
            landtype: Assignment::A,
        }
    }

    fn indoor_cbrs_coverage(cbsd_id: &str, hex: u64, signal_level: SignalLevel) -> CoverageObject {
        let owner: PublicKeyBinary = "112NqN2WWMwtK29PMzRby62fDydBJfsCLkCAf392stdok48ovNT6"
            .parse()
            .expect("failed owner parse");
        CoverageObject {
            indoor: true,
            hotspot_key: owner,
            seniority_timestamp: Utc::now(),
            cbsd_id: Some(cbsd_id.to_string()),
            coverage: vec![UnrankedCoverage {
                location: Cell::from_raw(hex).expect("valid h3 cell"),
                signal_power: 0,
                signal_level,
                assignments: hex_assignments_mock(),
            }],
        }
    }

    fn indoor_wifi_coverage(owner: &str, hex: u64, signal_level: SignalLevel) -> CoverageObject {
        let owner: PublicKeyBinary = owner.parse().expect("failed owner parse");
        CoverageObject {
            indoor: true,
            hotspot_key: owner,
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

    fn outdoor_cbrs_coverage(cbsd_id: &str, hex: u64, signal_power: i32) -> CoverageObject {
        let owner: PublicKeyBinary = "112NqN2WWMwtK29PMzRby62fDydBJfsCLkCAf392stdok48ovNT6"
            .parse()
            .expect("failed owner parse");
        CoverageObject {
            indoor: false,
            hotspot_key: owner,
            seniority_timestamp: Utc::now(),
            cbsd_id: Some(cbsd_id.to_string()),
            coverage: vec![UnrankedCoverage {
                location: Cell::from_raw(hex).expect("valid h3 cell"),
                signal_power,
                signal_level: SignalLevel::None,
                assignments: hex_assignments_mock(),
            }],
        }
    }

    fn outdoor_wifi_coverage(owner: &str, hex: u64, signal_power: i32) -> CoverageObject {
        let owner: PublicKeyBinary = owner.parse().expect("failed owner parse");
        CoverageObject {
            indoor: false,
            hotspot_key: owner,
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
