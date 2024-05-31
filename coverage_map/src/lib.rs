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
    // TODO(map): Does this need to indicate whether the coverage is indoor or outdoor?
    pub hex: Cell,
    pub rank: usize,
    pub indoor: bool,
    pub hotspot_key: PublicKeyBinary,
    pub cbsd_id: Option<String>,
    pub assignments: HexAssignments,
    pub boosted: Option<NonZeroU32>,
    pub signal_level: SignalLevel,
}

#[derive(Copy, Clone, Debug, PartialOrd, Ord, PartialEq, Eq)]
pub enum SignalLevel {
    None,
    Low,
    Medium,
    High,
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
