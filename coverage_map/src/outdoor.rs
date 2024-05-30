use std::{
    cmp::Ordering,
    collections::{hash_map::Entry, BinaryHeap, HashMap},
};

use chrono::{DateTime, Utc};
use helium_crypto::PublicKeyBinary;
use hex_assignments::assignment::HexAssignments;
use hextree::Cell;
use mobile_config::boosted_hex_info::BoostedHexes;

use crate::{
    CoverageObject, Rank, RankedCoverage, SignalLevel, UnrankedCoverage,
    MAX_OUTDOOR_RADIOS_PER_RES12_HEX,
};

/// Data structure for storing outdoor radios ranked by their coverage level
pub type OutdoorCellTree = HashMap<Cell, BinaryHeap<OutdoorCoverageLevel>>;

#[derive(Eq, Debug, Clone)]
pub struct OutdoorCoverageLevel {
    hotspot_key: PublicKeyBinary,
    cbsd_id: Option<String>,
    seniority_timestamp: DateTime<Utc>,
    signal_power: i32,
    signal_level: SignalLevel,
    assignments: HexAssignments,
}

impl PartialEq for OutdoorCoverageLevel {
    fn eq(&self, other: &Self) -> bool {
        self.signal_power == other.signal_power
            && self.seniority_timestamp == other.seniority_timestamp
    }
}

impl PartialOrd for OutdoorCoverageLevel {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OutdoorCoverageLevel {
    fn cmp(&self, other: &Self) -> Ordering {
        self.signal_power
            .cmp(&other.signal_power)
            .reverse()
            .then_with(|| self.seniority_timestamp.cmp(&other.seniority_timestamp))
    }
}

pub fn insert_outdoor_coverage_object(
    indoor: &mut OutdoorCellTree,
    coverage_object: CoverageObject,
) {
    for hex_coverage in coverage_object.coverage.into_iter() {
        insert_outdoor_coverage(
            indoor,
            &coverage_object.hotspot_key,
            &coverage_object.cbsd_id,
            coverage_object.seniority_timestamp,
            hex_coverage,
        );
    }
}

pub fn insert_outdoor_coverage(
    outdoor: &mut OutdoorCellTree,
    hotspot: &PublicKeyBinary,
    cbsd_id: &Option<String>,
    seniority_timestamp: DateTime<Utc>,
    hex_coverage: UnrankedCoverage,
) {
    outdoor
        .entry(hex_coverage.location)
        .or_default()
        .push(OutdoorCoverageLevel {
            hotspot_key: hotspot.clone(),
            cbsd_id: cbsd_id.clone(),
            seniority_timestamp,
            signal_level: hex_coverage.signal_level,
            signal_power: hex_coverage.signal_power,
            assignments: hex_coverage.assignments,
        });
}

pub fn clone_outdoor_coverage_into_submap(
    submap: &mut OutdoorCellTree,
    from: &OutdoorCellTree,
    coverage_obj: &CoverageObject,
) {
    for coverage in &coverage_obj.coverage {
        if let Entry::Vacant(e) = submap.entry(coverage.location) {
            if let Some(old_coverage_data) = from.get(&coverage.location) {
                e.insert(old_coverage_data.clone());
            }
        }
    }
}

pub fn into_outdoor_coverage_map(
    outdoor: OutdoorCellTree,
    boosted_hexes: &BoostedHexes,
    epoch_start: DateTime<Utc>,
) -> impl Iterator<Item = (PublicKeyBinary, RankedCoverage)> + '_ {
    outdoor.into_iter().flat_map(move |(hex, radios)| {
        let boosted = boosted_hexes.get_current_multiplier(hex, epoch_start);
        radios
            .into_sorted_vec()
            .into_iter()
            .take(MAX_OUTDOOR_RADIOS_PER_RES12_HEX)
            .enumerate()
            .flat_map(move |(rank, cov)| {
                Rank::from_outdoor_index(rank).map(move |rank| {
                    let key = cov.hotspot_key;
                    let cov = RankedCoverage {
                        rank,
                        hex,
                        cbsd_id: cov.cbsd_id,
                        assignments: cov.assignments,
                        boosted,
                        signal_level: cov.signal_level,
                    };
                    (key, cov)
                })
            })
    })
}
