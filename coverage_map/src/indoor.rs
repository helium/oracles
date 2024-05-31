use std::{
    cmp::Ordering,
    collections::{hash_map::Entry, BTreeMap, BinaryHeap, HashMap},
};

use chrono::{DateTime, Utc};
use helium_crypto::PublicKeyBinary;
use hex_assignments::assignment::HexAssignments;
use hextree::Cell;
use mobile_config::boosted_hex_info::BoostedHexes;

use crate::{
    CoverageObject, Rank, RankedCoverage, SignalLevel, UnrankedCoverage,
    MAX_INDOOR_RADIOS_PER_RES12_HEX,
};

pub type IndoorCellTree = HashMap<Cell, BTreeMap<SignalLevel, BinaryHeap<IndoorCoverageLevel>>>;

#[derive(Eq, Debug, Clone)]
pub struct IndoorCoverageLevel {
    hotspot_key: PublicKeyBinary,
    cbsd_id: Option<String>,
    seniority_timestamp: DateTime<Utc>,
    signal_level: SignalLevel,
    assignments: HexAssignments,
}

impl PartialEq for IndoorCoverageLevel {
    fn eq(&self, other: &Self) -> bool {
        self.seniority_timestamp == other.seniority_timestamp
    }
}

impl PartialOrd for IndoorCoverageLevel {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for IndoorCoverageLevel {
    fn cmp(&self, other: &Self) -> Ordering {
        self.seniority_timestamp.cmp(&other.seniority_timestamp)
    }
}

pub fn insert_indoor_coverage_object(indoor: &mut IndoorCellTree, coverage_object: CoverageObject) {
    for hex_coverage in coverage_object.coverage.into_iter() {
        insert_indoor_coverage(
            indoor,
            &coverage_object.hotspot_key,
            &coverage_object.cbsd_id,
            coverage_object.seniority_timestamp,
            hex_coverage,
        );
    }
}

pub fn insert_indoor_coverage(
    indoor: &mut IndoorCellTree,
    hotspot: &PublicKeyBinary,
    cbsd_id: &Option<String>,
    seniority_timestamp: DateTime<Utc>,
    hex_coverage: UnrankedCoverage,
) {
    indoor
        .entry(hex_coverage.location)
        .or_default()
        .entry(hex_coverage.signal_level)
        .or_default()
        .push(IndoorCoverageLevel {
            hotspot_key: hotspot.clone(),
            cbsd_id: cbsd_id.clone(),
            seniority_timestamp,
            signal_level: hex_coverage.signal_level,
            assignments: hex_coverage.assignments,
        })
}

pub fn clone_indoor_coverage_into_submap(
    submap: &mut IndoorCellTree,
    from: &IndoorCellTree,
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

pub fn into_indoor_coverage_map(
    indoor: IndoorCellTree,
    boosted_hexes: &BoostedHexes,
    epoch_start: DateTime<Utc>,
) -> impl Iterator<Item = RankedCoverage> + '_ {
    indoor
        .into_iter()
        .flat_map(move |(hex, mut radios)| {
            let boosted = boosted_hexes.get_current_multiplier(hex, epoch_start);
            radios.pop_last().map(move |(_, radios)| {
                radios
                    .into_sorted_vec()
                    .into_iter()
                    .take(MAX_INDOOR_RADIOS_PER_RES12_HEX)
                    .enumerate()
                    .flat_map(move |(rank, cov)| {
                        Rank::from_indoor_index(rank).map(move |rank| RankedCoverage {
                            hex,
                            rank,
                            hotspot_key: cov.hotspot_key,
                            cbsd_id: cov.cbsd_id,
                            assignments: cov.assignments,
                            boosted,
                            signal_level: cov.signal_level,
                        })
                    })
            })
        })
        .flatten()
}
