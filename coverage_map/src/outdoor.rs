use std::{
    cmp::Ordering,
    collections::{hash_map::Entry, BinaryHeap, HashMap},
};

use chrono::{DateTime, Utc};
use helium_crypto::PublicKeyBinary;
use hex_assignments::assignment::HexAssignments;
use hextree::Cell;

use crate::{BoostedHexMap, CoverageObject, RankedCoverage, SignalLevel, UnrankedCoverage};

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
    outdoor: &mut OutdoorCellTree,
    coverage_object: CoverageObject,
) {
    for hex_coverage in coverage_object.coverage.into_iter() {
        insert_outdoor_coverage(
            outdoor,
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
    boosted_hexes: &impl BoostedHexMap,
    epoch_start: DateTime<Utc>,
) -> impl Iterator<Item = RankedCoverage> + '_ {
    outdoor.into_iter().flat_map(move |(hex, radios)| {
        let boosted = boosted_hexes.get_current_multiplier(hex, epoch_start);
        radios
            .into_sorted_vec()
            .into_iter()
            .enumerate()
            .map(move |(rank, cov)| RankedCoverage {
                hex,
                rank: rank + 1,
                hotspot_key: cov.hotspot_key,
                cbsd_id: cov.cbsd_id,
                assignments: cov.assignments,
                boosted,
                signal_level: cov.signal_level,
            })
    })
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::*;
    use chrono::NaiveDate;
    use hex_assignments::Assignment;
    use hextree::Cell;

    #[test]
    fn ensure_outdoor_radios_ranked_by_power() {
        let mut outdoor_coverage = OutdoorCellTree::default();
        for cov_obj in vec![
            outdoor_cbrs_coverage("1", -946, date(2022, 8, 1)),
            outdoor_cbrs_coverage("2", -936, date(2022, 12, 5)),
            outdoor_cbrs_coverage("3", -887, date(2022, 12, 2)),
            outdoor_cbrs_coverage("4", -887, date(2022, 12, 1)),
            outdoor_cbrs_coverage("5", -773, date(2023, 5, 1)),
        ]
        .into_iter()
        {
            insert_outdoor_coverage_object(&mut outdoor_coverage, cov_obj);
        }
        let ranked: HashMap<_, _> =
            into_outdoor_coverage_map(outdoor_coverage, &NoBoostedHexes, Utc::now())
                .map(|x| (x.cbsd_id.clone().unwrap(), x))
                .collect();
        assert_eq!(ranked.get("5").unwrap().rank, 1);
        assert_eq!(ranked.get("4").unwrap().rank, 2);
        assert_eq!(ranked.get("3").unwrap().rank, 3);
        assert_eq!(ranked.get("1").unwrap().rank, 5);
        assert_eq!(ranked.get("2").unwrap().rank, 4);
    }

    fn hex_assignments_mock() -> HexAssignments {
        HexAssignments {
            footfall: Assignment::A,
            urbanized: Assignment::A,
            landtype: Assignment::A,
        }
    }

    fn date(year: i32, month: u32, day: u32) -> DateTime<Utc> {
        NaiveDate::from_ymd_opt(year, month, day)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap()
            .and_utc()
    }

    fn outdoor_cbrs_coverage(
        cbsd_id: &str,
        signal_power: i32,
        seniority_timestamp: DateTime<Utc>,
    ) -> CoverageObject {
        let owner: PublicKeyBinary = "112NqN2WWMwtK29PMzRby62fDydBJfsCLkCAf392stdok48ovNT6"
            .parse()
            .expect("failed owner parse");
        CoverageObject {
            indoor: false,
            hotspot_key: owner,
            seniority_timestamp,
            cbsd_id: Some(cbsd_id.to_string()),
            coverage: vec![UnrankedCoverage {
                location: Cell::from_raw(0x8a1fb46622dffff).expect("valid h3 cell"),
                signal_power,
                signal_level: SignalLevel::High,
                assignments: hex_assignments_mock(),
            }],
        }
    }
}
