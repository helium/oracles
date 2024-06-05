use std::{
    cmp::Ordering,
    collections::{hash_map::Entry, BTreeMap, BinaryHeap, HashMap},
};

use chrono::{DateTime, Utc};
use helium_crypto::PublicKeyBinary;
use hex_assignments::assignment::HexAssignments;
use hextree::Cell;

use crate::{BoostedHexMap, CoverageObject, RankedCoverage, SignalLevel, UnrankedCoverage};

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
    boosted_hexes: &impl BoostedHexMap,
    epoch_start: DateTime<Utc>,
) -> impl Iterator<Item = RankedCoverage> + '_ {
    indoor.into_iter().flat_map(move |(hex, radios)| {
        let boosted = boosted_hexes.get_current_multiplier(hex, epoch_start);
        radios
            .into_values()
            .flat_map(move |radios| radios.into_sorted_vec().into_iter())
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
    fn ensure_max_signal_level_selected() {
        let mut indoor_coverage = IndoorCellTree::default();
        for cov_obj in vec![
            indoor_cbrs_coverage("1", SignalLevel::None),
            indoor_cbrs_coverage("2", SignalLevel::Low),
            indoor_cbrs_coverage("3", SignalLevel::High),
            indoor_cbrs_coverage("4", SignalLevel::Low),
            indoor_cbrs_coverage("5", SignalLevel::None),
        ]
        .into_iter()
        {
            insert_indoor_coverage_object(&mut indoor_coverage, cov_obj);
        }
        let ranked: HashMap<_, _> =
            into_indoor_coverage_map(indoor_coverage, &NoBoostedHexes, Utc::now())
                .map(|x| (x.cbsd_id.clone().unwrap(), x))
                .collect();
        assert_eq!(ranked.get("3").unwrap().rank, 1);
        assert!({
            let rank = ranked.get("2").unwrap().rank;
            rank == 2 || rank == 3
        });
        assert!({
            let rank = ranked.get("4").unwrap().rank;
            rank == 2 || rank == 3
        });
        assert!({
            let rank = ranked.get("1").unwrap().rank;
            rank == 4 || rank == 5
        });
        assert!({
            let rank = ranked.get("5").unwrap().rank;
            rank == 4 || rank == 5
        });
    }

    #[test]
    fn ensure_oldest_radio_selected() {
        let mut indoor_coverage = IndoorCellTree::default();
        for cov_obj in vec![
            indoor_cbrs_coverage_with_date("1", SignalLevel::High, date(1980, 1, 1)),
            indoor_cbrs_coverage_with_date("2", SignalLevel::High, date(1970, 1, 5)),
            indoor_cbrs_coverage_with_date("3", SignalLevel::High, date(1990, 2, 2)),
            indoor_cbrs_coverage_with_date("4", SignalLevel::High, date(1970, 1, 4)),
            indoor_cbrs_coverage_with_date("5", SignalLevel::High, date(1975, 3, 3)),
            indoor_cbrs_coverage_with_date("6", SignalLevel::High, date(1970, 1, 3)),
            indoor_cbrs_coverage_with_date("7", SignalLevel::High, date(1974, 2, 2)),
            indoor_cbrs_coverage_with_date("8", SignalLevel::High, date(1970, 1, 2)),
            indoor_cbrs_coverage_with_date("9", SignalLevel::High, date(1976, 5, 2)),
            indoor_cbrs_coverage_with_date("10", SignalLevel::High, date(1970, 1, 1)),
        ]
        .into_iter()
        {
            insert_indoor_coverage_object(&mut indoor_coverage, cov_obj);
        }
        let ranked: HashMap<_, _> =
            into_indoor_coverage_map(indoor_coverage, &NoBoostedHexes, Utc::now())
                .map(|x| (x.cbsd_id.clone().unwrap(), x))
                .collect();
        assert_eq!(ranked.get("1").unwrap().rank, 9);
        assert_eq!(ranked.get("2").unwrap().rank, 5);
        assert_eq!(ranked.get("3").unwrap().rank, 10);
        assert_eq!(ranked.get("4").unwrap().rank, 4);
        assert_eq!(ranked.get("5").unwrap().rank, 7);
        assert_eq!(ranked.get("6").unwrap().rank, 3);
        assert_eq!(ranked.get("7").unwrap().rank, 6);
        assert_eq!(ranked.get("8").unwrap().rank, 2);
        assert_eq!(ranked.get("9").unwrap().rank, 8);
        assert_eq!(ranked.get("10").unwrap().rank, 1);
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

    #[test]
    fn single_radio() {
        let mut indoor_coverage = IndoorCellTree::default();

        insert_indoor_coverage_object(
            &mut indoor_coverage,
            indoor_cbrs_coverage_with_loc(
                "1",
                Cell::from_raw(0x8c2681a3064d9ff).unwrap(),
                date(2022, 2, 2),
            ),
        );
        insert_indoor_coverage_object(
            &mut indoor_coverage,
            indoor_cbrs_coverage_with_loc(
                "1",
                Cell::from_raw(0x8c2681a3064dbff).unwrap(),
                date(2022, 2, 2),
            ),
        );

        let coverage = into_indoor_coverage_map(indoor_coverage, &NoBoostedHexes, Utc::now())
            .collect::<Vec<_>>();
        // Both coverages should be ranked 1
        assert_eq!(coverage[0].rank, 1);
        assert_eq!(coverage[1].rank, 1);
    }

    fn indoor_cbrs_coverage(cbsd_id: &str, signal_level: SignalLevel) -> CoverageObject {
        let owner: PublicKeyBinary = "112NqN2WWMwtK29PMzRby62fDydBJfsCLkCAf392stdok48ovNT6"
            .parse()
            .expect("failed owner parse");
        CoverageObject {
            indoor: true,
            hotspot_key: owner,
            seniority_timestamp: Utc::now(),
            cbsd_id: Some(cbsd_id.to_string()),
            coverage: vec![UnrankedCoverage {
                location: Cell::from_raw(0x8a1fb46622dffff).expect("valid h3 cell"),
                signal_power: 0,
                signal_level,
                assignments: hex_assignments_mock(),
            }],
        }
    }

    fn indoor_cbrs_coverage_with_date(
        cbsd_id: &str,
        signal_level: SignalLevel,
        seniority_timestamp: DateTime<Utc>,
    ) -> CoverageObject {
        let owner: PublicKeyBinary = "112NqN2WWMwtK29PMzRby62fDydBJfsCLkCAf392stdok48ovNT6"
            .parse()
            .expect("failed owner parse");
        CoverageObject {
            indoor: true,
            hotspot_key: owner,
            seniority_timestamp,
            cbsd_id: Some(cbsd_id.to_string()),
            coverage: vec![UnrankedCoverage {
                location: Cell::from_raw(0x8a1fb46622dffff).expect("valid h3 cell"),
                signal_power: 0,
                signal_level,
                assignments: hex_assignments_mock(),
            }],
        }
    }

    fn indoor_cbrs_coverage_with_loc(
        cbsd_id: &str,
        location: Cell,
        seniority_timestamp: DateTime<Utc>,
    ) -> CoverageObject {
        let owner: PublicKeyBinary = "112NqN2WWMwtK29PMzRby62fDydBJfsCLkCAf392stdok48ovNT6"
            .parse()
            .expect("failed owner parse");
        CoverageObject {
            indoor: true,
            hotspot_key: owner,
            seniority_timestamp,
            cbsd_id: Some(cbsd_id.to_string()),
            coverage: vec![UnrankedCoverage {
                location,
                signal_power: 0,
                signal_level: SignalLevel::High,
                assignments: hex_assignments_mock(),
            }],
        }
    }
}
