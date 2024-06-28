use std::num::NonZeroU32;

use chrono::Utc;
use coverage_map::{BoostedHexMap, RankedCoverage, SignalLevel, UnrankedCoverage};
use coverage_point_calculator::{
    BytesPs, CoveragePoints, LocationTrust, RadioThreshold, RadioType, Result, Speedtest,
    SpeedtestTier,
};
use hex_assignments::{assignment::HexAssignments, Assignment};
use rust_decimal_macros::dec;

#[test]
fn base_radio_coverage_points() {
    let speedtests = vec![
        Speedtest {
            upload_speed: BytesPs::mbps(15),
            download_speed: BytesPs::mbps(150),
            latency_millis: 15,
            timestamp: Utc::now(),
        },
        Speedtest {
            upload_speed: BytesPs::mbps(15),
            download_speed: BytesPs::mbps(150),
            latency_millis: 15,
            timestamp: Utc::now(),
        },
    ];
    let location_trust_scores = vec![LocationTrust {
        meters_to_asserted: 1,
        trust_score: dec!(1.0),
    }];

    let hexes = vec![RankedCoverage {
        hotspot_key: vec![1],
        cbsd_id: None,
        hex: hextree::Cell::from_raw(0x8c2681a3064edff).unwrap(),
        rank: 1,
        signal_level: SignalLevel::High,
        assignments: HexAssignments {
            footfall: Assignment::A,
            landtype: Assignment::A,
            urbanized: Assignment::A,
        },
        boosted: NonZeroU32::new(0),
    }];

    for (radio_type, expcted_base_coverage_point) in [
        (RadioType::IndoorWifi, dec!(400)),
        (RadioType::IndoorCbrs, dec!(100)),
        (RadioType::OutdoorWifi, dec!(16)),
        (RadioType::OutdoorCbrs, dec!(4)),
    ] {
        let coverage_points = CoveragePoints::new(
            radio_type,
            RadioThreshold::Verified,
            speedtests.clone(),
            location_trust_scores.clone(),
            hexes.clone(),
        )
        .unwrap();

        assert_eq!(
            expcted_base_coverage_point,
            coverage_points.total_coverage_points
        );
    }
}

#[test]
fn radios_with_coverage() {
    // Enough hexes will be provided to each type of radio, that they are
    // awarded 400 coverage points.

    let base_hex = RankedCoverage {
        hotspot_key: vec![1],
        cbsd_id: None,
        hex: hextree::Cell::from_raw(0x8c2681a3064edff).unwrap(),
        rank: 1,
        signal_level: SignalLevel::High,
        assignments: HexAssignments {
            footfall: Assignment::A,
            landtype: Assignment::A,
            urbanized: Assignment::A,
        },
        boosted: NonZeroU32::new(0),
    };
    let base_hex_iter = std::iter::repeat(base_hex);

    let default_speedtests = vec![
        Speedtest {
            upload_speed: BytesPs::mbps(15),
            download_speed: BytesPs::mbps(150),
            latency_millis: 15,
            timestamp: Utc::now(),
        },
        Speedtest {
            upload_speed: BytesPs::mbps(15),
            download_speed: BytesPs::mbps(150),
            latency_millis: 15,
            timestamp: Utc::now(),
        },
    ];
    let default_location_trust_scores = vec![LocationTrust {
        meters_to_asserted: 1,
        trust_score: dec!(1.0),
    }];

    for (radio_type, num_hexes) in [
        (RadioType::IndoorWifi, 1),
        (RadioType::IndoorCbrs, 4),
        (RadioType::OutdoorWifi, 25),
        (RadioType::OutdoorCbrs, 100),
    ] {
        let coverage_points = CoveragePoints::new(
            radio_type,
            RadioThreshold::Verified,
            default_speedtests.clone(),
            default_location_trust_scores.clone(),
            base_hex_iter.clone().take(num_hexes).collect(),
        )
        .unwrap();

        assert_eq!(dec!(400), coverage_points.total_coverage_points);
    }
}

#[test]
fn cbrs_with_mixed_signal_level_coverage() -> Result {
    // Scenario One
    let coverage_points = indoor_cbrs_radio(
        SpeedtestTier::Good,
        &[
            top_ranked_coverage(0x8c2681a3064d9ff, SignalLevel::High),
            top_ranked_coverage(0x8c2681a3065d3ff, SignalLevel::Low),
            top_ranked_coverage(0x8c2681a306635ff, SignalLevel::Low),
            top_ranked_coverage(0x8c2681a3066e7ff, SignalLevel::Low),
            top_ranked_coverage(0x8c2681a3065adff, SignalLevel::Low),
            top_ranked_coverage(0x8c2681a339a4bff, SignalLevel::Low),
            top_ranked_coverage(0x8c2681a3065d7ff, SignalLevel::Low),
        ],
    )?;

    assert_eq!(dec!(250), coverage_points.reward_shares);

    Ok(())
}

#[test]
fn cbrs_with_partially_overlapping_coverage_and_differing_speedtests() -> Result {
    // Scenario two
    // Two radios, with a single overlapping hex and differing speedtest scores.
    let radio_1 = indoor_cbrs_radio(
        SpeedtestTier::Degraded,
        &[
            top_ranked_coverage(0x8c2681a3064d9ff, SignalLevel::High),
            second_ranked_coverage(0x8c2681a3065d3ff, SignalLevel::Low), // This hex is shared
            top_ranked_coverage(0x8c2681a306635ff, SignalLevel::Low),
            top_ranked_coverage(0x8c2681a3066e7ff, SignalLevel::Low),
            top_ranked_coverage(0x8c2681a3065adff, SignalLevel::Low),
            top_ranked_coverage(0x8c2681a339a4bff, SignalLevel::Low),
            top_ranked_coverage(0x8c2681a3065d7ff, SignalLevel::Low),
        ],
    )?;

    let radio_2 = indoor_cbrs_radio(
        SpeedtestTier::Good,
        &[
            top_ranked_coverage(0x8c2681a30641dff, SignalLevel::High),
            top_ranked_coverage(0x8c2681a3065d3ff, SignalLevel::Low), // This hex is shared
            top_ranked_coverage(0x8c2681a3066a9ff, SignalLevel::Low),
            top_ranked_coverage(0x8c2681a306607ff, SignalLevel::Low),
            top_ranked_coverage(0x8c2681a3066e9ff, SignalLevel::Low),
            top_ranked_coverage(0x8c2681a306481ff, SignalLevel::Low),
            top_ranked_coverage(0x8c2681a302991ff, SignalLevel::Low),
        ],
    )?;

    assert_eq!(dec!(112.5), radio_1.reward_shares);
    assert_eq!(dec!(250), radio_2.reward_shares);

    Ok(())
}

#[test]
fn cbrs_with_wholly_overlapping_coverage_and_differing_speedtests() -> Result {
    // Scenario Three
    // All radios cover the same hexes.
    // Seniority timestamps determine rank.
    // Only the first ranked radio (radio_4) should receive rewards.

    let mut coverage_map_builder = coverage_map::CoverageMapBuilder::default();
    let mut insert_coverage = |cbsd_id: &str, timestamp: &str| {
        coverage_map_builder.insert_coverage_object(coverage_map::CoverageObject {
            indoor: true,
            hotspot_key: vec![],
            cbsd_id: Some(cbsd_id.to_string()),
            seniority_timestamp: timestamp.parse().expect("valid timestamp"),
            coverage: vec![
                unranked_coverage(0x8c2681a3064d9ff, SignalLevel::High),
                unranked_coverage(0x8c2681a3065d3ff, SignalLevel::Low),
                unranked_coverage(0x8c2681a306635ff, SignalLevel::Low),
                unranked_coverage(0x8c2681a3066e7ff, SignalLevel::Low),
                unranked_coverage(0x8c2681a3065adff, SignalLevel::Low),
                unranked_coverage(0x8c2681a339a4bff, SignalLevel::Low),
                unranked_coverage(0x8c2681a3065d7ff, SignalLevel::Low),
            ],
        })
    };

    insert_coverage("serial-1", "2022-02-01 00:00:00.000000000 UTC");
    insert_coverage("serial-2", "2022-02-01 00:00:00.000000000 UTC");
    insert_coverage("serial-3", "2022-02-01 00:00:00.000000000 UTC");
    insert_coverage("serial-4", "2022-01-31 00:00:00.000000000 UTC"); // earliest
    insert_coverage("serial-5", "2022-02-01 00:00:00.000000000 UTC");
    insert_coverage("serial-6", "2022-02-02 00:00:00.000000000 UTC"); // latest

    let map = coverage_map_builder.build(&NoBoostedHexes, Utc::now());

    let radio_1 = indoor_cbrs_radio(SpeedtestTier::Poor, map.get_cbrs_coverage("serial-1"))?;
    let radio_2 = indoor_cbrs_radio(SpeedtestTier::Poor, map.get_cbrs_coverage("serial-2"))?;
    let radio_3 = indoor_cbrs_radio(SpeedtestTier::Good, map.get_cbrs_coverage("serial-3"))?;
    let radio_4 = indoor_cbrs_radio(SpeedtestTier::Good, map.get_cbrs_coverage("serial-4"))?;
    let radio_5 = indoor_cbrs_radio(SpeedtestTier::Fail, map.get_cbrs_coverage("serial-5"))?;
    let radio_6 = indoor_cbrs_radio(SpeedtestTier::Good, map.get_cbrs_coverage("serial-6"))?;

    assert_eq!(dec!(0), radio_1.reward_shares);
    assert_eq!(dec!(0), radio_2.reward_shares);
    assert_eq!(dec!(0), radio_3.reward_shares);
    assert_eq!(dec!(250), radio_4.reward_shares);
    assert_eq!(dec!(0), radio_5.reward_shares);
    assert_eq!(dec!(0), radio_6.reward_shares);

    Ok(())
}

#[test]
fn cbrs_outdoor_with_mixed_signal_level_coverage() -> Result {
    // Scenario four
    // Outdoor Cbrs with mixed signal level coverage

    let radio = CoveragePoints::new(
        RadioType::OutdoorCbrs,
        RadioThreshold::Verified,
        Speedtest::mock(SpeedtestTier::Good),
        vec![], // Location Trust is ignored for Cbrs
        vec![
            top_ranked_coverage(0x8c2681a3064d9ff, SignalLevel::High),
            top_ranked_coverage(0x8c2681a3065d3ff, SignalLevel::High),
            top_ranked_coverage(0x8c2681a306635ff, SignalLevel::Medium),
            top_ranked_coverage(0x8c2681a3066e7ff, SignalLevel::Medium),
            top_ranked_coverage(0x8c2681a3065adff, SignalLevel::Medium),
            top_ranked_coverage(0x8c2681a339a4bff, SignalLevel::Low),
            top_ranked_coverage(0x8c2681a3065d7ff, SignalLevel::Low),
            top_ranked_coverage(0x8c2681a306481ff, SignalLevel::Low),
            top_ranked_coverage(0x8c2681a30648bff, SignalLevel::Low),
            top_ranked_coverage(0x8c2681a30646bff, SignalLevel::Low),
        ],
    )
    .unwrap();

    assert_eq!(dec!(19), radio.reward_shares);

    Ok(())
}

#[test]
fn cbrs_outdoor_with_single_overlapping_coverage() -> Result {
    // Scenario Five
    // 2 radios overlapping a single hex with a medium Signal Level.
    // First radio has seniority.

    let mut coverage_map_builder = coverage_map::CoverageMapBuilder::default();

    coverage_map_builder.insert_coverage_object(coverage_map::CoverageObject {
        indoor: false,
        hotspot_key: vec![1],
        cbsd_id: Some("serial-1".to_string()),
        seniority_timestamp: "2022-02-01 00:00:00.000000000 UTC"
            .parse()
            .expect("valid timestamp"),
        coverage: vec![
            unranked_coverage(0x8c2681a302991ff, SignalLevel::High),
            unranked_coverage(0x8c2681a306601ff, SignalLevel::High),
            unranked_coverage(0x8c2681a306697ff, SignalLevel::High),
            unranked_coverage(0x8c2681a3028a7ff, SignalLevel::Medium), // This hex is shared
            unranked_coverage(0x8c2681a3064c1ff, SignalLevel::Medium),
            unranked_coverage(0x8c2681a30671bff, SignalLevel::Low),
            unranked_coverage(0x8c2681a306493ff, SignalLevel::Low),
            unranked_coverage(0x8c2681a30659dff, SignalLevel::Low),
        ],
    });
    coverage_map_builder.insert_coverage_object(coverage_map::CoverageObject {
        indoor: false,
        hotspot_key: vec![2],
        cbsd_id: Some("serial-2".to_string()),
        seniority_timestamp: "2022-02-01 00:00:01.000000000 UTC"
            .parse()
            .expect("valid timestamp"),
        coverage: vec![
            unranked_coverage(0x8c2681a3066abff, SignalLevel::High),
            unranked_coverage(0x8c2681a3028a7ff, SignalLevel::Medium), // This hex is shared
            unranked_coverage(0x8c2681a3066a9ff, SignalLevel::Low),
            unranked_coverage(0x8c2681a3066a5ff, SignalLevel::Low),
            unranked_coverage(0x8c2681a30640dff, SignalLevel::Low),
        ],
    });

    let map = coverage_map_builder.build(&NoBoostedHexes, Utc::now());

    let radio_1 = outdoor_cbrs_radio(SpeedtestTier::Degraded, map.get_cbrs_coverage("serial-1"))?;
    let radio_2 = outdoor_cbrs_radio(SpeedtestTier::Good, map.get_cbrs_coverage("serial-2"))?;

    assert_eq!(dec!(19) * dec!(0.5), radio_1.reward_shares);
    assert_eq!(dec!(8), radio_2.reward_shares);

    Ok(())
}

#[test]
fn cbrs_indoor_with_wholly_overlapping_coverage_and_no_failing_speedtests() -> Result {
    // Scenario Six
    // Similar to Scenario Three, but there are no failing speedtests.
    // Radios have the same coverage.

    let mut coverage_map_builder = coverage_map::CoverageMapBuilder::default();
    let mut insert_coverage = |cbsd_id: &str, timestamp: &str| {
        coverage_map_builder.insert_coverage_object(coverage_map::CoverageObject {
            indoor: true,
            hotspot_key: vec![0],
            cbsd_id: Some(cbsd_id.to_string()),
            seniority_timestamp: timestamp.parse().expect("valid timestamp"),
            coverage: vec![
                unranked_coverage(0x8c2681a3064d9ff, SignalLevel::High),
                unranked_coverage(0x8c2681a3065d3ff, SignalLevel::Low),
                unranked_coverage(0x8c2681a306635ff, SignalLevel::Low),
                unranked_coverage(0x8c2681a3066e7ff, SignalLevel::Low),
                unranked_coverage(0x8c2681a3065adff, SignalLevel::Low),
                unranked_coverage(0x8c2681a339a4bff, SignalLevel::Low),
                unranked_coverage(0x8c2681a3065d7ff, SignalLevel::Low),
            ],
        })
    };

    insert_coverage("serial-1", "2022-02-01 00:00:00.000000000 UTC");
    insert_coverage("serial-2", "2022-01-31 00:00:00.000000000 UTC"); // Oldest
    insert_coverage("serial-3", "2022-02-01 00:00:00.000000000 UTC");
    insert_coverage("serial-4", "2022-02-01 00:00:00.000000000 UTC");
    insert_coverage("serial-5", "2022-02-01 00:00:00.000000000 UTC");
    insert_coverage("serial-6", "2022-02-02 00:00:00.000000000 UTC"); // Newest
    let map = coverage_map_builder.build(&NoBoostedHexes, Utc::now());

    let radio_1 = indoor_cbrs_radio(SpeedtestTier::Poor, map.get_cbrs_coverage("serial-1"))?;
    let radio_2 = indoor_cbrs_radio(SpeedtestTier::Poor, map.get_cbrs_coverage("serial-2"))?;
    let radio_3 = indoor_cbrs_radio(SpeedtestTier::Good, map.get_cbrs_coverage("serial-3"))?;
    let radio_4 = indoor_cbrs_radio(SpeedtestTier::Good, map.get_cbrs_coverage("serial-4"))?;
    let radio_5 = indoor_cbrs_radio(SpeedtestTier::Good, map.get_cbrs_coverage("serial-5"))?;
    let radio_6 = indoor_cbrs_radio(SpeedtestTier::Good, map.get_cbrs_coverage("serial-6"))?;

    assert_eq!(dec!(0), radio_1.reward_shares);
    assert_eq!(dec!(62.5), radio_2.reward_shares);
    assert_eq!(dec!(0), radio_3.reward_shares);
    assert_eq!(dec!(0), radio_4.reward_shares);
    assert_eq!(dec!(0), radio_5.reward_shares);
    assert_eq!(dec!(0), radio_6.reward_shares);

    Ok(())
}

fn indoor_cbrs_radio(
    speedtest_tier: SpeedtestTier,
    coverage: &[RankedCoverage],
) -> Result<CoveragePoints> {
    CoveragePoints::new(
        RadioType::IndoorCbrs,
        RadioThreshold::Verified,
        Speedtest::mock(speedtest_tier),
        vec![],
        coverage.to_owned(),
    )
}

fn outdoor_cbrs_radio(
    speedtest_tier: SpeedtestTier,
    coverage: &[RankedCoverage],
) -> Result<CoveragePoints> {
    CoveragePoints::new(
        RadioType::OutdoorCbrs,
        RadioThreshold::Verified,
        Speedtest::mock(speedtest_tier),
        vec![],
        coverage.to_owned(),
    )
}

struct NoBoostedHexes;
impl BoostedHexMap for NoBoostedHexes {
    fn get_current_multiplier(
        &self,
        _cell: hextree::Cell,
        _ts: chrono::DateTime<Utc>,
    ) -> Option<NonZeroU32> {
        None
    }
}

fn top_ranked_coverage(hex: u64, signal_level: SignalLevel) -> RankedCoverage {
    ranked_coverage(hex, 1, signal_level)
}

fn second_ranked_coverage(hex: u64, signal_level: SignalLevel) -> RankedCoverage {
    ranked_coverage(hex, 2, signal_level)
}

fn ranked_coverage(hex: u64, rank: usize, signal_level: SignalLevel) -> RankedCoverage {
    RankedCoverage {
        hex: hextree::Cell::from_raw(hex).expect("valid h3 hex"),
        rank,
        hotspot_key: vec![1],
        cbsd_id: Some("serial".to_string()),
        assignments: HexAssignments {
            footfall: Assignment::A,
            landtype: Assignment::A,
            urbanized: Assignment::A,
        },
        boosted: None,
        signal_level,
    }
}

fn unranked_coverage(hex: u64, signal_level: SignalLevel) -> UnrankedCoverage {
    UnrankedCoverage {
        location: hextree::Cell::from_raw(hex).expect("valid h3 hex"),
        signal_power: 42,
        signal_level,
        assignments: HexAssignments {
            footfall: Assignment::A,
            landtype: Assignment::A,
            urbanized: Assignment::A,
        },
    }
}
