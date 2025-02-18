use std::num::NonZeroU32;

use chrono::Utc;
use coverage_map::{BoostedHexMap, RankedCoverage, SignalLevel, UnrankedCoverage};
use coverage_point_calculator::{
    BytesPs, CoveragePoints, LocationTrust, OracleBoostingStatus, RadioType, Result,
    SPBoostedRewardEligibility, Speedtest, SpeedtestTier,
};
use hex_assignments::{assignment::HexAssignments, Assignment};
use rust_decimal::{prelude::FromPrimitive, Decimal};
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
            service_provider_override: Assignment::C,
        },
        boosted: NonZeroU32::new(0),
    }];

    for (radio_type, expected_base_coverage_point) in [
        (RadioType::IndoorWifi, dec!(400)),
        (RadioType::OutdoorWifi, dec!(16)),
    ] {
        let coverage_points = CoveragePoints::new(
            radio_type,
            SPBoostedRewardEligibility::Eligible,
            speedtests.clone(),
            location_trust_scores.clone(),
            hexes.clone(),
            OracleBoostingStatus::Eligible,
        )
        .unwrap();

        assert_eq!(
            expected_base_coverage_point,
            coverage_points.coverage_points_v1()
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
            service_provider_override: Assignment::C,
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

    for (radio_type, num_hexes) in [(RadioType::IndoorWifi, 1), (RadioType::OutdoorWifi, 25)] {
        let coverage_points = CoveragePoints::new(
            radio_type,
            SPBoostedRewardEligibility::Eligible,
            default_speedtests.clone(),
            default_location_trust_scores.clone(),
            base_hex_iter.clone().take(num_hexes).collect(),
            OracleBoostingStatus::Eligible,
        )
        .unwrap();

        assert_eq!(dec!(400), coverage_points.coverage_points_v1());
    }
}

#[test]
fn outdoor_wifi_with_mixed_signal_level_coverage() -> Result {
    // https://github.com/helium/HIP/blob/main/0093-addition-of-wifi-aps-to-mobile-subdao.md#342-outdoor-access-points-rewards
    let coverage_points = outdoor_wifi_radio(
        SpeedtestTier::Good,
        &[
            top_ranked_coverage(0x8c2681a3064d9ff, SignalLevel::High), // 16
            top_ranked_coverage(0x8c2681a3065d3ff, SignalLevel::Medium), // 8
            top_ranked_coverage(0x8c2681a306635ff, SignalLevel::Medium), // 8
            top_ranked_coverage(0x8c2681a3066e7ff, SignalLevel::Low),  // 4
            top_ranked_coverage(0x8c2681a3065adff, SignalLevel::Low),  // 4
        ],
    )?;

    assert_eq!(dec!(40), coverage_points.total_shares());

    Ok(())
}

#[test]
fn outdoor_wifi_with_partially_overlapping_coverage_and_differing_speedtests() -> Result {
    // https://github.com/helium/HIP/blob/main/0105-modification-of-mobile-subdao-hex-limits.md#outdoor-wi-fi
    // Two radios, with a single overlapping hex and differing speedtest scores.
    let radio_1 = outdoor_wifi_radio(
        SpeedtestTier::Degraded, // multiplier 0.5 on all hexes
        &[
            top_ranked_coverage(0x8c2681a3064d9ff, SignalLevel::High), // 16
            // This hex is shared (0.5 multiplier)
            second_ranked_coverage(0x8c2681a3065d3ff, SignalLevel::Medium), // 8 * 0.5 = 4
            top_ranked_coverage(0x8c2681a306635ff, SignalLevel::Medium),    // 8
            top_ranked_coverage(0x8c2681a3066e7ff, SignalLevel::Low),       // 4
            top_ranked_coverage(0x8c2681a3065d7ff, SignalLevel::Low),       // 4
        ],
    )?;

    let radio_2 = outdoor_wifi_radio(
        SpeedtestTier::Good,
        &[
            top_ranked_coverage(0x8c2681a30641dff, SignalLevel::High), // 16
            top_ranked_coverage(0x8c2681a3065d3ff, SignalLevel::Medium), // 8
            top_ranked_coverage(0x8c2681a3066a9ff, SignalLevel::Medium), // 8
            top_ranked_coverage(0x8c2681a306481ff, SignalLevel::Low),  // 4
            top_ranked_coverage(0x8c2681a302991ff, SignalLevel::Low),  // 4
        ],
    )?;

    assert_eq!(dec!(18), radio_1.total_shares());
    assert_eq!(dec!(40), radio_2.total_shares());

    Ok(())
}

#[test]
fn outdoor_wifi_with_wholly_overlapping_coverage_and_differing_speedtests() -> Result {
    // https://github.com/helium/HIP/blob/main/0105-modification-of-mobile-subdao-hex-limits.md#modeling-wi-fi-ap-ranking
    // All radios cover the same hexes.
    // Seniority timestamps determine rank.
    // The first three ranked radio (radio_4) should receive rewards.

    let mut coverage_map_builder = coverage_map::CoverageMapBuilder::default();
    let mut insert_coverage = |hotspot_key: Vec<u8>, timestamp: &str| {
        coverage_map_builder.insert_coverage_object(coverage_map::CoverageObject {
            indoor: true,
            hotspot_key,
            cbsd_id: None,
            seniority_timestamp: timestamp.parse().expect("valid timestamp"),
            coverage: vec![
                unranked_coverage(0x8c2681a3064d9ff, SignalLevel::High), // 16
                unranked_coverage(0x8c2681a3065d3ff, SignalLevel::High), // 16
                unranked_coverage(0x8c2681a306635ff, SignalLevel::Medium), // 8
                unranked_coverage(0x8c2681a3066e7ff, SignalLevel::Low),  // 4
            ],
        })
    };

    insert_coverage(vec![1], "2022-02-01 00:00:00.000000000 UTC");
    insert_coverage(vec![2], "2022-02-02 00:00:00.000000000 UTC");
    insert_coverage(vec![3], "2022-02-03 00:00:00.000000000 UTC");
    insert_coverage(vec![4], "2022-02-04 00:00:00.000000000 UTC");
    insert_coverage(vec![5], "2022-02-05 00:00:00.000000000 UTC");
    insert_coverage(vec![6], "2022-02-06 00:00:00.000000000 UTC");

    let map = coverage_map_builder.build(&NoBoostedHexes, Utc::now());

    let radio_1 = outdoor_wifi_radio(SpeedtestTier::Good, map.get_wifi_coverage(&[1]))?;
    let radio_2 = outdoor_wifi_radio(SpeedtestTier::Acceptable, map.get_wifi_coverage(&[2]))?;
    let radio_3 = outdoor_wifi_radio(SpeedtestTier::Degraded, map.get_wifi_coverage(&[3]))?;
    let radio_4 = outdoor_wifi_radio(SpeedtestTier::Good, map.get_wifi_coverage(&[4]))?;
    let radio_5 = outdoor_wifi_radio(SpeedtestTier::Fail, map.get_wifi_coverage(&[5]))?;
    let radio_6 = outdoor_wifi_radio(SpeedtestTier::Good, map.get_wifi_coverage(&[6]))?;

    assert_eq!(dec!(44), radio_1.total_shares());
    assert_eq!(dec!(16.5), radio_2.total_shares());
    assert_eq!(dec!(5.5), radio_3.total_shares());
    assert_eq!(dec!(0), radio_4.total_shares());
    assert_eq!(dec!(0), radio_5.total_shares());
    assert_eq!(dec!(0), radio_6.total_shares());

    Ok(())
}

#[test]
fn wifi_outdoor_with_mixed_signal_level_coverage() -> Result {
    let radio = CoveragePoints::new(
        RadioType::OutdoorWifi,
        SPBoostedRewardEligibility::Eligible,
        Speedtest::mock(SpeedtestTier::Good),
        vec![LocationTrust {
            meters_to_asserted: 1,
            trust_score: Decimal::from_u8(1).unwrap(),
        }],
        vec![
            top_ranked_coverage(0x8c2681a3064d9ff, SignalLevel::High), // 16
            top_ranked_coverage(0x8c2681a3065d3ff, SignalLevel::High), // 16
            top_ranked_coverage(0x8c2681a306635ff, SignalLevel::Medium), // 8
            top_ranked_coverage(0x8c2681a3066e7ff, SignalLevel::Medium), // 8
            top_ranked_coverage(0x8c2681a3065adff, SignalLevel::Medium), // 8
            top_ranked_coverage(0x8c2681a339a4bff, SignalLevel::Low),  // 4
            top_ranked_coverage(0x8c2681a3065d7ff, SignalLevel::Low),  // 4
        ],
        OracleBoostingStatus::Eligible,
    )
    .unwrap();

    assert_eq!(
        Decimal::from_i32(16 * 2 + 8 * 3 + 4 * 2).unwrap(),
        radio.total_shares()
    );

    Ok(())
}

#[test]
fn wifi_outdoor_with_single_overlapping_coverage() -> Result {
    // Scenario Five
    // 2 radios overlapping a single hex with a medium Signal Level.
    // First radio has older seniority and degradet speedtest

    let mut coverage_map_builder = coverage_map::CoverageMapBuilder::default();

    coverage_map_builder.insert_coverage_object(coverage_map::CoverageObject {
        indoor: false,
        hotspot_key: vec![1],
        cbsd_id: None,
        seniority_timestamp: "2022-02-01 00:00:00.000000000 UTC"
            .parse()
            .expect("valid timestamp"),
        coverage: vec![
            unranked_coverage(0x8c2681a302991ff, SignalLevel::High), // 16
            unranked_coverage(0x8c2681a3028a7ff, SignalLevel::Medium), // 8. This hex is shared
            unranked_coverage(0x8c2681a30659dff, SignalLevel::Low),  // 4
        ],
    });
    coverage_map_builder.insert_coverage_object(coverage_map::CoverageObject {
        indoor: false,
        hotspot_key: vec![2],
        cbsd_id: None,
        seniority_timestamp: "2022-02-02 00:00:01.000000000 UTC"
            .parse()
            .expect("valid timestamp"),
        coverage: vec![
            unranked_coverage(0x8c2681a3066abff, SignalLevel::High), // 16
            unranked_coverage(0x8c2681a3028a7ff, SignalLevel::Medium), // 8 * 0.5 = 4(This hex is shared)
            unranked_coverage(0x8c2681a3066a9ff, SignalLevel::Low),    // 4
        ],
    });

    let map = coverage_map_builder.build(&NoBoostedHexes, Utc::now());

    let radio_1 = outdoor_wifi_radio(SpeedtestTier::Degraded, map.get_wifi_coverage(&[1]))?;
    let radio_2 = outdoor_wifi_radio(SpeedtestTier::Good, map.get_wifi_coverage(&[2]))?;

    assert_eq!(dec!(28) * dec!(0.50), radio_1.total_shares());
    assert_eq!(dec!(24), radio_2.total_shares());

    Ok(())
}

#[test]
fn widi_indoor_with_wholly_overlapping_coverage_and_no_failing_speedtests() -> Result {
    let mut coverage_map_builder = coverage_map::CoverageMapBuilder::default();
    let mut insert_coverage = |hotspot_key: Vec<u8>, timestamp: &str| {
        coverage_map_builder.insert_coverage_object(coverage_map::CoverageObject {
            indoor: true,
            hotspot_key,
            cbsd_id: None,
            seniority_timestamp: timestamp.parse().expect("valid timestamp"),
            coverage: vec![
                unranked_coverage(0x8c2681a3064d9ff, SignalLevel::High), // 16
            ],
        })
    };

    insert_coverage(vec![1], "2022-02-01 00:00:00.000000000 UTC");
    insert_coverage(vec![2], "2022-02-02 00:00:00.000000000 UTC");
    insert_coverage(vec![3], "2022-02-03 00:00:00.000000000 UTC");
    insert_coverage(vec![4], "2022-02-04 00:00:00.000000000 UTC");
    insert_coverage(vec![5], "2022-02-05 00:00:00.000000000 UTC");
    insert_coverage(vec![6], "2022-02-06 00:00:00.000000000 UTC");
    let map = coverage_map_builder.build(&NoBoostedHexes, Utc::now());

    let radio_1 = indoor_wifi_radio(SpeedtestTier::Good, map.get_wifi_coverage(&[1]))?;
    let radio_2 = indoor_wifi_radio(SpeedtestTier::Good, map.get_wifi_coverage(&[2]))?;
    let radio_3 = indoor_wifi_radio(SpeedtestTier::Good, map.get_wifi_coverage(&[3]))?;
    let radio_4 = indoor_wifi_radio(SpeedtestTier::Good, map.get_wifi_coverage(&[4]))?;
    let radio_5 = indoor_wifi_radio(SpeedtestTier::Good, map.get_wifi_coverage(&[5]))?;
    let radio_6 = indoor_wifi_radio(SpeedtestTier::Good, map.get_wifi_coverage(&[6]))?;

    assert_eq!(dec!(400), radio_1.total_shares());
    assert_eq!(dec!(0), radio_2.total_shares());
    assert_eq!(dec!(0), radio_3.total_shares());
    assert_eq!(dec!(0), radio_4.total_shares());
    assert_eq!(dec!(0), radio_5.total_shares());
    assert_eq!(dec!(0), radio_6.total_shares());

    Ok(())
}

fn indoor_wifi_radio(
    speedtest_tier: SpeedtestTier,
    coverage: &[RankedCoverage],
) -> Result<CoveragePoints> {
    CoveragePoints::new(
        RadioType::IndoorWifi,
        SPBoostedRewardEligibility::Eligible,
        Speedtest::mock(speedtest_tier),
        vec![LocationTrust {
            meters_to_asserted: 1,
            trust_score: Decimal::from_u8(1).unwrap(),
        }],
        coverage.to_owned(),
        OracleBoostingStatus::Eligible,
    )
}

fn outdoor_wifi_radio(
    speedtest_tier: SpeedtestTier,
    coverage: &[RankedCoverage],
) -> Result<CoveragePoints> {
    CoveragePoints::new(
        RadioType::OutdoorWifi,
        SPBoostedRewardEligibility::Eligible,
        Speedtest::mock(speedtest_tier),
        vec![LocationTrust {
            meters_to_asserted: 1,
            trust_score: Decimal::from_u8(1).unwrap(),
        }],
        coverage.to_owned(),
        OracleBoostingStatus::Eligible,
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
            service_provider_override: Assignment::C,
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
            service_provider_override: Assignment::C,
        },
    }
}
