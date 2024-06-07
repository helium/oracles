use std::num::NonZeroU32;

use chrono::Utc;
use coverage_map::{RankedCoverage, SignalLevel};
use coverage_point_calculator::{
    calculate_coverage_points,
    location::LocationTrust,
    make_rewardable_radio,
    speedtest::{BytesPs, Speedtest},
    RadioThreshold, RadioType,
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
        let radio = make_rewardable_radio(
            radio_type,
            RadioThreshold::Verified,
            speedtests.clone(),
            location_trust_scores.clone(),
            hexes.clone(),
        )
        .unwrap();

        let coverage_points = calculate_coverage_points(&radio);
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
        let radio = make_rewardable_radio(
            radio_type,
            RadioThreshold::Verified,
            default_speedtests.clone(),
            default_location_trust_scores.clone(),
            base_hex_iter.clone().take(num_hexes).collect(),
        )
        .unwrap();

        let coverage_points = calculate_coverage_points(&radio);
        assert_eq!(dec!(400), coverage_points.total_coverage_points);
    }
}
