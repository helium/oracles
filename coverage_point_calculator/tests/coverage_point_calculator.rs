use std::{collections::HashMap, num::NonZeroU32, str::FromStr};

use chrono::Utc;
use coverage_map::{RankedCoverage, SignalLevel};
use coverage_point_calculator::{
    calculate_coverage_points,
    location::{LocationTrust, Meters},
    speedtest::{BytesPs, Millis, Speedtest},
    RadioThreshold, RadioType, RewardableRadio,
};
use hex_assignments::{assignment::HexAssignments, Assignment};
use rust_decimal_macros::dec;

#[test]
fn base_radio_coverage_points() {
    let speedtests = vec![
        Speedtest {
            upload_speed: BytesPs::mbps(15),
            download_speed: BytesPs::mbps(150),
            latency: Millis::new(15),
            timestamp: Utc::now(),
        },
        Speedtest {
            upload_speed: BytesPs::mbps(15),
            download_speed: BytesPs::mbps(150),
            latency: Millis::new(15),
            timestamp: Utc::now(),
        },
    ];
    let location_trust_scores = vec![LocationTrust {
        distance_to_asserted: Meters::new(1),
        trust_score: dec!(1.0),
    }];

    let pubkey = helium_crypto::PublicKeyBinary::from_str(
        "112NqN2WWMwtK29PMzRby62fDydBJfsCLkCAf392stdok48ovNT6",
    )
    .unwrap();

    let hexes = vec![RankedCoverage {
        hotspot_key: pubkey,
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

    let mut radios = vec![];
    for radio_type in [
        RadioType::IndoorWifi,
        RadioType::IndoorCbrs,
        RadioType::OutdoorWifi,
        RadioType::OutdoorCbrs,
    ] {
        let radio = RewardableRadio::new(
            radio_type,
            speedtests.clone(),
            location_trust_scores.clone(),
            RadioThreshold::Verified,
            hexes.clone(),
        )
        .unwrap();
        radios.push(radio.clone());
        println!(
            "{radio_type:?} \t--> {}",
            calculate_coverage_points(radio).total_coverage_points
        );
    }

    let output = radios
        .into_iter()
        .map(|r| {
            (
                r.radio_type(),
                calculate_coverage_points(r).total_coverage_points,
            )
        })
        .collect::<Vec<_>>();
    println!("{output:#?}");
}

#[test]
fn radio_unique_coverage() {
    let pubkey = helium_crypto::PublicKeyBinary::from_str(
        "112NqN2WWMwtK29PMzRby62fDydBJfsCLkCAf392stdok48ovNT6",
    )
    .unwrap();

    // all radios will receive 400 coverage points
    let base_hex = RankedCoverage {
        hotspot_key: pubkey,
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
    let hex = std::iter::repeat(base_hex);

    let mut map = HashMap::new();
    map.insert("indoor_wifi", hex.clone().take(1).collect());
    map.insert("indoor_cbrs", hex.clone().take(4).collect());
    map.insert("outdoor_wifi", hex.clone().take(25).collect());
    map.insert("outdoor_cbrs", hex.clone().take(100).collect());

    fn hexes(
        coverage_map: &HashMap<&str, Vec<RankedCoverage>>,
        key: &RadioType,
    ) -> Vec<RankedCoverage> {
        let key = match key {
            RadioType::IndoorWifi => "indoor_wifi",
            RadioType::OutdoorWifi => "outdoor_wifi",
            RadioType::IndoorCbrs => "indoor_cbrs",
            RadioType::OutdoorCbrs => "outdoor_cbrs",
        };
        coverage_map.get(key).unwrap().clone()
    }

    let default_speedtests = vec![
        Speedtest {
            upload_speed: BytesPs::mbps(15),
            download_speed: BytesPs::mbps(150),
            latency: Millis::new(15),
            timestamp: Utc::now(),
        },
        Speedtest {
            upload_speed: BytesPs::mbps(15),
            download_speed: BytesPs::mbps(150),
            latency: Millis::new(15),
            timestamp: Utc::now(),
        },
    ];
    let default_location_trust_scores = vec![LocationTrust {
        distance_to_asserted: Meters::new(1),
        trust_score: dec!(1.0),
    }];

    let mut radios = vec![];
    for radio_type in [
        RadioType::IndoorWifi,
        RadioType::IndoorCbrs,
        RadioType::OutdoorWifi,
        RadioType::OutdoorCbrs,
    ] {
        radios.push(
            RewardableRadio::new(
                radio_type,
                default_speedtests.clone(),
                default_location_trust_scores.clone(),
                RadioThreshold::Verified,
                hexes(&map, &radio_type),
            )
            .unwrap(),
        );
    }

    let coverage_points = radios
        .into_iter()
        .map(|r| {
            (
                r.radio_type(),
                calculate_coverage_points(r).total_coverage_points,
            )
        })
        .collect::<Vec<_>>();
    println!("{coverage_points:#?}")
}
