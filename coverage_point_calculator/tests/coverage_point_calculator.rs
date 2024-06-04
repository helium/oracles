use std::{collections::HashMap, num::NonZeroU32};

use coverage_map::SignalLevel;
use coverage_point_calculator::{
    calculate_coverage_points,
    location::{LocationTrust, Meters},
    speedtest::{BytesPs, Millis, Speedtest},
    CoverageMapExt, CoveredHex, RadioThreshold, RadioType, Rank, RewardableRadio,
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
        },
        Speedtest {
            upload_speed: BytesPs::mbps(15),
            download_speed: BytesPs::mbps(150),
            latency: Millis::new(15),
        },
    ];
    let location_trust_scores = vec![LocationTrust {
        distance_to_asserted: Meters::new(1),
        trust_score: dec!(1.0),
    }];

    struct TestCoverageMap;

    impl CoverageMapExt<()> for TestCoverageMap {
        fn hexes(&self, _radio: &()) -> Vec<CoveredHex> {
            vec![CoveredHex {
                cell: hextree::Cell::from_raw(0x8c2681a3064edff).unwrap(),
                rank: Rank::new(1).unwrap(),
                signal_level: SignalLevel::High,
                assignments: HexAssignments {
                    footfall: Assignment::A,
                    landtype: Assignment::A,
                    urbanized: Assignment::A,
                },
                boosted: NonZeroU32::new(0),
            }]
        }
    }

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
            TestCoverageMap.hexes(&()),
        );
        radios.push(radio.clone());
        println!(
            "{radio_type:?} \t--> {}",
            calculate_coverage_points(radio).coverage_points
        );
    }

    let output = radios
        .into_iter()
        .map(|r| (r.radio_type, calculate_coverage_points(r).coverage_points))
        .collect::<Vec<_>>();
    println!("{output:#?}");
}

#[test]
fn radio_unique_coverage() {
    // all radios will receive 400 coverage points
    let base_hex = CoveredHex {
        cell: hextree::Cell::from_raw(0x8c2681a3064edff).unwrap(),
        rank: Rank::new(1).unwrap(),
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

    struct TestCoverageMap<'a>(HashMap<&'a str, Vec<CoveredHex>>);
    let coverage_map = TestCoverageMap(map);

    impl CoverageMapExt<RadioType> for TestCoverageMap<'_> {
        fn hexes(&self, key: &RadioType) -> Vec<CoveredHex> {
            let key = match key {
                RadioType::IndoorWifi => "indoor_wifi",
                RadioType::OutdoorWifi => "outdoor_wifi",
                RadioType::IndoorCbrs => "indoor_cbrs",
                RadioType::OutdoorCbrs => "outdoor_cbrs",
            };
            self.0.get(key).unwrap().clone()
        }
    }

    let default_speedtests = vec![
        Speedtest {
            upload_speed: BytesPs::mbps(15),
            download_speed: BytesPs::mbps(150),
            latency: Millis::new(15),
        },
        Speedtest {
            upload_speed: BytesPs::mbps(15),
            download_speed: BytesPs::mbps(150),
            latency: Millis::new(15),
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
        radios.push(RewardableRadio::new(
            radio_type,
            default_speedtests.clone(),
            default_location_trust_scores.clone(),
            RadioThreshold::Verified,
            coverage_map.hexes(&radio_type),
        ));
    }

    let coverage_points = radios
        .into_iter()
        .map(|r| (r.radio_type, calculate_coverage_points(r).coverage_points))
        .collect::<Vec<_>>();

    // let coverage_points = make_rewardable_radios(&radios, &coverage_map)
    //     .map(|r| (r.radio_type, calculate_coverage_points(r).coverage_points))
    //     .collect::<Vec<_>>();
    println!("{coverage_points:#?}")
}
