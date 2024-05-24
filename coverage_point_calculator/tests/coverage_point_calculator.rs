use std::{collections::HashMap, num::NonZeroU32};

use coverage_point_calculator::{
    calculate_coverage_points, make_rewardable_radio, make_rewardable_radios, Assignment,
    Assignments, CoverageMap, CoveredHex, MaxOneMultplier, Radio, RadioType, Rank, SignalLevel,
    Speedtest,
};
use rust_decimal_macros::dec;

#[test]
fn base_radio_coverage_points() {
    struct TestRadio(RadioType);
    struct TestCoverageMap;

    impl Radio for TestRadio {
        fn radio_type(&self) -> RadioType {
            self.0
        }

        fn speedtest(&self) -> Speedtest {
            Speedtest::Good
        }

        fn location_trust_scores(&self) -> Vec<MaxOneMultplier> {
            vec![dec!(1)]
        }

        fn verified_radio_threshold(&self) -> bool {
            true
        }
    }

    impl CoverageMap for TestCoverageMap {
        fn hexes(&self, _radio: &impl Radio) -> Vec<CoveredHex> {
            vec![CoveredHex {
                rank: Rank::new(1).unwrap(),
                signal_level: SignalLevel::High,
                assignments: Assignments {
                    footfall: Assignment::A,
                    landtype: Assignment::A,
                    urbanized: Assignment::A,
                },
                boosted: NonZeroU32::new(0),
            }]
        }
    }

    for radio_type in [
        RadioType::IndoorWifi,
        RadioType::IndoorCbrs,
        RadioType::OutdoorWifi,
        RadioType::OutdoorCbrs,
    ] {
        let radio = make_rewardable_radio(&TestRadio(radio_type), &TestCoverageMap);
        println!(
            "{radio_type:?} \t--> {}",
            calculate_coverage_points(radio).coverage_points
        );
    }

    let radios = vec![
        TestRadio(RadioType::IndoorWifi),
        TestRadio(RadioType::IndoorCbrs),
        TestRadio(RadioType::OutdoorWifi),
        TestRadio(RadioType::OutdoorCbrs),
    ];
    let output = make_rewardable_radios(&radios, &TestCoverageMap)
        .map(|r| (r.radio_type, calculate_coverage_points(r).coverage_points))
        .collect::<Vec<_>>();
    println!("{output:#?}");
}

#[test]
fn radio_unique_coverage() {
    struct TestRadio(RadioType);

    let radios = vec![
        TestRadio(RadioType::IndoorWifi),
        TestRadio(RadioType::IndoorCbrs),
        TestRadio(RadioType::OutdoorWifi),
        TestRadio(RadioType::OutdoorCbrs),
    ];

    impl Radio for TestRadio {
        fn radio_type(&self) -> RadioType {
            self.0
        }

        fn speedtest(&self) -> Speedtest {
            Speedtest::Good
        }

        fn location_trust_scores(&self) -> Vec<MaxOneMultplier> {
            vec![dec!(1)]
        }

        fn verified_radio_threshold(&self) -> bool {
            true
        }
    }

    // all radios will receive 400 coverage points
    let base_hex = CoveredHex {
        rank: Rank::new(1).unwrap(),
        signal_level: SignalLevel::High,
        assignments: Assignments {
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

    impl CoverageMap for TestCoverageMap<'_> {
        fn hexes(&self, radio: &impl Radio) -> Vec<CoveredHex> {
            let key = match radio.radio_type() {
                RadioType::IndoorWifi => "indoor_wifi",
                RadioType::OutdoorWifi => "outdoor_wifi",
                RadioType::IndoorCbrs => "indoor_cbrs",
                RadioType::OutdoorCbrs => "outdoor_cbrs",
            };
            self.0.get(key).unwrap().clone()
        }
    }

    let coverage_points = make_rewardable_radios(&radios, &coverage_map)
        .map(|r| (r.radio_type, calculate_coverage_points(r).coverage_points))
        .collect::<Vec<_>>();
    println!("{coverage_points:#?}")
}
