use anyhow::Context;
use chrono::{DateTime, Duration, Utc};
use file_store::{
    coverage::RadioHexSignalLevel,
    heartbeat::{CbrsHeartbeat, CbrsHeartbeatIngestReport},
    speedtest::CellSpeedtest,
};
use futures::stream::{self, StreamExt};
use h3o::CellIndex;
use helium_crypto::PublicKeyBinary;
use helium_proto::services::poc_mobile::{
    CoverageObjectValidity, OracleBoostingHexAssignment, SignalLevel,
};
use mobile_config::boosted_hex_info::BoostedHexes;
use mobile_verifier::{
    boosting_oracles::{set_oracle_boosting_assignments, Assignment, HexBoostData, UnassignedHex},
    coverage::{CoverageClaimTimeCache, CoverageObject, CoverageObjectCache, Seniority},
    geofence::GeofenceValidator,
    heartbeats::{Heartbeat, HeartbeatReward, LocationCache, SeniorityUpdate, ValidatedHeartbeat},
    radio_threshold::VerifiedRadioThresholds,
    reward_shares::CoveragePoints,
    speedtests::Speedtest,
    speedtests_average::{SpeedtestAverage, SpeedtestAverages},
    GatewayResolution, GatewayResolver,
};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use sqlx::PgPool;
use std::{collections::HashMap, pin::pin};
use uuid::Uuid;

#[derive(Clone)]
struct MockGeofence;

impl GeofenceValidator for MockGeofence {
    fn in_valid_region(&self, _heartbeat: &Heartbeat) -> bool {
        true
    }
}

#[derive(Copy, Clone)]
struct AllOwnersValid;

#[async_trait::async_trait]
impl GatewayResolver for AllOwnersValid {
    type Error = std::convert::Infallible;

    async fn resolve_gateway(
        &self,
        _address: &PublicKeyBinary,
    ) -> Result<GatewayResolution, Self::Error> {
        Ok(GatewayResolution::AssertedLocation(0x8c2681a3064d9ff))
    }
}

fn heartbeats<'a>(
    num: usize,
    start: DateTime<Utc>,
    hotspot_key: &'a PublicKeyBinary,
    cbsd_id: &'a str,
    lon: f64,
    lat: f64,
    coverage_object: Uuid,
) -> impl Iterator<Item = CbrsHeartbeatIngestReport> + 'a {
    (0..num).map(move |i| {
        let report = CbrsHeartbeat {
            pubkey: hotspot_key.clone(),
            lon,
            lat,
            operation_mode: true,
            cbsd_id: cbsd_id.to_string(),
            // Unused:
            hotspot_type: String::new(),
            cell_id: 0,
            timestamp: DateTime::<Utc>::MIN_UTC,
            cbsd_category: String::new(),
            coverage_object: Vec::from(coverage_object.into_bytes()),
        };
        CbrsHeartbeatIngestReport {
            report,
            received_timestamp: start + Duration::hours(i as i64),
        }
    })
}

fn bytes_per_s(mbps: u64) -> u64 {
    mbps * 125000
}

fn acceptable_speedtest(pubkey: PublicKeyBinary, timestamp: DateTime<Utc>) -> Speedtest {
    Speedtest {
        report: CellSpeedtest {
            pubkey,
            timestamp,
            upload_speed: bytes_per_s(10),
            download_speed: bytes_per_s(100),
            latency: 25,
            serial: "".to_string(),
        },
    }
}

fn signal_level(hex: &str, signal_level: SignalLevel) -> anyhow::Result<RadioHexSignalLevel> {
    Ok(RadioHexSignalLevel {
        location: hex.parse()?,
        signal_level,
        signal_power: 0, // Unused
    })
}

fn hex_cell(loc: &str) -> hextree::Cell {
    hextree::Cell::from_raw(u64::from_str_radix(loc, 16).unwrap()).unwrap()
}

#[sqlx::test]
async fn test_footfall_and_urbanization_report(pool: PgPool) -> anyhow::Result<()> {
    let uuid = Uuid::new_v4();
    let cbsd_id = "P27-SCE4255W120200039521XGB0102".to_string();

    fn new_hex_assingment(
        cell: &mut CellIndex,
        footfall: Assignment,
        landtype: Assignment,
        urbanized: Assignment,
        assignment_multiplier: u32,
    ) -> OracleBoostingHexAssignment {
        let loc = cell.to_string();
        *cell = cell.succ().unwrap();
        OracleBoostingHexAssignment {
            location: loc,
            assignment_multiplier,
            urbanized: urbanized.into(),
            footfall: footfall.into(),
            landtype: landtype.into(),
        }
    }

    let hexes = {
        // NOTE(mj): Cell is mutated in constructor to keep elements aligned for readability
        let mut cell = CellIndex::try_from(0x8c2681a3064d9ff)?;
        use Assignment::*;
        vec![
            // yellow - POI ≥ 1 Urbanized
            new_hex_assingment(&mut cell, A, A, A, 1000),
            new_hex_assingment(&mut cell, A, B, A, 1000),
            new_hex_assingment(&mut cell, A, C, A, 1000),
            // orange - POI ≥ 1 Not Urbanized
            new_hex_assingment(&mut cell, A, A, B, 1000),
            new_hex_assingment(&mut cell, A, B, B, 1000),
            new_hex_assingment(&mut cell, A, C, B, 1000),
            // light green - Point of Interest Urbanized
            new_hex_assingment(&mut cell, B, A, A, 700),
            new_hex_assingment(&mut cell, B, B, A, 700),
            new_hex_assingment(&mut cell, B, C, A, 700),
            // dark green - Point of Interest Not Urbanized
            new_hex_assingment(&mut cell, B, A, B, 500),
            new_hex_assingment(&mut cell, B, B, B, 500),
            new_hex_assingment(&mut cell, B, C, B, 500),
            // light blue - No POI Urbanized
            new_hex_assingment(&mut cell, C, A, A, 400),
            new_hex_assingment(&mut cell, C, B, A, 300),
            new_hex_assingment(&mut cell, C, C, A, 50),
            // dark blue - No POI Not Urbanized
            new_hex_assingment(&mut cell, C, A, B, 200),
            new_hex_assingment(&mut cell, C, B, B, 150),
            new_hex_assingment(&mut cell, C, C, B, 30),
            // gray - Outside of USA
            new_hex_assingment(&mut cell, A, A, C, 0),
            new_hex_assingment(&mut cell, A, B, C, 0),
            new_hex_assingment(&mut cell, A, C, C, 0),
            new_hex_assingment(&mut cell, B, A, C, 0),
            new_hex_assingment(&mut cell, B, B, C, 0),
            new_hex_assingment(&mut cell, B, C, C, 0),
            new_hex_assingment(&mut cell, C, A, C, 0),
            new_hex_assingment(&mut cell, C, B, C, 0),
            new_hex_assingment(&mut cell, C, C, C, 0),
        ]
    };

    let coverage_object = file_store::coverage::CoverageObject {
        pub_key: PublicKeyBinary::from(vec![1]),
        uuid,
        key_type: file_store::coverage::KeyType::CbsdId(cbsd_id.clone()),
        coverage_claim_time: "2022-01-01 00:00:00.000000000 UTC".parse()?,
        indoor: true,
        signature: Vec::new(),
        coverage: hexes
            .iter()
            .map(|hex| signal_level(&hex.location, SignalLevel::High).unwrap())
            .collect(),
        trust_score: 1000,
    };

    let mut footfall = HashMap::<hextree::Cell, Assignment>::new();
    let mut urbanized = HashMap::<hextree::Cell, Assignment>::new();
    let mut landtype = HashMap::<hextree::Cell, Assignment>::new();
    for hex in hexes.iter() {
        urbanized.insert(hex_cell(&hex.location), hex.urbanized().into());
        footfall.insert(hex_cell(&hex.location), hex.footfall().into());
        landtype.insert(hex_cell(&hex.location), hex.landtype().into());
    }

    let mut transaction = pool.begin().await?;
    CoverageObject {
        coverage_object,
        validity: CoverageObjectValidity::Valid,
    }
    .save(&mut transaction)
    .await?;
    transaction.commit().await?;

    let unassigned_hexes = UnassignedHex::fetch_unassigned(&pool);
    let hex_boost_data = HexBoostData::builder()
        .footfall(footfall)
        .landtype(landtype)
        .urbanization(urbanized)
        .build()?;
    let oba = set_oracle_boosting_assignments(unassigned_hexes, &hex_boost_data, &pool)
        .await?
        .collect::<Vec<_>>();

    assert_eq!(oba.len(), 1);
    assert_eq!(oba[0].assignments, hexes);

    Ok(())
}

#[sqlx::test]
async fn test_footfall_and_urbanization_and_landtype(pool: PgPool) -> anyhow::Result<()> {
    let start: DateTime<Utc> = "2022-01-01 00:00:00.000000000 UTC".parse()?;
    let end: DateTime<Utc> = "2022-01-02 00:00:00.000000000 UTC".parse()?;

    struct TestHex {
        loc: String,
        landtype: Assignment,
        footfall: Assignment,
        urbanized: Assignment,
        expected_score: Decimal,
    }

    impl TestHex {
        fn new(
            cell: &mut CellIndex,
            footfall: Assignment,
            landtype: Assignment,
            urbanized: Assignment,
            expected_score: usize,
        ) -> Self {
            let loc = cell.to_string();
            *cell = cell.succ().unwrap();
            Self {
                loc,
                landtype,
                footfall,
                urbanized,
                expected_score: Decimal::from(expected_score),
            }
        }
    }

    let hexes = {
        // NOTE(mj): Cell is mutated in constructor to keep elements aligned for readability
        let mut cell = CellIndex::try_from(0x8c2681a3064d9ff)?;
        use Assignment::*;
        vec![
            // yellow - POI ≥ 1 Urbanized
            TestHex::new(&mut cell, A, A, A, 400),
            TestHex::new(&mut cell, A, B, A, 400),
            TestHex::new(&mut cell, A, C, A, 400),
            // orange - POI ≥ 1 Not Urbanized
            TestHex::new(&mut cell, A, A, B, 400),
            TestHex::new(&mut cell, A, B, B, 400),
            TestHex::new(&mut cell, A, C, B, 400),
            // light green - Point of Interest Urbanized
            TestHex::new(&mut cell, B, A, A, 280),
            TestHex::new(&mut cell, B, B, A, 280),
            TestHex::new(&mut cell, B, C, A, 280),
            // dark green - Point of Interest Not Urbanized
            TestHex::new(&mut cell, B, A, B, 200),
            TestHex::new(&mut cell, B, B, B, 200),
            TestHex::new(&mut cell, B, C, B, 200),
            // light blue - No POI Urbanized
            TestHex::new(&mut cell, C, A, A, 160),
            TestHex::new(&mut cell, C, B, A, 120),
            TestHex::new(&mut cell, C, C, A, 20),
            // dark blue - No POI Not Urbanized
            TestHex::new(&mut cell, C, A, B, 80),
            TestHex::new(&mut cell, C, B, B, 60),
            TestHex::new(&mut cell, C, C, B, 12),
            // gray - Outside of USA
            TestHex::new(&mut cell, A, A, C, 0),
            TestHex::new(&mut cell, A, B, C, 0),
            TestHex::new(&mut cell, A, C, C, 0),
            TestHex::new(&mut cell, B, A, C, 0),
            TestHex::new(&mut cell, B, B, C, 0),
            TestHex::new(&mut cell, B, C, C, 0),
            TestHex::new(&mut cell, C, A, C, 0),
            TestHex::new(&mut cell, C, B, C, 0),
            TestHex::new(&mut cell, C, C, C, 0),
        ]
    };
    let sum = hexes.iter().map(|h| h.expected_score).sum::<Decimal>();

    assert_eq!(27, hexes.len());
    assert_eq!(dec!(4292), sum);

    let mut footfall = HashMap::new();
    let mut landtype = HashMap::new();
    let mut urbanized = HashMap::new();
    for hex in hexes.iter() {
        footfall.insert(hex_cell(&hex.loc), hex.footfall);
        urbanized.insert(hex_cell(&hex.loc), hex.urbanized);
        landtype.insert(hex_cell(&hex.loc), hex.landtype);
    }

    let uuid = Uuid::new_v4();
    let cbsd_id = "P27-SCE4255W120200039521XGB0102".to_string();
    let owner: PublicKeyBinary = "11xtYwQYnvkFYnJ9iZ8kmnetYKwhdi87Mcr36e1pVLrhBMPLjV9".parse()?;

    let coverage_object = file_store::coverage::CoverageObject {
        pub_key: PublicKeyBinary::from(vec![1]),
        uuid,
        key_type: file_store::coverage::KeyType::CbsdId(cbsd_id.clone()),
        coverage_claim_time: "2022-01-01 00:00:00.000000000 UTC".parse()?,
        indoor: true,
        signature: Vec::new(),
        coverage: hexes
            .iter()
            .map(|hex| signal_level(&hex.loc, SignalLevel::High).unwrap())
            .collect(),
        trust_score: 1000,
    };

    let mut transaction = pool.begin().await?;
    CoverageObject {
        coverage_object,
        validity: CoverageObjectValidity::Valid,
    }
    .save(&mut transaction)
    .await?;
    transaction.commit().await?;

    let unassigned_hexes = UnassignedHex::fetch_unassigned(&pool);
    let hex_boost_data = HexBoostData::builder()
        .footfall(footfall)
        .landtype(landtype)
        .urbanization(urbanized)
        .build()?;
    let _ = set_oracle_boosting_assignments(unassigned_hexes, &hex_boost_data, &pool).await?;

    let heartbeats = heartbeats(12, start, &owner, &cbsd_id, 0.0, 0.0, uuid);

    let coverage_objects = CoverageObjectCache::new(&pool);
    let coverage_claim_time_cache = CoverageClaimTimeCache::new();
    let location_cache = LocationCache::new(&pool);

    let epoch = start..end;
    let mut heartbeats = pin!(ValidatedHeartbeat::validate_heartbeats(
        stream::iter(heartbeats.map(Heartbeat::from)),
        &AllOwnersValid,
        &coverage_objects,
        &location_cache,
        2000,
        2000,
        &epoch,
        &MockGeofence,
    ));
    let mut transaction = pool.begin().await?;
    while let Some(heartbeat) = heartbeats.next().await.transpose()? {
        let coverage_claim_time = coverage_claim_time_cache
            .fetch_coverage_claim_time(
                heartbeat.heartbeat.key(),
                &heartbeat.heartbeat.coverage_object,
                &mut transaction,
            )
            .await?;
        let latest_seniority =
            Seniority::fetch_latest(heartbeat.heartbeat.key(), &mut transaction).await?;
        let seniority_update = SeniorityUpdate::determine_update_action(
            &heartbeat,
            coverage_claim_time.unwrap(),
            epoch.start,
            latest_seniority,
        );
        seniority_update.execute(&mut transaction).await?;
        heartbeat.save(&mut transaction).await?;
    }
    transaction.commit().await?;

    let last_timestamp = end - Duration::hours(12);
    let owner_speedtests = vec![
        acceptable_speedtest(owner.clone(), last_timestamp),
        acceptable_speedtest(owner.clone(), end),
    ];
    let mut averages = HashMap::new();
    averages.insert(owner.clone(), SpeedtestAverage::from(owner_speedtests));
    let speedtest_avgs = SpeedtestAverages { averages };

    let heartbeats = HeartbeatReward::validated(&pool, &epoch);
    let coverage_points = CoveragePoints::aggregate_points(
        &pool,
        heartbeats,
        &speedtest_avgs,
        &BoostedHexes::default(),
        &VerifiedRadioThresholds::default(),
        &epoch,
    )
    .await
    .context("aggregating points")?;

    //        (Footfall, Landtype, Urbanized)
    // Hex   | Assignment | Points Equation | Sum
    // -----------------------------------------------
    // == yellow - POI ≥ 1 Urbanized
    // hex1  | A, A, A    | 400 * 1         | 400
    // hex2  | A, B, A    | 400 * 1         | 400
    // hex3  | A, C, A    | 400 * 1         | 400
    // == orange - POI ≥ 1 Not Urbanized
    // hex4  | A, A, B    | 400 * 1         | 400
    // hex5  | A, B, B    | 400 * 1         | 400
    // hex6  | A, C, B    | 400 * 1         | 400
    // == light green - Point of Interest Urbanized
    // hex7  | B, A, A    | 400 * 0.70      | 280
    // hex8  | B, B, A    | 400 * 0.70      | 280
    // hex9  | B, C, A    | 400 * 0.70      | 280
    // == dark green - Point of Interest Not Urbanized
    // hex10 | B, A, B    | 400 * 0.50      | 200
    // hex11 | B, B, B    | 400 * 0.50      | 200
    // hex12 | B, C, B    | 400 * 0.50      | 200
    // == light blue - No POI Urbanized
    // hex13 | C, A, A    | 400 * 0.40     | 160
    // hex14 | C, B, A    | 400 * 0.30     | 120
    // hex15 | C, C, A    | 400 * 0.05     | 20
    // == dark blue - No POI Not Urbanized
    // hex16 | C, A, B    | 400 * 0.20     | 80
    // hex17 | C, B, B    | 400 * 0.15     | 60
    // hex18 | C, C, B    | 400 * 0.03     | 12
    // == gray - Outside of USA
    // hex19 | A, A, C    | 400 * 0.00     | 0
    // hex20 | A, B, C    | 400 * 0.00     | 0
    // hex21 | A, C, C    | 400 * 0.00     | 0
    // hex22 | B, A, C    | 400 * 0.00     | 0
    // hex23 | B, B, C    | 400 * 0.00     | 0
    // hex24 | B, C, C    | 400 * 0.00     | 0
    // hex25 | C, A, C    | 400 * 0.00     | 0
    // hex26 | C, B, C    | 400 * 0.00     | 0
    // hex27 | C, C, C    | 400 * 0.00     | 0
    // -----------------------------------------------
    //                                     = 4,292

    assert_eq!(coverage_points.hotspot_points(&owner), dec!(4292.0));

    Ok(())
}
