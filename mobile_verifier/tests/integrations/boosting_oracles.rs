use crate::common::{self, GatewayClientAllOwnersValid, MockHexBoostDataColl};
use anyhow::Context;
use chrono::{DateTime, Duration, Utc};
use file_store::{
    coverage::RadioHexSignalLevel,
    file_upload::{self, FileUpload},
    speedtest::CellSpeedtest,
    traits::{FileSinkCommitStrategy, FileSinkRollTime, FileSinkWriteExt},
    wifi_heartbeat::{WifiHeartbeat, WifiHeartbeatIngestReport},
};
use futures::stream::{self, StreamExt};
use h3o::CellIndex;
use helium_crypto::PublicKeyBinary;
use helium_proto::services::poc_mobile::OracleBoostingReportV1;
use helium_proto::services::poc_mobile::{
    CoverageObjectValidity, LocationSource, OracleBoostingHexAssignment, SignalLevel,
};
use hex_assignments::Assignment;
use mobile_config::boosted_hex_info::BoostedHexes;
use mobile_verifier::{
    banning::BannedRadios,
    boosting_oracles::DataSetDownloaderDaemon,
    coverage::{
        new_coverage_object_notification_channel, CoverageClaimTimeCache, CoverageObject,
        CoverageObjectCache, NewCoverageObjectNotification,
    },
    geofence::GeofenceValidator,
    heartbeats::{last_location::LocationCache, Heartbeat, HeartbeatReward, ValidatedHeartbeat},
    reward_shares::CoverageShares,
    rewarder::boosted_hex_eligibility::BoostedHexEligibility,
    seniority::{Seniority, SeniorityUpdate},
    speedtests::Speedtest,
    speedtests_average::{SpeedtestAverage, SpeedtestAverages},
    unique_connections::UniqueConnectionCounts,
};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use sqlx::PgPool;
use std::{collections::HashMap, pin::pin};
use tempfile::TempDir;
use uuid::Uuid;

#[derive(Clone)]
struct MockGeofence;

impl GeofenceValidator for MockGeofence {
    fn in_valid_region(&self, _heartbeat: &Heartbeat) -> bool {
        true
    }
}

fn heartbeats(
    num: usize,
    start: DateTime<Utc>,
    hotspot_key: &PublicKeyBinary,
    lon: f64,
    lat: f64,
    coverage_object: Uuid,
) -> impl Iterator<Item = WifiHeartbeatIngestReport> + '_ {
    (0..num).map(move |i| {
        let report = WifiHeartbeat {
            pubkey: hotspot_key.clone(),
            lon,
            lat,
            operation_mode: true,
            timestamp: DateTime::<Utc>::MIN_UTC,
            coverage_object: Vec::from(coverage_object.into_bytes()),
            location_validation_timestamp: Some(start),
            location_source: LocationSource::Gps,
        };
        WifiHeartbeatIngestReport {
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

use aws_local::*;
use hex_assignments::HexBoostData;
use std::{path::PathBuf, str::FromStr};

pub async fn create_data_set_downloader(
    pool: PgPool,
    file_paths: Vec<PathBuf>,
    file_upload: FileUpload,
    new_coverage_object_notification: NewCoverageObjectNotification,
    tmp_dir: &TempDir,
) -> (DataSetDownloaderDaemon, PathBuf, String) {
    let bucket_name = gen_bucket_name();

    let awsl = AwsLocal::new(AWS_ENDPOINT, &bucket_name).await;

    for file_path in file_paths {
        awsl.put_file_to_aws(&file_path).await.unwrap();
    }

    let uuid: Uuid = Uuid::new_v4();
    let data_set_directory = tmp_dir.path().join(uuid.to_string());
    tokio::fs::create_dir_all(data_set_directory.clone())
        .await
        .unwrap();

    let file_store = awsl.file_store.clone();
    let poll_duration = std::time::Duration::from_secs(4);

    let (oracle_boosting_reports, _) = OracleBoostingReportV1::file_sink(
        tmp_dir.path(),
        file_upload.clone(),
        FileSinkCommitStrategy::Automatic,
        FileSinkRollTime::Duration(std::time::Duration::from_secs(15 * 60)),
        env!("CARGO_PKG_NAME"),
    )
    .await
    .unwrap();

    let mut data_set_downloader = DataSetDownloaderDaemon::new(
        pool,
        HexBoostData::default(),
        file_store,
        oracle_boosting_reports,
        data_set_directory.clone(),
        new_coverage_object_notification,
        poll_duration,
    );

    data_set_downloader.fetch_first_datasets().await.unwrap();
    data_set_downloader.check_for_new_data_sets().await.unwrap();
    (data_set_downloader, data_set_directory, bucket_name)
}

pub async fn hex_assignment_file_exist(pool: &PgPool, filename: &str) -> bool {
    sqlx::query_scalar::<_, bool>(
        r#"
            SELECT EXISTS(SELECT 1 FROM hex_assignment_data_set_status WHERE filename = $1)
        "#,
    )
    .bind(filename)
    .fetch_one(pool)
    .await
    .unwrap()
}

#[sqlx::test]
async fn test_dataset_downloader(pool: PgPool) {
    // Scenario:
    // 1. DataSetDownloader downloads initial files
    // 2. Upload a new file
    // 3. DataSetDownloader downloads new file

    let paths = [
        "footfall.1722895200000.gz",
        "urbanization.1722895200000.gz",
        "landtype.1722895200000.gz",
        "service_provider_override.1739404800000.gz",
    ];

    let file_paths: Vec<PathBuf> = paths
        .iter()
        .map(|f| PathBuf::from(format!("./tests/integrations/fixtures/{}", f)))
        .collect();

    let (file_upload_tx, _file_upload_rx) = file_upload::message_channel();
    let file_upload = FileUpload {
        sender: file_upload_tx,
    };

    let (_, new_coverage_obj_notification) = new_coverage_object_notification_channel();

    let tmp_dir = TempDir::new().expect("Unable to create temp dir");
    let (mut data_set_downloader, _, bucket_name) = create_data_set_downloader(
        pool.clone(),
        file_paths,
        file_upload,
        new_coverage_obj_notification,
        &tmp_dir,
    )
    .await;
    assert!(hex_assignment_file_exist(&pool, "footfall.1722895200000.gz").await);
    assert!(hex_assignment_file_exist(&pool, "urbanization.1722895200000.gz").await);
    assert!(hex_assignment_file_exist(&pool, "landtype.1722895200000.gz").await);
    assert!(hex_assignment_file_exist(&pool, "service_provider_override.1739404800000.gz").await);

    let awsl = AwsLocal::new(AWS_ENDPOINT, &bucket_name).await;
    awsl.put_file_to_aws(
        &PathBuf::from_str("./tests/integrations/fixtures/footfall.1732895200000.gz").unwrap(),
    )
    .await
    .unwrap();
    data_set_downloader.check_for_new_data_sets().await.unwrap();
    assert!(hex_assignment_file_exist(&pool, "footfall.1732895200000.gz").await);
}

#[sqlx::test]
async fn test_footfall_and_urbanization_report(pool: PgPool) -> anyhow::Result<()> {
    let uuid = Uuid::new_v4();

    fn new_hex_assignment(
        cell: &mut CellIndex,
        footfall: Assignment,
        landtype: Assignment,
        urbanized: Assignment,
        service_provider_override: Assignment,
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
            service_provider_override: service_provider_override.into(),
        }
    }

    let hexes = {
        // NOTE(mj): Cell is mutated in constructor to keep elements aligned for readability
        let mut cell = CellIndex::try_from(0x8c2681a3064d9ff)?;
        use hex_assignments::Assignment::*;
        vec![
            // yellow - POI ≥ 1 Urbanized, no service provider override
            new_hex_assignment(&mut cell, A, A, A, C, 1000),
            new_hex_assignment(&mut cell, A, B, A, C, 1000),
            new_hex_assignment(&mut cell, A, C, A, C, 1000),
            // orange - POI ≥ 1 Not Urbanized, no service provider override
            new_hex_assignment(&mut cell, A, A, B, C, 1000),
            new_hex_assignment(&mut cell, A, B, B, C, 1000),
            new_hex_assignment(&mut cell, A, C, B, C, 1000),
            // light green - Point of Interest Urbanized, no service provider override
            new_hex_assignment(&mut cell, B, A, A, C, 700),
            new_hex_assignment(&mut cell, B, B, A, C, 700),
            new_hex_assignment(&mut cell, B, C, A, C, 700),
            // dark green - Point of Interest Not Urbanized, no service provider override
            new_hex_assignment(&mut cell, B, A, B, C, 500),
            new_hex_assignment(&mut cell, B, B, B, C, 500),
            new_hex_assignment(&mut cell, B, C, B, C, 500),
            // HRP-20250409 footfall C
            new_hex_assignment(&mut cell, C, A, A, C, 30),
            new_hex_assignment(&mut cell, C, B, A, C, 30),
            new_hex_assignment(&mut cell, C, C, A, C, 30),
            new_hex_assignment(&mut cell, C, A, B, C, 30),
            new_hex_assignment(&mut cell, C, B, B, C, 30),
            new_hex_assignment(&mut cell, C, C, B, C, 30),
            // gray - Outside of USA, no service provider override
            new_hex_assignment(&mut cell, A, A, C, C, 0),
            new_hex_assignment(&mut cell, A, B, C, C, 0),
            new_hex_assignment(&mut cell, A, C, C, C, 0),
            new_hex_assignment(&mut cell, B, A, C, C, 0),
            new_hex_assignment(&mut cell, B, B, C, C, 0),
            new_hex_assignment(&mut cell, B, C, C, C, 0),
            new_hex_assignment(&mut cell, C, A, C, C, 0),
            new_hex_assignment(&mut cell, C, B, C, C, 0),
            new_hex_assignment(&mut cell, C, C, C, C, 0),
        ]
    };

    let coverage_object = file_store::coverage::CoverageObject {
        pub_key: PublicKeyBinary::from(vec![1]),
        uuid,
        key_type: file_store::coverage::KeyType::HotspotKey(PublicKeyBinary::from(vec![1])),
        coverage_claim_time: "2022-01-01 00:00:00.000000000 UTC".parse()?,
        indoor: true,
        signature: Vec::new(),
        coverage: hexes
            .iter()
            .map(|hex| signal_level(&hex.location, SignalLevel::High).unwrap())
            .collect(),
        trust_score: 1000,
    };

    let mut hex_boost_data = MockHexBoostDataColl::default();

    for hex in hexes.iter() {
        hex_boost_data
            .urbanized
            .insert(hex_cell(&hex.location), hex.urbanized().into());
        hex_boost_data
            .footfall
            .insert(hex_cell(&hex.location), hex.footfall().into());
        hex_boost_data
            .landtype
            .insert(hex_cell(&hex.location), hex.landtype().into());
        if hex.service_provider_override {
            hex_boost_data
                .service_provider_override
                .insert(hex_cell(&hex.location));
        }
    }

    let mut transaction = pool.begin().await?;
    CoverageObject {
        coverage_object,
        validity: CoverageObjectValidity::Valid,
    }
    .save(&mut transaction)
    .await?;
    transaction.commit().await?;

    let oba = common::set_unassigned_oracle_boosting_assignments(&pool, &hex_boost_data).await?;

    assert_eq!(oba.len(), 1);
    assert_eq!(oba[0].assignments, hexes);

    Ok(())
}

#[sqlx::test]
async fn test_footfall_and_urbanization_and_landtype_and_service_provider_override(
    pool: PgPool,
) -> anyhow::Result<()> {
    let end: DateTime<Utc> = Utc::now() + Duration::minutes(10);
    let start: DateTime<Utc> = end - Duration::days(1);

    struct TestHex {
        loc: String,
        landtype: Assignment,
        footfall: Assignment,
        urbanized: Assignment,
        service_provider_override: Assignment,
        expected_score: Decimal,
    }

    impl TestHex {
        fn new(
            cell: &mut CellIndex,
            footfall: Assignment,
            landtype: Assignment,
            urbanized: Assignment,
            service_provider_override: Assignment,
            expected_score: usize,
        ) -> Self {
            let loc = cell.to_string();
            *cell = cell.succ().unwrap();
            Self {
                loc,
                landtype,
                footfall,
                urbanized,
                service_provider_override,
                expected_score: Decimal::from(expected_score),
            }
        }
    }

    let hexes = {
        // NOTE(mj): Cell is mutated in constructor to keep elements aligned for readability
        let mut cell = CellIndex::try_from(0x8c2681a3064c5ff)?;
        use hex_assignments::Assignment::*;
        vec![
            // yellow - POI ≥ 1 Urbanized, no service provider override
            TestHex::new(&mut cell, A, A, A, C, 400),
            TestHex::new(&mut cell, A, B, A, C, 400),
            TestHex::new(&mut cell, A, C, A, C, 400),
            // orange - POI ≥ 1 Not Urbanized, no service provider override
            TestHex::new(&mut cell, A, A, B, C, 400),
            TestHex::new(&mut cell, A, B, B, C, 400),
            TestHex::new(&mut cell, A, C, B, C, 400),
            // light green - Point of Interest Urbanized, no service provider override
            TestHex::new(&mut cell, B, A, A, C, 280),
            TestHex::new(&mut cell, B, B, A, C, 280),
            TestHex::new(&mut cell, B, C, A, C, 280),
            // dark green - Point of Interest Not Urbanized, no service provider override
            TestHex::new(&mut cell, B, A, B, C, 200),
            TestHex::new(&mut cell, B, B, B, C, 200),
            TestHex::new(&mut cell, B, C, B, C, 200),
            // light blue - No POI Urbanized, no service provider override
            TestHex::new(&mut cell, C, A, A, C, 160),
            TestHex::new(&mut cell, C, B, A, C, 120),
            TestHex::new(&mut cell, C, C, A, C, 20),
            // dark blue - No POI Not Urbanized, no service provider override
            TestHex::new(&mut cell, C, A, B, C, 80),
            TestHex::new(&mut cell, C, B, B, C, 60),
            TestHex::new(&mut cell, C, C, B, C, 12),
            // gray - Outside of USA, no service provider override
            TestHex::new(&mut cell, A, A, C, C, 0),
            TestHex::new(&mut cell, A, B, C, C, 0),
            TestHex::new(&mut cell, A, C, C, C, 0),
            TestHex::new(&mut cell, B, A, C, C, 0),
            TestHex::new(&mut cell, B, B, C, C, 0),
            TestHex::new(&mut cell, B, C, C, C, 0),
            TestHex::new(&mut cell, C, A, C, C, 0),
            TestHex::new(&mut cell, C, B, C, C, 0),
            TestHex::new(&mut cell, C, C, C, C, 0),
            // gray - Outside of USA, HAS service provider override
            TestHex::new(&mut cell, C, C, C, A, 100),
        ]
    };
    let sum = hexes.iter().map(|h| h.expected_score).sum::<Decimal>();

    assert_eq!(28, hexes.len());
    assert_eq!(dec!(4392), sum);

    let mut hex_boost_data = MockHexBoostDataColl::default();

    for hex in hexes.iter() {
        hex_boost_data
            .footfall
            .insert(hex_cell(&hex.loc), hex.footfall);
        hex_boost_data
            .urbanized
            .insert(hex_cell(&hex.loc), hex.urbanized);
        hex_boost_data
            .landtype
            .insert(hex_cell(&hex.loc), hex.landtype);
        if hex.service_provider_override == Assignment::A {
            hex_boost_data
                .service_provider_override
                .insert(hex_cell(&hex.loc));
        }
    }

    let uuid = Uuid::new_v4();

    let pub_key = PublicKeyBinary::from(vec![1]);
    let coverage_object = file_store::coverage::CoverageObject {
        pub_key: pub_key.clone(),
        uuid,
        key_type: file_store::coverage::KeyType::HotspotKey(pub_key.clone()),
        coverage_claim_time: start,
        indoor: true,
        signature: Vec::new(),
        coverage: hexes
            .iter()
            .map(|hex| signal_level(&hex.loc, SignalLevel::Low).unwrap())
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

    let _ = common::set_unassigned_oracle_boosting_assignments(&pool, &hex_boost_data).await?;

    let hb_pubkey = pub_key.clone();
    let heartbeats = heartbeats(13, start, &hb_pubkey, -105.272404746, 40.019418356, uuid);

    let coverage_objects = CoverageObjectCache::new(&pool);
    let coverage_claim_time_cache = CoverageClaimTimeCache::new();
    let location_cache = LocationCache::new(&pool);

    let epoch = start..end;
    let mut heartbeats = pin!(ValidatedHeartbeat::validate_heartbeats(
        stream::iter(heartbeats.map(Heartbeat::from)),
        &GatewayClientAllOwnersValid,
        &coverage_objects,
        &location_cache,
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
            latest_seniority,
        )?;
        seniority_update.execute(&mut transaction).await?;
        heartbeat.save(&mut transaction).await?;
    }
    transaction.commit().await?;

    let last_timestamp = end - Duration::hours(12);
    let owner_speedtests = vec![
        acceptable_speedtest(hb_pubkey.clone(), last_timestamp),
        acceptable_speedtest(hb_pubkey.clone(), end),
    ];
    let mut averages = HashMap::new();
    averages.insert(hb_pubkey.clone(), SpeedtestAverage::from(owner_speedtests));
    let speedtest_avgs = SpeedtestAverages { averages };

    let heartbeats = HeartbeatReward::validated(&pool, &epoch);
    let coverage_shares = CoverageShares::new(
        &pool,
        heartbeats,
        &speedtest_avgs,
        &BoostedHexes::default(),
        &BoostedHexEligibility::default(),
        &BannedRadios::default(),
        &UniqueConnectionCounts::default(),
        &epoch,
    )
    .await
    .context("aggregating points")?;

    // (Footfall, Landtype, Urbanized, Service Provider Selected)
    // Hex   | Assignment | Points Equation | Sum
    // -----------------------------------------------
    // == yellow - POI ≥ 1 Urbanized, no service provider override
    // hex1  | A, A, A, C    | 100 * 1         | 100
    // hex2  | A, B, A, C    | 100 * 1         | 100
    // hex3  | A, C, A, C    | 100 * 1         | 100
    // == orange - POI ≥ 1 Not Urbanized, no service provider override
    // hex4  | A, A, B, C    | 100 * 1         | 100
    // hex5  | A, B, B, C    | 100 * 1         | 100
    // hex6  | A, C, B, C    | 100 * 1         | 100
    // == light green - Point of Interest Urbanized, no service provider override
    // hex7  | B, A, A, C    | 100 * 0.70      | 70
    // hex8  | B, B, A, C    | 100 * 0.70      | 70
    // hex9  | B, C, A, C    | 100 * 0.70      | 70
    // == dark green - Point of Interest Not Urbanized, no service provider override
    // hex10 | B, A, B, C    | 100 * 0.50      | 50
    // hex11 | B, B, B, C    | 100 * 0.50      | 50
    // hex12 | B, C, B, C    | 100 * 0.50      | 50
    // == HRP-20250409 footfall C
    // hex13 | C, A, A, C    | 100 * 0.03      | 3
    // hex14 | C, B, A, C    | 100 * 0.03      | 3
    // hex15 | C, C, A, C    | 100 * 0.03      | 3
    // hex16 | C, A, B, C    | 100 * 0.03      | 3
    // hex17 | C, B, B, C    | 100 * 0.03      | 3
    // hex18 | C, C, B, C    | 100 * 0.03      | 3
    // == gray - Outside of USA, no service provider override
    // hex19 | A, A, C, C    | 100 * 0.00      | 0
    // hex20 | A, B, C, C    | 100 * 0.00      | 0
    // hex21 | A, C, C, C    | 100 * 0.00      | 0
    // hex22 | B, A, C, C    | 100 * 0.00      | 0
    // hex23 | B, B, C, C    | 100 * 0.00      | 0
    // hex24 | B, C, C, C    | 100 * 0.00      | 0
    // hex25 | C, A, C, C    | 100 * 0.00      | 0
    // hex26 | C, B, C, C    | 100 * 0.00      | 0
    // hex27 | C, C, C, C    | 100 * 0.00      | 0
    // == gray - Outside of USA, HAS service provider override
    // hex28 | A, A, C, A    | 100 * 1.00      | 100

    // -----------------------------------------------
    //                                         = 1,078

    assert_eq!(
        coverage_shares.test_hotspot_reward_shares(&pub_key),
        dec!(1078.0)
    );

    Ok(())
}
