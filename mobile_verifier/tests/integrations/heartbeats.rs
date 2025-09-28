use chrono::{DateTime, Utc};
use file_store::file_sink::FileSinkClient;
use futures::stream;
use futures_util::TryStreamExt;
use helium_crypto::PublicKeyBinary;
use helium_proto::services::poc_mobile::{HeartbeatValidity, LocationSource};
use mobile_verifier::{
    cell_type::CellType,
    coverage::CoverageClaimTimeCache,
    heartbeats::{
        process_validated_heartbeats, HbType, Heartbeat, HeartbeatReward, ValidatedHeartbeat,
    },
};
use retainer::Cache;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use sqlx::PgPool;
use tokio::sync::mpsc;
use uuid::Uuid;

#[sqlx::test]
async fn test_save_wifi_heartbeat(pool: PgPool) -> anyhow::Result<()> {
    let coverage_object = Uuid::new_v4();
    let heartbeat = ValidatedHeartbeat {
        heartbeat: Heartbeat {
            hb_type: HbType::Wifi,
            hotspot_key: "11eX55faMbqZB7jzN4p67m6w7ScPMH6ubnvCjCPLh72J49PaJEL"
                .parse()
                .unwrap(),
            operation_mode: true,
            lat: 0.0,
            lon: 0.0,
            coverage_object: Some(coverage_object),
            location_validation_timestamp: None,
            timestamp: "2023-08-23 00:00:00.000000000 UTC".parse().unwrap(),
            location_source: LocationSource::Skyhook,
        },
        cell_type: CellType::SercommIndoor,
        distance_to_asserted: Some(1000), // Cannot be null
        coverage_meta: None,
        location_trust_score_multiplier: dec!(1.0),
        validity: HeartbeatValidity::Valid,
    };

    let mut transaction = pool.begin().await?;

    heartbeat.save(&mut transaction).await?;

    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM wifi_heartbeats")
        .fetch_one(&mut *transaction)
        .await?;

    assert_eq!(count, 1);

    Ok(())
}

#[sqlx::test]
async fn only_fetch_latest_hotspot(pool: PgPool) -> anyhow::Result<()> {
    let coverage_object = Uuid::new_v4();
    let hotspot_1: PublicKeyBinary =
        "112NqN2WWMwtK29PMzRby62fDydBJfsCLkCAf392stdok48ovNT6".parse()?;
    let hotspot_2: PublicKeyBinary =
        "11sctWiP9r5wDJVuDe1Th4XSL2vaawaLLSQF8f8iokAoMAJHxqp".parse()?;
    sqlx::query(
        r#"
INSERT INTO wifi_heartbeats (hotspot_key, cell_type, latest_timestamp, truncated_timestamp, coverage_object, location_trust_score_multiplier, distance_to_asserted)
VALUES
    ($2, 'novagenericwifiindoor', '2023-08-25 00:00:00+00', '2023-08-25 00:00:00+00', $3, 1.0, 0),
    ($2, 'novagenericwifiindoor', '2023-08-25 01:00:00+00', '2023-08-25 01:00:00+00', $3, 1.0, 0),
    ($1, 'novagenericwifiindoor', '2023-08-25 02:00:00+00', '2023-08-25 02:00:00+00', $3, 1.0, 0),
    ($2, 'novagenericwifiindoor', '2023-08-25 03:00:00+00', '2023-08-25 03:00:00+00', $3, 1.0, 0),
    ($1, 'novagenericwifiindoor', '2023-08-25 04:00:00+00', '2023-08-25 04:00:00+00', $3, 1.0, 0),
    ($2, 'novagenericwifiindoor', '2023-08-25 05:00:00+00', '2023-08-25 05:00:00+00', $3, 1.0, 0),
    ($1, 'novagenericwifiindoor', '2023-08-25 06:00:00+00', '2023-08-25 06:00:00+00', $3, 1.0, 0),
    ($2, 'novagenericwifiindoor', '2023-08-25 07:00:00+00', '2023-08-25 07:00:00+00', $3, 1.0, 0),
    ($1, 'novagenericwifiindoor', '2023-08-25 08:00:00+00', '2023-08-25 08:00:00+00', $3, 1.0, 0),
    ($2, 'novagenericwifiindoor', '2023-08-25 09:00:00+00', '2023-08-25 09:00:00+00', $3, 1.0, 0),
    ($1, 'novagenericwifiindoor', '2023-08-25 10:00:00+00', '2023-08-25 10:00:00+00', $3, 1.0, 0),
    ($2, 'novagenericwifiindoor', '2023-08-25 11:00:00+00', '2023-08-25 11:00:00+00', $3, 1.0, 0),
    ($1, 'novagenericwifiindoor', '2023-08-25 12:00:00+00', '2023-08-25 12:00:00+00', $3, 1.0, 0),
    ($2, 'novagenericwifiindoor', '2023-08-25 13:00:00+00', '2023-08-25 13:00:00+00', $3, 1.0, 0),
    ($1, 'novagenericwifiindoor', '2023-08-25 14:00:00+00', '2023-08-25 14:00:00+00', $3, 1.0, 0),
    ($2, 'novagenericwifiindoor', '2023-08-25 15:00:00+00', '2023-08-25 15:00:00+00', $3, 1.0, 0),
    ($1, 'novagenericwifiindoor', '2023-08-25 16:00:00+00', '2023-08-25 16:00:00+00', $3, 1.0, 0),
    ($2, 'novagenericwifiindoor', '2023-08-25 17:00:00+00', '2023-08-25 17:00:00+00', $3, 1.0, 0),
    ($1, 'novagenericwifiindoor', '2023-08-25 18:00:00+00', '2023-08-25 18:00:00+00', $3, 1.0, 0),
    ($2, 'novagenericwifiindoor', '2023-08-25 19:00:00+00', '2023-08-25 19:00:00+00', $3, 1.0, 0),
    ($1, 'novagenericwifiindoor', '2023-08-25 20:00:00+00', '2023-08-25 20:00:00+00', $3, 1.0, 0),
    ($2, 'novagenericwifiindoor', '2023-08-25 21:00:00+00', '2023-08-25 21:00:00+00', $3, 1.0, 0),
    ($1, 'novagenericwifiindoor', '2023-08-25 22:00:00+00', '2023-08-25 22:00:00+00', $3, 1.0, 0),
    ($2, 'novagenericwifiindoor', '2023-08-25 23:00:00+00', '2023-08-25 23:00:00+00', $3, 1.0, 0)
"#,
    )
    .bind(&hotspot_1)
    .bind(&hotspot_2)
    .bind(coverage_object)
    .execute(&pool)
    .await?;

    let start_period: DateTime<Utc> = "2023-08-25 00:00:00.000000000 UTC".parse()?;
    let end_period: DateTime<Utc> = "2023-08-26 00:00:00.000000000 UTC".parse()?;
    let heartbeat_reward: Vec<_> = HeartbeatReward::validated(&pool, &(start_period..end_period))
        .try_collect()
        .await?;

    assert_eq!(
        heartbeat_reward,
        vec![HeartbeatReward {
            hotspot_key: hotspot_2,
            trust_score_multipliers: vec![Decimal::ONE; 13],
            distances_to_asserted: vec![0; 13],
            coverage_object,
        }]
    );

    Ok(())
}

#[sqlx::test]
async fn ensure_minimum_count(pool: PgPool) -> anyhow::Result<()> {
    let coverage_object = Uuid::new_v4();
    let hotspot_1: PublicKeyBinary =
        "112NqN2WWMwtK29PMzRby62fDydBJfsCLkCAf392stdok48ovNT6".parse()?;

    sqlx::query(
        r#"
INSERT INTO wifi_heartbeats (hotspot_key, cell_type, latest_timestamp, truncated_timestamp, coverage_object, location_trust_score_multiplier, distance_to_asserted)
VALUES
    ($1, 'novagenericwifiindoor', '2023-08-25 00:00:00+00', '2023-08-25 00:00:00+00', $2, 1.0, 0),
    ($1, 'novagenericwifiindoor', '2023-08-25 01:00:00+00', '2023-08-25 01:00:00+00', $2, 1.0, 0),
    ($1, 'novagenericwifiindoor', '2023-08-25 02:00:00+00', '2023-08-25 02:00:00+00', $2, 1.0, 0),
    ($1, 'novagenericwifiindoor', '2023-08-25 03:00:00+00', '2023-08-25 03:00:00+00', $2, 1.0, 0),
    ($1, 'novagenericwifiindoor', '2023-08-25 04:00:00+00', '2023-08-25 04:00:00+00', $2, 1.0, 0),
    ($1, 'novagenericwifiindoor', '2023-08-25 05:00:00+00', '2023-08-25 05:00:00+00', $2, 1.0, 0),
    ($1, 'novagenericwifiindoor', '2023-08-25 06:00:00+00', '2023-08-25 06:00:00+00', $2, 1.0, 0),
    ($1, 'novagenericwifiindoor', '2023-08-25 07:00:00+00', '2023-08-25 07:00:00+00', $2, 1.0, 0),
    ($1, 'novagenericwifiindoor', '2023-08-25 08:00:00+00', '2023-08-25 08:00:00+00', $2, 1.0, 0),
    ($1, 'novagenericwifiindoor', '2023-08-25 09:00:00+00', '2023-08-25 09:00:00+00', $2, 1.0, 0),
    ($1, 'novagenericwifiindoor', '2023-08-25 10:00:00+00', '2023-08-25 10:00:00+00', $2, 1.0, 0)
"#,
    )
    .bind(&hotspot_1)
    .bind(coverage_object)
    .execute(&pool)
    .await?;

    let start_period: DateTime<Utc> = "2023-08-25 00:00:00.000000000 UTC".parse()?;
    let end_period: DateTime<Utc> = "2023-08-26 00:00:00.000000000 UTC".parse()?;
    let heartbeat_reward: Vec<_> = HeartbeatReward::validated(&pool, &(start_period..end_period))
        .try_collect()
        .await?;

    assert!(heartbeat_reward.is_empty());

    Ok(())
}

#[sqlx::test]
async fn ensure_wifi_hotspots_are_rewarded(pool: PgPool) -> anyhow::Result<()> {
    let early_coverage_object = Uuid::new_v4();
    let latest_coverage_object = Uuid::new_v4();
    let hotspot: PublicKeyBinary =
        "112NqN2WWMwtK29PMzRby62fDydBJfsCLkCAf392stdok48ovNT6".parse()?;
    sqlx::query(
        r#"
INSERT INTO wifi_heartbeats (hotspot_key, cell_type, latest_timestamp, truncated_timestamp, coverage_object, location_trust_score_multiplier, distance_to_asserted)
VALUES
    ($1, 'novagenericwifiindoor', '2023-08-25 00:00:00+00', '2023-08-25 00:00:00+00', $2, 1.0, 0),
    ($1, 'novagenericwifiindoor', '2023-08-25 01:00:00+00', '2023-08-25 01:00:00+00', $2, 1.0, 0),
    ($1, 'novagenericwifiindoor', '2023-08-25 02:00:00+00', '2023-08-25 02:00:00+00', $2, 1.0, 0),
    ($1, 'novagenericwifiindoor', '2023-08-25 03:00:00+00', '2023-08-25 03:00:00+00', $2, 1.0, 0),
    ($1, 'novagenericwifiindoor', '2023-08-25 04:00:00+00', '2023-08-25 04:00:00+00', $2, 1.0, 0),
    ($1, 'novagenericwifiindoor', '2023-08-25 05:00:00+00', '2023-08-25 05:00:00+00', $2, 1.0, 0),
    ($1, 'novagenericwifiindoor', '2023-08-25 06:00:00+00', '2023-08-25 06:00:00+00', $2, 1.0, 0),
    ($1, 'novagenericwifiindoor', '2023-08-25 07:00:00+00', '2023-08-25 07:00:00+00', $2, 1.0, 0),
    ($1, 'novagenericwifiindoor', '2023-08-25 08:00:00+00', '2023-08-25 08:00:00+00', $2, 1.0, 0),
    ($1, 'novagenericwifiindoor', '2023-08-25 09:00:00+00', '2023-08-25 09:00:00+00', $2, 1.0, 0),
    ($1, 'novagenericwifiindoor', '2023-08-25 10:00:00+00', '2023-08-25 10:00:00+00', $2, 1.0, 0),
    ($1, 'novagenericwifiindoor', '2023-08-25 11:00:00+00', '2023-08-25 11:00:00+00', $3, 1.0, 0)
"#,
    )
    .bind(&hotspot)
    .bind(early_coverage_object)
    .bind(latest_coverage_object)
    .execute(&pool)
    .await?;

    let start_period: DateTime<Utc> = "2023-08-25 00:00:00.000000000 UTC".parse()?;
    let end_period: DateTime<Utc> = "2023-08-26 00:00:00.000000000 UTC".parse()?;
    let heartbeat_reward: Vec<_> = HeartbeatReward::validated(&pool, &(start_period..end_period))
        .try_collect()
        .await?;

    assert_eq!(
        heartbeat_reward,
        vec![HeartbeatReward {
            hotspot_key: hotspot,
            trust_score_multipliers: vec![Decimal::ONE; 12],
            distances_to_asserted: vec![0; 12],
            coverage_object: latest_coverage_object,
        }]
    );

    Ok(())
}

#[sqlx::test]
async fn ensure_wifi_hotspots_use_average_location_trust_score(pool: PgPool) -> anyhow::Result<()> {
    let early_coverage_object = Uuid::new_v4();
    let latest_coverage_object = Uuid::new_v4();
    let hotspot: PublicKeyBinary =
        "112NqN2WWMwtK29PMzRby62fDydBJfsCLkCAf392stdok48ovNT6".parse()?;
    sqlx::query(
        r#"
INSERT INTO wifi_heartbeats (hotspot_key, cell_type, latest_timestamp, truncated_timestamp, coverage_object, location_trust_score_multiplier, distance_to_asserted)
VALUES
    ($1, 'novagenericwifiindoor', '2023-08-25 00:00:00+00', '2023-08-25 00:00:00+00', $2, 1.0, 0),
    ($1, 'novagenericwifiindoor', '2023-08-25 01:00:00+00', '2023-08-25 01:00:00+00', $2, 1.0, 0),
    ($1, 'novagenericwifiindoor', '2023-08-25 02:00:00+00', '2023-08-25 02:00:00+00', $2, 1.0, 0),
    ($1, 'novagenericwifiindoor', '2023-08-25 03:00:00+00', '2023-08-25 03:00:00+00', $2, 1.0, 0),
    ($1, 'novagenericwifiindoor', '2023-08-25 04:00:00+00', '2023-08-25 04:00:00+00', $2, 1.0, 0),
    ($1, 'novagenericwifiindoor', '2023-08-25 05:00:00+00', '2023-08-25 05:00:00+00', $2, 1.0, 0),
    ($1, 'novagenericwifiindoor', '2023-08-25 06:00:00+00', '2023-08-25 06:00:00+00', $2, 1.0, 0),
    ($1, 'novagenericwifiindoor', '2023-08-25 07:00:00+00', '2023-08-25 07:00:00+00', $2, 1.0, 0),
    ($1, 'novagenericwifiindoor', '2023-08-25 08:00:00+00', '2023-08-25 08:00:00+00', $2, 0.25, 0),
    ($1, 'novagenericwifiindoor', '2023-08-25 09:00:00+00', '2023-08-25 09:00:00+00', $2, 0.25, 0),
    ($1, 'novagenericwifiindoor', '2023-08-25 10:00:00+00', '2023-08-25 10:00:00+00', $2, 0.25, 0),
    ($1, 'novagenericwifiindoor', '2023-08-25 11:00:00+00', '2023-08-25 11:00:00+00', $3, 0.25, 0)
"#,
    )
    .bind(&hotspot)
    .bind(early_coverage_object)
    .bind(latest_coverage_object)
    .execute(&pool)
    .await?;

    let start_period: DateTime<Utc> = "2023-08-25 00:00:00.000000000 UTC".parse()?;
    let end_period: DateTime<Utc> = "2023-08-26 00:00:00.000000000 UTC".parse()?;
    let heartbeat_reward: Vec<_> = HeartbeatReward::validated(&pool, &(start_period..end_period))
        .try_collect()
        .await?;

    assert_eq!(
        heartbeat_reward,
        vec![HeartbeatReward {
            hotspot_key: hotspot,
            trust_score_multipliers: vec![
                dec!(1.0),
                dec!(1.0),
                dec!(1.0),
                dec!(1.0),
                dec!(1.0),
                dec!(1.0),
                dec!(1.0),
                dec!(1.0),
                dec!(0.25),
                dec!(0.25),
                dec!(0.25),
                dec!(0.25)
            ],
            distances_to_asserted: vec![0; 12],
            coverage_object: latest_coverage_object,
        }]
    );

    Ok(())
}

#[sqlx::test]
async fn test_process_validated_heartbeats(pool: PgPool) -> anyhow::Result<()> {
    // This test case shows problem I've found in process_validated_heartbeats function
    // 1. process_validated_heartbeats processes only the first heartbeat per hour and ignores
    //    others. In this test case we have first good heartbeat (multiplier = 1)
    //    and others nine are bad (multiplier = 0), the final result is 1
    // 2. In wifi_heartbeats table latest_timestamp always contains the value of the first
    //    timestamp (not the latest)
    let coverage_object = Uuid::new_v4();

    // Closure to create heartbeats with different parameters
    let create_heartbeat = |minutes: u32,
                            distance_to_asserted: i64,
                            location_trust_score_multiplier: Decimal|
     -> ValidatedHeartbeat {
        ValidatedHeartbeat {
            heartbeat: Heartbeat {
                hb_type: HbType::Wifi,
                hotspot_key: "11eX55faMbqZB7jzN4p67m6w7ScPMH6ubnvCjCPLh72J49PaJEL"
                    .parse()
                    .unwrap(),
                operation_mode: true,
                lat: 0.0,
                lon: 0.0,
                coverage_object: Some(coverage_object),
                location_validation_timestamp: None,
                timestamp: format!("2023-08-23 00:{:02}:00.000000000 UTC", minutes)
                    .parse()
                    .unwrap(),
                location_source: LocationSource::Gps,
            },
            cell_type: CellType::NovaGenericWifiOutdoor,
            distance_to_asserted: Some(distance_to_asserted),
            coverage_meta: None,
            location_trust_score_multiplier,
            validity: HeartbeatValidity::Valid,
        }
    };

    // Create two similar heartbeats in the same hour with different trust scores
    let good_hb = create_heartbeat(5, 0, dec!(1.0));
    // pt - poor trust
    let pt_hb_1 = create_heartbeat(10, 2000, dec!(0.0));
    let pt_hb_2 = create_heartbeat(15, 2000, dec!(0.0));
    let pt_hb_3 = create_heartbeat(20, 2000, dec!(0.0));
    let pt_hb_4 = create_heartbeat(25, 2000, dec!(0.0));
    let pt_hb_5 = create_heartbeat(30, 2000, dec!(0.0));
    let pt_hb_6 = create_heartbeat(35, 2000, dec!(0.0));
    let pt_hb_7 = create_heartbeat(40, 2000, dec!(0.0));
    let pt_hb_8 = create_heartbeat(45, 2000, dec!(0.0));
    let pt_hb_9 = create_heartbeat(50, 2000, dec!(0.0));

    // Create mock file sinks
    let (heartbeat_tx, mut heartbeat_rx) = mpsc::channel(10);
    let (seniority_tx, _) = mpsc::channel(10);
    let heartbeat_sink = FileSinkClient::new(heartbeat_tx, "test_heartbeats");
    let seniority_sink = FileSinkClient::new(seniority_tx, "test_seniority");

    // Create cache and coverage claim time cache
    let heartbeat_cache = Cache::<(String, DateTime<Utc>), ()>::new();
    let coverage_claim_time_cache = CoverageClaimTimeCache::new();

    // Create stream of validated heartbeats
    let hb_list = vec![
        good_hb.clone(),
        pt_hb_1.clone(),
        pt_hb_2.clone(),
        pt_hb_3.clone(),
        pt_hb_4.clone(),
        pt_hb_5.clone(),
        pt_hb_6.clone(),
        pt_hb_7.clone(),
        pt_hb_8.clone(),
        pt_hb_9.clone(),
    ];
    let heartbeats = stream::iter(hb_list.into_iter().map(Ok));

    // Start transaction
    let mut transaction = pool.begin().await?;

    // Process the heartbeats
    process_validated_heartbeats(
        heartbeats,
        &heartbeat_cache,
        &coverage_claim_time_cache,
        &heartbeat_sink,
        &seniority_sink,
        &mut transaction,
    )
    .await?;

    // Verify  heartbeats were written to file sink
    // (Check only two but should be more in channel)
    let heartbeat_message1 = heartbeat_rx
        .recv()
        .await
        .expect("Should receive good heartbeat");
    let heartbeat_message2 = heartbeat_rx
        .recv()
        .await
        .expect("Should receive poor trust heartbeat");

    match heartbeat_message1 {
        file_store::file_sink::Message::Data(_, heartbeat) => {
            assert_eq!(heartbeat.validity, HeartbeatValidity::Valid as i32);
            assert_eq!(heartbeat.location_trust_score_multiplier, 1000); // 1.0 * 1000
            assert_eq!(heartbeat.distance_to_asserted, 0);
        }
        _ => panic!("Expected heartbeat data message"),
    }

    match heartbeat_message2 {
        file_store::file_sink::Message::Data(_, heartbeat) => {
            assert_eq!(heartbeat.validity, HeartbeatValidity::Valid as i32);
            assert_eq!(heartbeat.location_trust_score_multiplier, 0); // 0.0 * 1000
            assert_eq!(heartbeat.distance_to_asserted, 2000);
        }
        _ => panic!("Expected heartbeat data message"),
    }

    // Verify that wifi_heartbeat was saved to database
    transaction.commit().await?;

    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM wifi_heartbeats")
        .fetch_one(&pool)
        .await?;

    assert_eq!(count, 1); // Only 1 row due to ON CONFLICT

    // Verify the heartbeat data in database (should be the latest one - poor trust)
    let db_heartbeat: (String, Decimal, DateTime<Utc>) = sqlx::query_as(
        "SELECT hotspot_key::text, location_trust_score_multiplier, latest_timestamp FROM wifi_heartbeats",
    )
    .fetch_one(&pool)
    .await?;

    assert_eq!(db_heartbeat.0, good_hb.heartbeat.hotspot_key.to_string());
    // WRONG!(?)
    assert_eq!(db_heartbeat.1, dec!(1.0)); // Despite there is only 1 of 10 heartbeats with location
                                           // trust score = 1 and others have 0, the total result
                                           // is 1, because others heartbeats are ignored

    // WRONG! It is not the latest timestamp, it is the first one
    assert_eq!(db_heartbeat.2.to_string(), "2023-08-23 00:05:00 UTC");

    Ok(())
}
