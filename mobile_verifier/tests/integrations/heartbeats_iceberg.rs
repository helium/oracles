use chrono::{DateTime, Duration, SubsecRound, Utc};
use file_store::aws_local::AwsLocal;
use file_store_oracles::FileType;
use helium_crypto::PublicKeyBinary;
use helium_proto::services::poc_mobile::{
    CellType as ProtoCellType, Heartbeat as HeartbeatProto, HeartbeatValidity, LocationSource,
};
use mobile_config::gateway::service::info::DeviceType;
use mobile_verifier::{
    backfill::BackfillOptions,
    cell_type::CellType,
    heartbeats::{backfill::HeartbeatBackfiller, Heartbeat, ValidatedHeartbeat},
    iceberg::{self, heartbeat::IcebergHeartbeat},
};
use rust_decimal_macros::dec;
use sqlx::PgPool;
use uuid::Uuid;

const TEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

fn test_backfill_options(
    process_name: &str,
    start_after: DateTime<Utc>,
    stop_after: DateTime<Utc>,
) -> BackfillOptions {
    BackfillOptions {
        process_name: process_name.to_string(),
        start_after,
        stop_after,
        poll_duration: Some(std::time::Duration::from_millis(100)),
        idle_timeout: Some(std::time::Duration::from_millis(500)),
    }
}

fn make_validated_heartbeat_proto(
    timestamp: DateTime<Utc>,
    validity: HeartbeatValidity,
) -> HeartbeatProto {
    let pubkey: PublicKeyBinary = "112NqN2WWMwtK29PMzRby62fDydBJfsCLkCAf392stdok48ovNT6"
        .parse()
        .unwrap();
    HeartbeatProto {
        pub_key: pubkey.as_ref().to_vec(),
        timestamp: timestamp.timestamp() as u64,
        cell_type: ProtoCellType::NovaGenericWifiIndoor as i32,
        validity: validity as i32,
        lat: 37.7749,
        lon: -122.4194,
        coverage_object: Uuid::new_v4().as_bytes().to_vec(),
        location_validation_timestamp: timestamp.timestamp() as u64,
        distance_to_asserted: 250,
        location_trust_score_multiplier: 750,
        location_source: LocationSource::Skyhook as i32,
        ..Default::default()
    }
}

fn truncated_now() -> DateTime<Utc> {
    Utc::now().trunc_subsecs(3)
}

fn make_validated_heartbeat(index: u8) -> ValidatedHeartbeat {
    let now = truncated_now();
    let device_types = [
        DeviceType::Cbrs,
        DeviceType::WifiIndoor,
        DeviceType::WifiOutdoor,
        DeviceType::WifiDataOnly,
    ];
    let device_type = device_types[index as usize % device_types.len()];

    ValidatedHeartbeat {
        heartbeat: Heartbeat {
            hotspot_key: PublicKeyBinary::from(vec![index, index, index]),
            operation_mode: true,
            lat: 37.0 + f64::from(index),
            lon: -122.0 + f64::from(index),
            coverage_object: Some(Uuid::new_v4()),
            location_validation_timestamp: Some(now - chrono::Duration::hours(1)),
            location_source: LocationSource::Skyhook,
            timestamp: now,
            heartbeat_timestamp: now - chrono::Duration::seconds(30),
        },
        cell_type: CellType::NovaGenericWifiIndoor,
        location_trust_score_multiplier: dec!(0.75),
        distance_to_asserted: Some(i64::from(index) * 100),
        asserted_location: Some(0x8a1fb466d2dffff),
        device_type: Some(device_type),
        coverage_meta: None,
        validity: HeartbeatValidity::Valid,
    }
}

#[tokio::test]
async fn write_single_heartbeat_with_all_fields() -> anyhow::Result<()> {
    let harness = crate::common::setup_iceberg().await?;
    let writer = harness
        .get_table_writer::<IcebergHeartbeat>(iceberg::heartbeat::TABLE_NAME)
        .await?;

    let vhb = make_validated_heartbeat(1);
    let iceberg_hb = IcebergHeartbeat::from(&vhb);

    writer
        .write_idempotent("test_single", vec![iceberg_hb.clone()])
        .await?;

    let trino = harness.trino();
    let all = iceberg::heartbeat::get_all(trino).await?;

    assert_eq!(all, vec![iceberg_hb]);

    Ok(())
}

#[tokio::test]
async fn write_heartbeat_with_no_optional_fields() -> anyhow::Result<()> {
    let harness = crate::common::setup_iceberg().await?;
    let writer = harness
        .get_table_writer::<IcebergHeartbeat>(iceberg::heartbeat::TABLE_NAME)
        .await?;

    let now = truncated_now();
    let vhb = ValidatedHeartbeat {
        heartbeat: Heartbeat {
            hotspot_key: PublicKeyBinary::from(vec![10, 20, 30]),
            operation_mode: true,
            lat: 40.0,
            lon: -74.0,
            coverage_object: None,
            location_validation_timestamp: None,
            location_source: LocationSource::Gps,
            timestamp: now,
            heartbeat_timestamp: now - chrono::Duration::seconds(10),
        },
        cell_type: CellType::NovaGenericWifiIndoor,
        location_trust_score_multiplier: dec!(1.0),
        distance_to_asserted: None,
        asserted_location: None,
        device_type: None,
        coverage_meta: None,
        validity: HeartbeatValidity::Valid,
    };

    let iceberg_hb = IcebergHeartbeat::from(&vhb);

    writer
        .write_idempotent("test_no_optionals", vec![iceberg_hb.clone()])
        .await?;

    let trino = harness.trino();
    let all = iceberg::heartbeat::get_all(trino).await?;

    assert_eq!(all.len(), 1);
    assert_eq!(all[0], iceberg_hb);

    Ok(())
}

#[tokio::test]
async fn write_multiple_heartbeats() -> anyhow::Result<()> {
    let harness = crate::common::setup_iceberg().await?;
    let writer = harness
        .get_table_writer::<IcebergHeartbeat>(iceberg::heartbeat::TABLE_NAME)
        .await?;

    let heartbeats: Vec<IcebergHeartbeat> = (0..5)
        .map(|i| IcebergHeartbeat::from(&make_validated_heartbeat(i)))
        .collect();

    writer.write_idempotent("test_multiple", heartbeats).await?;

    let trino = harness.trino();
    let all = iceberg::heartbeat::get_all(trino).await?;

    assert_eq!(all.len(), 5);

    Ok(())
}

#[tokio::test]
async fn write_heartbeats_all_device_types() -> anyhow::Result<()> {
    let harness = crate::common::setup_iceberg().await?;
    let writer = harness
        .get_table_writer::<IcebergHeartbeat>(iceberg::heartbeat::TABLE_NAME)
        .await?;

    let device_types = [
        (DeviceType::Cbrs, "cbrs"),
        (DeviceType::WifiIndoor, "wifi_indoor"),
        (DeviceType::WifiOutdoor, "wifi_outdoor"),
        (DeviceType::WifiDataOnly, "wifi_data_only"),
    ];

    let now = truncated_now();
    let heartbeats: Vec<IcebergHeartbeat> = device_types
        .into_iter()
        .enumerate()
        .map(|(i, (dt, _))| {
            let vhb = ValidatedHeartbeat {
                heartbeat: Heartbeat {
                    hotspot_key: PublicKeyBinary::from(vec![i as u8 + 50]),
                    operation_mode: true,
                    lat: 37.0,
                    lon: -122.0,
                    coverage_object: Some(Uuid::new_v4()),
                    location_validation_timestamp: None,
                    location_source: LocationSource::Skyhook,
                    timestamp: now,
                    heartbeat_timestamp: now - chrono::Duration::seconds(5),
                },
                cell_type: CellType::NovaGenericWifiIndoor,
                location_trust_score_multiplier: dec!(1.0),
                distance_to_asserted: None,
                asserted_location: None,
                device_type: Some(dt),
                coverage_meta: None,
                validity: HeartbeatValidity::Valid,
            };
            IcebergHeartbeat::from(&vhb)
        })
        .collect();

    writer
        .write_idempotent("test_device_types", heartbeats)
        .await?;

    let trino = harness.trino();
    let all = iceberg::heartbeat::get_all(trino).await?;

    assert_eq!(all.len(), 4);

    // Verify each device type roundtripped by checking that the serialized
    // JSON of each row contains the expected device_type string
    let all_json: Vec<String> = all
        .iter()
        .map(|h| serde_json::to_string(h).unwrap())
        .collect();
    for (_, expected_str) in &device_types {
        assert!(
            all_json.iter().any(|json| json.contains(expected_str)),
            "expected device_type {expected_str} not found in results"
        );
    }

    Ok(())
}

#[tokio::test]
async fn maybe_write_idempotent_handles_some_and_none() -> anyhow::Result<()> {
    let harness = crate::common::setup_iceberg().await?;
    let writer = harness
        .get_table_writer::<IcebergHeartbeat>(iceberg::heartbeat::TABLE_NAME)
        .await?;

    let vhb = make_validated_heartbeat(2);
    let iceberg_hb = IcebergHeartbeat::from(&vhb);

    iceberg::maybe_write_idempotent(Some(&writer), "test_maybe", vec![iceberg_hb.clone()]).await?;

    let trino = harness.trino();
    let all = iceberg::heartbeat::get_all(trino).await?;
    assert_eq!(all.len(), 1);
    assert_eq!(all[0], iceberg_hb);

    // None path — should produce no error and no additional rows
    iceberg::maybe_write_idempotent::<IcebergHeartbeat>(None, "test_maybe_none", vec![]).await?;

    let all_after = iceberg::heartbeat::get_all(trino).await?;
    assert_eq!(all_after.len(), 1);

    Ok(())
}

#[tokio::test]
async fn empty_write_produces_no_rows() -> anyhow::Result<()> {
    let harness = crate::common::setup_iceberg().await?;
    let writer = harness
        .get_table_writer::<IcebergHeartbeat>(iceberg::heartbeat::TABLE_NAME)
        .await?;

    writer.write_idempotent("test_empty", vec![]).await?;

    let trino = harness.trino();
    let all = iceberg::heartbeat::get_all(trino).await?;

    assert_eq!(all.len(), 0);

    Ok(())
}

// ── Backfill tests (DB needed for file-tracking state) ────────────────────────

#[sqlx::test]
async fn backfill_writes_validated_heartbeats_to_iceberg(pool: PgPool) -> anyhow::Result<()> {
    let harness = crate::common::setup_iceberg().await?;
    let (writer, writer_task, _spool_dir) = crate::common::make_batched_writer::<IcebergHeartbeat>(
        &harness,
        iceberg::heartbeat::table_definition()?,
    )
    .await?;

    let awsl = AwsLocal::new().await;
    awsl.create_bucket().await?;

    let base_time = Utc::now() - Duration::hours(1);
    let start_time = base_time - Duration::minutes(1);
    let file1_time = base_time;
    let file2_time = base_time + Duration::minutes(5);
    let end_time = base_time + Duration::days(42);

    awsl.put_protos_at_time(
        FileType::ValidatedHeartbeat.to_string(),
        vec![make_validated_heartbeat_proto(
            file1_time,
            HeartbeatValidity::Valid,
        )],
        file1_time,
    )
    .await?;

    awsl.put_protos_at_time(
        FileType::ValidatedHeartbeat.to_string(),
        vec![make_validated_heartbeat_proto(
            file2_time,
            HeartbeatValidity::Valid,
        )],
        file2_time,
    )
    .await?;

    let opts = test_backfill_options("test-heartbeat-backfill", start_time, end_time);
    let (backfiller, server) =
        HeartbeatBackfiller::create_batched(pool, awsl.bucket_client(), writer, opts).await?;

    tokio::time::timeout(
        TEST_TIMEOUT,
        task_manager::TaskManager::builder()
            .add_task(writer_task)
            .add_task(server)
            .add_task(backfiller)
            .build()
            .start(),
    )
    .await
    .map_err(|_| anyhow::anyhow!("backfill timed out after {:?}", TEST_TIMEOUT))??;

    let rows = iceberg::heartbeat::get_all(harness.trino()).await?;
    assert_eq!(rows.len(), 2, "expected 2 heartbeats in iceberg");

    awsl.cleanup().await?;
    Ok(())
}

#[sqlx::test]
async fn backfill_filters_invalid_heartbeats(pool: PgPool) -> anyhow::Result<()> {
    let harness = crate::common::setup_iceberg().await?;
    let (writer, writer_task, _spool_dir) = crate::common::make_batched_writer::<IcebergHeartbeat>(
        &harness,
        iceberg::heartbeat::table_definition()?,
    )
    .await?;

    let awsl = AwsLocal::new().await;
    awsl.create_bucket().await?;

    let base_time = Utc::now() - Duration::hours(1);
    let start_time = base_time - Duration::minutes(1);
    let file_time = base_time;
    let end_time = base_time + Duration::days(42);

    awsl.put_protos_at_time(
        FileType::ValidatedHeartbeat.to_string(),
        vec![
            make_validated_heartbeat_proto(file_time, HeartbeatValidity::Valid),
            make_validated_heartbeat_proto(file_time, HeartbeatValidity::HeartbeatOutsideRange),
            make_validated_heartbeat_proto(file_time, HeartbeatValidity::GatewayNotFound),
        ],
        file_time,
    )
    .await?;

    let opts = test_backfill_options("test-heartbeat-backfill-filter", start_time, end_time);
    let (backfiller, server) =
        HeartbeatBackfiller::create_batched(pool, awsl.bucket_client(), writer, opts).await?;

    tokio::time::timeout(
        TEST_TIMEOUT,
        task_manager::TaskManager::builder()
            .add_task(writer_task)
            .add_task(server)
            .add_task(backfiller)
            .build()
            .start(),
    )
    .await
    .map_err(|_| anyhow::anyhow!("backfill timed out after {:?}", TEST_TIMEOUT))??;

    let rows = iceberg::heartbeat::get_all(harness.trino()).await?;
    assert_eq!(
        rows.len(),
        1,
        "only valid heartbeats should be written to iceberg"
    );

    awsl.cleanup().await?;
    Ok(())
}
