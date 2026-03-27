use chrono::{DateTime, SubsecRound, Utc};
use helium_crypto::PublicKeyBinary;
use helium_proto::services::poc_mobile::{HeartbeatValidity, LocationSource};
use mobile_config::gateway::service::info::DeviceType;
use mobile_verifier::{
    cell_type::CellType,
    heartbeats::{HbType, Heartbeat, ValidatedHeartbeat},
    iceberg::{self, heartbeat::IcebergHeartbeat},
};
use rust_decimal_macros::dec;
use uuid::Uuid;

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
            hb_type: HbType::Wifi,
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

    let mut txn = writer.begin("test_single").await?;
    txn.write(vec![iceberg_hb.clone()]).await?;
    txn.publish().await?;

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
            hb_type: HbType::Wifi,
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

    let mut txn = writer.begin("test_no_optionals").await?;
    txn.write(vec![iceberg_hb.clone()]).await?;
    txn.publish().await?;

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

    let mut txn = writer.begin("test_multiple").await?;
    txn.write(heartbeats).await?;
    txn.publish().await?;

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
                    hb_type: HbType::Wifi,
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

    let mut txn = writer.begin("test_device_types").await?;
    txn.write(heartbeats).await?;
    txn.publish().await?;

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
async fn write_heartbeats_via_maybe_begin_maybe_publish() -> anyhow::Result<()> {
    let harness = crate::common::setup_iceberg().await?;
    let writer = harness
        .get_table_writer::<IcebergHeartbeat>(iceberg::heartbeat::TABLE_NAME)
        .await?;

    let vhb = make_validated_heartbeat(2);
    let iceberg_hb = IcebergHeartbeat::from(&vhb);

    // Test with Some(writer) path
    let mut txn = iceberg::maybe_begin(Some(&writer), "test_maybe").await?;
    assert!(txn.is_some());
    txn.as_mut()
        .unwrap()
        .write(vec![iceberg_hb.clone()])
        .await?;
    iceberg::maybe_publish(txn).await?;

    let trino = harness.trino();
    let all = iceberg::heartbeat::get_all(trino).await?;
    assert_eq!(all.len(), 1);
    assert_eq!(all[0], iceberg_hb);

    // Test with None path — should produce no error and no additional rows
    let txn_none = iceberg::maybe_begin::<IcebergHeartbeat>(None, "test_maybe_none").await?;
    assert!(txn_none.is_none());
    iceberg::maybe_publish(txn_none).await?;

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

    let mut txn = writer.begin("test_empty").await?;
    txn.write(vec![]).await?;
    txn.publish().await?;

    let trino = harness.trino();
    let all = iceberg::heartbeat::get_all(trino).await?;

    assert_eq!(all.len(), 0);

    Ok(())
}
