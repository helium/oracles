use chrono::{TimeZone, Utc};
use common::generate_keypair;
use helium_crypto::PublicKeyBinary;
use helium_proto::services::poc_mobile::{
    DataTransferRadioAccessTechnology, RadioUsageCarrierTransferInfo,
};
use std::str::FromStr;

mod common;

const PUBKEY1: &str = "113HRxtzxFbFUjDEJJpyeMRZRtdAW38LAUnB5mshRwi6jt7uFbt";

#[tokio::test]
async fn submit_ban() -> anyhow::Result<()> {
    let (mut client, trigger) = common::setup_mobile().await?;

    let pubkey = PublicKeyBinary::from_str(PUBKEY1)?;
    let response = client.submit_ban(pubkey.clone().into()).await?;

    let report = client.ban_recv().await?;
    assert_eq!(report.received_timestamp_ms, response.timestamp_ms);

    let inner_report = report.report.expect("inner report");
    assert_eq!(PublicKeyBinary::from(inner_report.hotspot_pubkey), pubkey);

    trigger.trigger();
    Ok(())
}

#[tokio::test]
async fn submit_unique_connections() -> anyhow::Result<()> {
    let (mut client, trigger) = common::setup_mobile().await?;

    let pubkey = PublicKeyBinary::from_str(PUBKEY1)?;
    let timestamp = Utc::now();
    let end = timestamp - chrono::Duration::days(1);
    let start = end - chrono::Duration::days(7);

    const UNIQUE_CONNECTIONS: u64 = 42;

    let response = client
        .submit_unique_connections(pubkey.into(), start, end, UNIQUE_CONNECTIONS)
        .await?;

    let report = client.unique_connection_recv().await?;

    let Some(inner_report) = report.report else {
        anyhow::bail!("No report found")
    };

    assert_eq!(inner_report.timestamp, response.timestamp);
    assert_eq!(inner_report.unique_connections, UNIQUE_CONNECTIONS);

    trigger.trigger();

    Ok(())
}

#[tokio::test]
async fn submit_verified_subscriber_mapping_event() -> anyhow::Result<()> {
    let (mut client, trigger) = common::setup_mobile().await?;

    let subscriber_id = vec![0];
    let total_reward_points = 100;

    let res = client
        .submit_verified_subscriber_mapping_event(subscriber_id.clone(), total_reward_points)
        .await;

    assert!(res.is_ok());

    let timestamp: String = res.unwrap().id;

    match client.subscriber_mapping_recv().await {
        Ok(report) => {
            assert_eq!(timestamp, report.received_timestamp.to_string());

            match report.report {
                None => panic!("No report found"),
                Some(event) => {
                    assert_eq!(subscriber_id, event.subscriber_id);
                    assert_eq!(total_reward_points, event.total_reward_points);
                }
            }
        }
        Err(e) => panic!("got error {e}"),
    }

    trigger.trigger();
    Ok(())
}

#[tokio::test]
async fn submit_hex_usage_report() -> anyhow::Result<()> {
    let (mut client, trigger) = common::setup_mobile().await?;

    const HEX: u64 = 360;
    const SERVICE_PROVIDER_USER_COUNT: u64 = 10;
    const DISCO_MAPPING_USER_COUNT: u64 = 11;
    const OFFLOAD_USER_COUNT: u64 = 12;
    const SERVICE_PROVIDER_TRANSFER_BYTES: u64 = 13;
    const OFFLOAD_TRANSFER_BYTES: u64 = 14;

    let res = client
        .submit_hex_usage_req(
            HEX,
            SERVICE_PROVIDER_USER_COUNT,
            DISCO_MAPPING_USER_COUNT,
            OFFLOAD_USER_COUNT,
            SERVICE_PROVIDER_TRANSFER_BYTES,
            OFFLOAD_TRANSFER_BYTES,
        )
        .await;

    assert!(res.is_ok());

    let timestamp: String = res.unwrap().id;

    match client.hex_usage_recv().await {
        Ok(report) => {
            assert_eq!(timestamp, report.received_timestamp.to_string());

            match report.report {
                None => panic!("No report found"),
                Some(event) => {
                    assert_eq!(HEX, event.hex);
                    assert_eq!(
                        SERVICE_PROVIDER_USER_COUNT,
                        event.service_provider_user_count
                    );
                    assert_eq!(DISCO_MAPPING_USER_COUNT, event.disco_mapping_user_count);
                    assert_eq!(OFFLOAD_USER_COUNT, event.offload_user_count);
                    assert_eq!(
                        SERVICE_PROVIDER_TRANSFER_BYTES,
                        event.service_provider_transfer_bytes
                    );
                    assert_eq!(OFFLOAD_TRANSFER_BYTES, event.offload_transfer_bytes);
                }
            }
        }
        Err(e) => panic!("got error {e}"),
    }

    trigger.trigger();
    Ok(())
}

#[tokio::test]
async fn submit_radio_usage_report() -> anyhow::Result<()> {
    let (mut client, trigger) = common::setup_mobile().await?;

    let hotspot_pubkey = PublicKeyBinary::from_str(PUBKEY1)?;
    const SERVICE_PROVIDER_USER_COUNT: u64 = 10;
    const DISCO_MAPPING_USER_COUNT: u64 = 11;
    const OFFLOAD_USER_COUNT: u64 = 12;
    const SERVICE_PROVIDER_TRANSFER_BYTES: u64 = 13;
    const OFFLOAD_TRANSFER_BYTES: u64 = 14;
    let radio_usage_carrier_info = RadioUsageCarrierTransferInfo {
        transfer_bytes: OFFLOAD_TRANSFER_BYTES,
        user_count: 2,
        carrier_id_v2: 2,
        ..Default::default()
    };

    let res = client
        .submit_radio_usage_req(
            hotspot_pubkey.clone(),
            SERVICE_PROVIDER_USER_COUNT,
            DISCO_MAPPING_USER_COUNT,
            OFFLOAD_USER_COUNT,
            SERVICE_PROVIDER_TRANSFER_BYTES,
            OFFLOAD_TRANSFER_BYTES,
            vec![radio_usage_carrier_info],
        )
        .await;

    assert!(res.is_ok());

    let timestamp: String = res.unwrap().id;

    match client.radio_usage_recv().await {
        Ok(report) => {
            assert_eq!(timestamp, report.received_timestamp.to_string());

            match report.report {
                None => panic!("No report found"),
                Some(event) => {
                    assert_eq!(hotspot_pubkey.as_ref(), event.hotspot_pubkey);
                    assert_eq!(
                        SERVICE_PROVIDER_USER_COUNT,
                        event.service_provider_user_count
                    );
                    assert_eq!(DISCO_MAPPING_USER_COUNT, event.disco_mapping_user_count);
                    assert_eq!(OFFLOAD_USER_COUNT, event.offload_user_count);
                    assert_eq!(
                        SERVICE_PROVIDER_TRANSFER_BYTES,
                        event.service_provider_transfer_bytes
                    );
                    assert_eq!(OFFLOAD_TRANSFER_BYTES, event.offload_transfer_bytes);
                    assert_eq!(OFFLOAD_TRANSFER_BYTES, event.offload_transfer_bytes);
                    assert_eq!(vec![radio_usage_carrier_info], event.carrier_transfer_info);
                }
            }
        }
        Err(e) => panic!("got error {e}"),
    }

    trigger.trigger();
    Ok(())
}

#[tokio::test]
async fn cell_heartbeat_after() {
    let (mut client, _trigger) = common::setup_mobile().await.unwrap();

    let keypair = generate_keypair();

    // Cell heartbeat is disabled but should return OK
    let res = client
        .submit_cell_heartbeat(&keypair, "cbsd-1")
        .await
        .unwrap();
    // Make sure response is parseable to a timestamp
    // And it is close to the request time
    let resp_sec = Utc
        .timestamp_millis_opt(res.id.parse::<i64>().unwrap())
        .unwrap();
    let now_sec = Utc::now();

    let diff = now_sec - resp_sec;
    assert!(diff.num_seconds() < 100);
}

#[tokio::test]
async fn wifi_data_transfer() -> anyhow::Result<()> {
    let (mut client, trigger) = common::setup_mobile().await?;

    let keypair = generate_keypair();

    client
        .submit_data_transfer(&keypair, DataTransferRadioAccessTechnology::Wlan)
        .await?;

    let ingest_report = client.data_transfer_recv().await?;

    let ingest_pubkey = ingest_report
        .report
        .unwrap()
        .data_transfer_usage
        .unwrap()
        .pub_key;

    assert_eq!(ingest_pubkey, keypair.public_key().to_vec());

    trigger.trigger();
    Ok(())
}

#[tokio::test]
async fn cbrs_data_transfer_after() -> anyhow::Result<()> {
    let (mut client, trigger) = common::setup_mobile().await?;

    let keypair = generate_keypair();

    client
        .submit_data_transfer(&keypair, DataTransferRadioAccessTechnology::Eutran)
        .await?;

    assert!(client.is_data_transfer_rx_empty()?);

    trigger.trigger();
    Ok(())
}
