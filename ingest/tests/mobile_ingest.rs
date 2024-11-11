use helium_crypto::PublicKeyBinary;
use std::str::FromStr;

mod common;

const PUBKEY1: &str = "113HRxtzxFbFUjDEJJpyeMRZRtdAW38LAUnB5mshRwi6jt7uFbt";

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
    let cbsd_id = "cbsd_id".to_string();
    const SERVICE_PROVIDER_USER_COUNT: u64 = 10;
    const DISCO_MAPPING_USER_COUNT: u64 = 11;
    const OFFLOAD_USER_COUNT: u64 = 12;
    const SERVICE_PROVIDER_TRANSFER_BYTES: u64 = 13;
    const OFFLOAD_TRANSFER_BYTES: u64 = 14;

    let res = client
        .submit_radio_usage_req(
            hotspot_pubkey.clone(),
            cbsd_id.clone(),
            SERVICE_PROVIDER_USER_COUNT,
            DISCO_MAPPING_USER_COUNT,
            OFFLOAD_USER_COUNT,
            SERVICE_PROVIDER_TRANSFER_BYTES,
            OFFLOAD_TRANSFER_BYTES,
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
                    assert_eq!(cbsd_id, event.cbsd_id);
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
