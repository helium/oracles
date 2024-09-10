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
    const HELIUM_MOBILE_SUBSCRIBER_AVG_COUNT: u64 = 10;
    const HELIUM_MOBILE_DISCO_MAPPING_AVG_COUNT: u64 = 11;
    const OFFLOAD_AVG_COUNT: u64 = 12;
    const TMO_CELL_AVG_COUNT: u64 = 13;

    let res = client
        .submit_hex_usage_req(
            HEX,
            HELIUM_MOBILE_SUBSCRIBER_AVG_COUNT,
            HELIUM_MOBILE_DISCO_MAPPING_AVG_COUNT,
            OFFLOAD_AVG_COUNT,
            TMO_CELL_AVG_COUNT,
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
                        HELIUM_MOBILE_SUBSCRIBER_AVG_COUNT,
                        event.helium_mobile_subscriber_avg_count
                    );
                    assert_eq!(
                        HELIUM_MOBILE_DISCO_MAPPING_AVG_COUNT,
                        event.helium_mobile_disco_mapping_avg_count
                    );
                    assert_eq!(OFFLOAD_AVG_COUNT, event.offload_avg_count);
                    assert_eq!(TMO_CELL_AVG_COUNT, event.tmo_cell_avg_count);
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
    const HELIUM_MOBILE_SUBSCRIBER_AVG_COUNT: u64 = 10;
    const HELIUM_MOBILE_DISCO_MAPPING_AVG_COUNT: u64 = 11;
    const OFFLOAD_AVG_COUNT: u64 = 12;

    let res = client
        .submit_radio_usage_req(
            hotspot_pubkey.clone(),
            cbsd_id.clone(),
            HELIUM_MOBILE_SUBSCRIBER_AVG_COUNT,
            HELIUM_MOBILE_DISCO_MAPPING_AVG_COUNT,
            OFFLOAD_AVG_COUNT,
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
                        HELIUM_MOBILE_SUBSCRIBER_AVG_COUNT,
                        event.helium_mobile_subscriber_avg_count
                    );
                    assert_eq!(
                        HELIUM_MOBILE_DISCO_MAPPING_AVG_COUNT,
                        event.helium_mobile_disco_mapping_avg_count
                    );
                    assert_eq!(OFFLOAD_AVG_COUNT, event.offload_avg_count);
                }
            }
        }
        Err(e) => panic!("got error {e}"),
    }

    trigger.trigger();
    Ok(())
}
