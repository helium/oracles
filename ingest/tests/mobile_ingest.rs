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

    let hex = 0;
    let helium_mobile_subscriber_avg_count = 10;
    let helium_mobile_disco_mapping_avg_count = 11;
    let offload_avg_count = 12;
    let tmo_cell_avg_count = 13;

    let res = client
        .submit_hex_usage_req(
            hex,
            helium_mobile_subscriber_avg_count,
            helium_mobile_disco_mapping_avg_count,
            offload_avg_count,
            tmo_cell_avg_count,
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
                    assert_eq!(hex, event.hex);
                    assert_eq!(
                        helium_mobile_subscriber_avg_count,
                        event.helium_mobile_subscriber_avg_count
                    );
                    assert_eq!(
                        helium_mobile_disco_mapping_avg_count,
                        event.helium_mobile_disco_mapping_avg_count
                    );
                    assert_eq!(offload_avg_count, event.offload_avg_count);
                    assert_eq!(tmo_cell_avg_count, event.tmo_cell_avg_count);
                }
            }
        }
        Err(e) => panic!("got error {e}"),
    }

    trigger.trigger();
    Ok(())
}

#[tokio::test]
async fn submit_hotspot_usage_report() -> anyhow::Result<()> {
    let (mut client, trigger) = common::setup_mobile().await?;

    let hotspot_pubkey = PublicKeyBinary::from_str(PUBKEY1)?;
    let cbsd_id = "cbsd_id".to_string();
    let helium_mobile_subscriber_avg_count = 10;
    let helium_mobile_disco_mapping_avg_count = 11;
    let offload_avg_count = 12;

    let res = client
        .submit_hotspot_usage_req(
            hotspot_pubkey.clone(),
            cbsd_id.clone(),
            helium_mobile_subscriber_avg_count,
            helium_mobile_disco_mapping_avg_count,
            offload_avg_count,
        )
        .await;

    assert!(res.is_ok());

    let timestamp: String = res.unwrap().id;

    match client.hotspot_usage_recv().await {
        Ok(report) => {
            assert_eq!(timestamp, report.received_timestamp.to_string());

            match report.report {
                None => panic!("No report found"),
                Some(event) => {
                    assert_eq!(hotspot_pubkey.as_ref(), event.hotspot_pubkey);
                    assert_eq!(cbsd_id, event.cbsd_id);
                    assert_eq!(
                        helium_mobile_subscriber_avg_count,
                        event.helium_mobile_subscriber_avg_count
                    );
                    assert_eq!(
                        helium_mobile_disco_mapping_avg_count,
                        event.helium_mobile_disco_mapping_avg_count
                    );
                    assert_eq!(offload_avg_count, event.offload_avg_count);
                }
            }
        }
        Err(e) => panic!("got error {e}"),
    }

    trigger.trigger();
    Ok(())
}
