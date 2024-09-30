use helium_proto::services::poc_mobile::{RadioLocationEstimateV1, RleEventV1};
use rust_decimal::prelude::*;

mod common;

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

    match client.recv_subscriber_mapping().await {
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
async fn submit_radio_location_estimates() -> anyhow::Result<()> {
    let (mut client, trigger) = common::setup_mobile().await?;

    let radio_id = "radio_id".to_string();
    let estimates = vec![RadioLocationEstimateV1 {
        radius: to_proto_decimal(2.0),
        confidence: to_proto_decimal(0.75),
        events: vec![RleEventV1 {
            id: "event_1".to_string(),
            timestamp: 0,
        }],
    }];

    let res = client
        .submit_radio_location_estimates(radio_id.clone(), estimates.clone())
        .await;

    assert!(res.is_ok());

    let timestamp: String = res.unwrap().id;

    match client.recv_radio_location_estimates().await {
        Ok(report) => {
            assert_eq!(timestamp, report.received_timestamp.to_string());

            match report.report {
                None => panic!("No report found"),
                Some(req) => {
                    assert_eq!(radio_id, req.radio_id);
                    assert_eq!(estimates, req.estimates);
                }
            }
        }
        Err(e) => panic!("got error {e}"),
    }

    trigger.trigger();
    Ok(())
}

fn to_proto_decimal(x: f64) -> Option<helium_proto::Decimal> {
    let d = Decimal::from_f64(x).unwrap();
    Some(helium_proto::Decimal {
        value: d.to_string(),
    })
}
