use h3o::LatLng;
use helium_crypto::{KeyTag, Keypair, PublicKey};
use helium_proto::services::poc_mobile::{
    radio_location_estimates_req_v1::Entity, RadioLocationCorrelationV1, RadioLocationEstimateV1,
    RadioLocationEstimatesReqV1,
};
use rand::rngs::OsRng;
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

    let key_pair = Keypair::generate(KeyTag::default(), &mut OsRng);
    let public_key = key_pair.public_key();
    let hex = LatLng::new(41.41208, -122.19288)
        .unwrap()
        .to_cell(h3o::Resolution::Twelve);
    let estimates = vec![RadioLocationEstimateV1 {
        hex: u64::from(hex),
        grid_distance: 2,
        confidence: to_proto_decimal(0.75),
        radio_location_correlations: vec![RadioLocationCorrelationV1 {
            id: "event_1".to_string(),
            timestamp: 0,
        }],
    }];

    let res = client
        .submit_radio_location_estimates(public_key, estimates.clone())
        .await;

    assert!(res.is_ok());

    let timestamp: String = res.unwrap().id;

    match client.recv_radio_location_estimates().await {
        Ok(report) => {
            assert_eq!(timestamp, report.received_timestamp.to_string());

            match report.report {
                None => panic!("No report found"),
                Some(req) => {
                    let req_public_key = wifi_public_key(req.clone())?;
                    assert_eq!(public_key.to_string(), req_public_key.to_string());
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

fn wifi_public_key(req: RadioLocationEstimatesReqV1) -> anyhow::Result<PublicKey> {
    let entity: Entity = req.entity.unwrap();
    let Entity::WifiPubKey(public_key_bytes) = entity.clone() else {
        anyhow::bail!("not WifiPubKey")
    };
    let public_key = PublicKey::from_bytes(&public_key_bytes)?;

    Ok(public_key)
}
