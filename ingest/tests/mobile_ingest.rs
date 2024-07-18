use helium_proto::services::poc_mobile::VerifiedSubscriberMappingEventIngestReportV1;
use prost::Message;

mod common;

#[tokio::test]
async fn submit_verified_subscriber_mapping_event() -> anyhow::Result<()> {
    let (mut client, file_sink_rx, trigger) = common::setup_mobile().await?;

    let subscriber_id = vec![0];
    let total_reward_points = 100;

    let res = client
        .submit_verified_subscriber_mapping_event(subscriber_id.clone(), total_reward_points)
        .await;

    assert!(res.is_ok());

    let timestamp: String = res.unwrap().id;

    match common::recv(file_sink_rx).await {
        Ok(data) => {
            let report = VerifiedSubscriberMappingEventIngestReportV1::decode(data.as_slice())
                .expect("unable to decode into VerifiedSubscriberMappingEventIngestReportV1");

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
