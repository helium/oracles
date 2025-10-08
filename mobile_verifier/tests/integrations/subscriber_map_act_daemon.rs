use async_trait::async_trait;
use aws_local::{gen_bucket_name, AwsLocal};
use chrono::DateTime;
use file_store::{
    file_info_poller::LookbackBehavior,
    file_sink::{FileSinkClient, Message},
    file_source, FileType,
};
use helium_crypto::PublicKeyBinary;
use helium_proto::services::{
    mobile_config::NetworkKeyRole,
    poc_mobile::{
        SubscriberMappingActivityIngestReportV1, SubscriberMappingActivityReqV1,
        SubscriberReportVerificationStatus, SubscriberVerifiedMappingEventVerificationStatus,
    },
};
use mobile_config::client::{
    authorization_client::AuthorizationVerifier, entity_client::EntityVerifier, ClientError,
};
use mobile_verifier::subscriber_mapping_activity::SubscriberMappingActivityDaemon;

use sqlx::PgPool;
use std::string::ToString;
use triggered::trigger;

struct AuthVer;
#[async_trait]
impl AuthorizationVerifier for AuthVer {
    async fn verify_authorized_key(
        &self,
        _: &PublicKeyBinary,
        _: NetworkKeyRole,
    ) -> Result<bool, ClientError> {
        Ok(true)
    }
}

struct EntVer;

#[async_trait]
impl EntityVerifier for EntVer {
    async fn verify_rewardable_entity(&self, _: &[u8]) -> Result<bool, ClientError> {
        Ok(true)
    }
}

pub async fn put_subscriber_reports_to_aws(
    awsl: &AwsLocal,
    reports: Vec<SubscriberMappingActivityIngestReportV1>,
) {
    awsl.put_proto_to_aws(
        reports,
        FileType::SubscriberMappingActivityIngestReport,
        concat!(
            env!("CARGO_PKG_NAME"),
            "_subscriber_mapping_activity_ingest_report"
        ),
    )
    .await
    .unwrap();
}

#[sqlx::test]
async fn test_process_map_act_ingest_report_file(pool: PgPool) {
    let bucket_name = gen_bucket_name();
    let endpoint = aws_local_default_endpoint();
    let awsl = AwsLocal::new(endpoint.as_str(), &bucket_name).await;

    let (stream_receiver, stream_server) = file_source::Continuous::prost_source()
        .state(pool.clone())
        .store(awsl.file_store.clone())
        .lookback(DateTime::UNIX_EPOCH)
        .prefix(FileType::SubscriberMappingActivityIngestReport.to_string())
        .poll_duration(std::time::Duration::from_millis(300))
        .create()
        .await
        .unwrap();

    let (tx, mut rx) = tokio::sync::mpsc::channel(10);
    let file_sink_client = FileSinkClient::new(tx, "nope");

    let subs_daemon = SubscriberMappingActivityDaemon::new(
        pool,
        AuthVer,
        EntVer,
        stream_receiver,
        file_sink_client,
    );
    let (_trigger, listener) = trigger();
    let act_req = {
        helium_proto::services::poc_mobile::SubscriberMappingActivityReqV1 {
            subscriber_id: vec![10],
            discovery_reward_shares: 2,
            verification_reward_shares: 3,
            timestamp: 4,
            carrier_pub_key: vec![11],
            signature: vec![12],
            reward_override_entity_key: "something".to_string(),
        }
    };
    put_subscriber_reports_to_aws(
        &awsl,
        vec![SubscriberMappingActivityIngestReportV1 {
            received_timestamp: 1,
            report: Some(act_req.clone()),
        }],
    )
    .await;

    let _future = stream_server.start(listener.clone()).await.unwrap();
    let _handle = tokio::spawn(subs_daemon.run(listener.clone()));

    let res = rx.recv().await.unwrap();
    match res {
        Message::Data(_, ref verifier_map_act) => {
            assert_eq!(
                verifier_map_act.status(),
                SubscriberReportVerificationStatus::Valid
            );
            let report = verifier_map_act.report.clone().unwrap().report.unwrap();
            dbg!(&report);
            assert_eq!(report, act_req)
        }
        _ => panic!("Expected Data message, got {:?}", res),
    };
}
