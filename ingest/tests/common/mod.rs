use anyhow::bail;
use backon::{ExponentialBuilder, Retryable};
use file_store::file_sink::FileSinkClient;
use helium_crypto::{KeyTag, Keypair, Network, Sign};
use helium_proto::services::poc_mobile::{
    Client as PocMobileClient, SubscriberVerifiedMappingEventIngestReportV1,
    SubscriberVerifiedMappingEventReqV1, SubscriberVerifiedMappingEventResV1,
};
use ingest::server_mobile::GrpcServer;
use prost::Message;
use rand::rngs::OsRng;
use std::{net::SocketAddr, sync::Arc, time::Duration};
use tokio::{net::TcpListener, sync::mpsc::Receiver, time::timeout};
use tonic::{
    metadata::{Ascii, MetadataValue},
    transport::Channel,
    Request,
};
use triggered::Trigger;

pub async fn setup_mobile() -> anyhow::Result<(TestClient, Trigger)> {
    let key_pair = generate_keypair();

    let socket_addr = {
        let tcp_listener = TcpListener::bind("127.0.0.1:0").await?;
        tcp_listener.local_addr()?
    };

    let token = "api_token";
    let api_token = format!("Bearer {token}")
        .parse::<MetadataValue<_>>()
        .ok()
        .unwrap();

    let (trigger, listener) = triggered::trigger();

    let (cbrs_heartbeat_tx, _rx) = tokio::sync::mpsc::channel(10);
    let (wifi_heartbeat_tx, _rx) = tokio::sync::mpsc::channel(10);
    let (speedtest_tx, _rx) = tokio::sync::mpsc::channel(10);
    let (data_transfer_tx, _rx) = tokio::sync::mpsc::channel(10);
    let (subscriber_location_tx, _rx) = tokio::sync::mpsc::channel(10);
    let (radio_threshold_tx, _rx) = tokio::sync::mpsc::channel(10);
    let (invalidated_threshold_tx, _rx) = tokio::sync::mpsc::channel(10);
    let (coverage_obj_tx, _rx) = tokio::sync::mpsc::channel(10);
    let (sp_boosted_tx, _rx) = tokio::sync::mpsc::channel(10);
    let (subscriber_mapping_tx, subscriber_mapping_rx) = tokio::sync::mpsc::channel(10);
    let (promotion_rewards_tx, _rx) = tokio::sync::mpsc::channel(10);

    tokio::spawn(async move {
        let grpc_server = GrpcServer::new(
            FileSinkClient::new(cbrs_heartbeat_tx, "noop"),
            FileSinkClient::new(wifi_heartbeat_tx, "noop"),
            FileSinkClient::new(speedtest_tx, "noop"),
            FileSinkClient::new(data_transfer_tx, "noop"),
            FileSinkClient::new(subscriber_location_tx, "noop"),
            FileSinkClient::new(radio_threshold_tx, "noop"),
            FileSinkClient::new(invalidated_threshold_tx, "noop"),
            FileSinkClient::new(coverage_obj_tx, "noop"),
            FileSinkClient::new(sp_boosted_tx, "noop"),
            FileSinkClient::new(subscriber_mapping_tx, "test_file_sink"),
            FileSinkClient::new(promotion_rewards_tx, "noop"),
            Network::MainNet,
            socket_addr,
            api_token,
        );

        grpc_server.run(listener).await
    });

    let client = TestClient::new(
        socket_addr,
        key_pair,
        token.to_string(),
        subscriber_mapping_rx,
    )
    .await;

    Ok((client, trigger))
}

pub struct TestClient {
    client: PocMobileClient<Channel>,
    key_pair: Arc<Keypair>,
    authorization: MetadataValue<Ascii>,
    file_sink_rx:
        Receiver<file_store::file_sink::Message<SubscriberVerifiedMappingEventIngestReportV1>>,
}

impl TestClient {
    pub async fn new(
        socket_addr: SocketAddr,
        key_pair: Keypair,
        api_token: String,
        file_sink_rx: Receiver<
            file_store::file_sink::Message<SubscriberVerifiedMappingEventIngestReportV1>,
        >,
    ) -> TestClient {
        let client = (|| PocMobileClient::connect(format!("http://{socket_addr}")))
            .retry(&ExponentialBuilder::default())
            .await
            .expect("client connect");

        TestClient {
            client,
            key_pair: Arc::new(key_pair),
            authorization: format!("Bearer {}", api_token).try_into().unwrap(),
            file_sink_rx,
        }
    }

    pub async fn recv(mut self) -> anyhow::Result<SubscriberVerifiedMappingEventIngestReportV1> {
        match timeout(Duration::from_secs(2), self.file_sink_rx.recv()).await {
            Ok(Some(msg)) => match msg {
                file_store::file_sink::Message::Commit(_) => bail!("got Commit"),
                file_store::file_sink::Message::Rollback(_) => bail!("got Rollback"),
                file_store::file_sink::Message::Data(_, data) => Ok(data),
            },
            Ok(None) => bail!("got none"),
            Err(reason) => bail!("got error {reason}"),
        }
    }

    pub async fn submit_verified_subscriber_mapping_event(
        &mut self,
        subscriber_id: Vec<u8>,
        total_reward_points: u64,
    ) -> anyhow::Result<SubscriberVerifiedMappingEventResV1> {
        let mut req = SubscriberVerifiedMappingEventReqV1 {
            subscriber_id,
            total_reward_points,
            timestamp: 0,
            carrier_mapping_key: self.key_pair.public_key().to_vec(),
            signature: vec![],
        };

        req.signature = self.key_pair.sign(&req.encode_to_vec()).expect("sign");

        let mut request = Request::new(req);
        let metadata = request.metadata_mut();

        metadata.insert("authorization", self.authorization.clone());

        let res = self
            .client
            .submit_subscriber_verified_mapping_event(request)
            .await?;

        Ok(res.into_inner())
    }
}

pub fn generate_keypair() -> Keypair {
    Keypair::generate(KeyTag::default(), &mut OsRng)
}
