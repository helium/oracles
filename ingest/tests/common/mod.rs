use anyhow::bail;
use backon::{ExponentialBuilder, Retryable};
use chrono::{DateTime, Utc};
use file_store::file_sink::FileSinkClient;
use file_store::mobile_ban::proto::{BanAction, BanDetailsV1, BanReason};
use helium_crypto::{KeyTag, Keypair, Network, PublicKeyBinary, Sign};
use helium_proto::services::poc_mobile::{
    BanIngestReportV1, BanReqV1, BanRespV1, CarrierIdV2, CellHeartbeatReqV1, CellHeartbeatRespV1,
    DataTransferEvent, DataTransferRadioAccessTechnology, DataTransferSessionIngestReportV1,
    DataTransferSessionReqV1, DataTransferSessionRespV1, HexUsageStatsIngestReportV1,
    HexUsageStatsReqV1, HexUsageStatsResV1, RadioUsageCarrierTransferInfo,
    RadioUsageStatsIngestReportV1, RadioUsageStatsReqV1, RadioUsageStatsResV1,
    UniqueConnectionsIngestReportV1, UniqueConnectionsReqV1, UniqueConnectionsRespV1,
};
use helium_proto::services::{
    mobile_config::NetworkKeyRole,
    poc_mobile::{
        Client as PocMobileClient, SubscriberVerifiedMappingEventIngestReportV1,
        SubscriberVerifiedMappingEventReqV1, SubscriberVerifiedMappingEventResV1,
    },
};
use ingest::server_mobile::GrpcServer;
use mobile_config::client::authorization_client::AuthorizationVerifier;
use mobile_config::client::ClientError;
use prost::Message;
use rand::rngs::OsRng;
use std::{net::SocketAddr, sync::Arc, time::Duration};
use tokio::sync::mpsc::error::TryRecvError;
use tokio::{net::TcpListener, sync::mpsc::Receiver, time::timeout};
use tonic::{
    async_trait,
    metadata::{Ascii, MetadataValue},
    transport::Channel,
    Request,
};
use triggered::Trigger;

struct MockAuthorizationClient;

#[async_trait]
impl AuthorizationVerifier for MockAuthorizationClient {
    async fn verify_authorized_key(
        &self,
        _pubkey: &PublicKeyBinary,
        _role: NetworkKeyRole,
    ) -> Result<bool, ClientError> {
        Ok(true)
    }
}
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

    let (wifi_heartbeat_tx, _rx) = tokio::sync::mpsc::channel(10);
    let (speedtest_tx, _rx) = tokio::sync::mpsc::channel(10);
    let (data_transfer_tx, data_transfer_rx) = tokio::sync::mpsc::channel(10);
    let (subscriber_location_tx, _rx) = tokio::sync::mpsc::channel(10);
    let (radio_threshold_tx, _rx) = tokio::sync::mpsc::channel(10);
    let (invalidated_threshold_tx, _rx) = tokio::sync::mpsc::channel(10);
    let (coverage_obj_tx, _rx) = tokio::sync::mpsc::channel(10);
    let (sp_boosted_tx, _rx) = tokio::sync::mpsc::channel(10);
    let (subscriber_mapping_tx, subscriber_mapping_rx) = tokio::sync::mpsc::channel(10);
    let (hex_usage_stat_tx, hex_usage_stat_rx) = tokio::sync::mpsc::channel(10);
    let (radio_usage_stat_tx, radio_usage_stat_rx) = tokio::sync::mpsc::channel(10);
    let (unique_connections_tx, unique_connections_rx) = tokio::sync::mpsc::channel(10);
    let (subscriber_mapping_activity_tx, _subscriber_mapping_activity_rx) =
        tokio::sync::mpsc::channel(10);
    let (ban_tx, ban_rx) = tokio::sync::mpsc::channel(10);

    tokio::spawn(async move {
        let grpc_server = GrpcServer::new(
            FileSinkClient::new(wifi_heartbeat_tx, "noop"),
            FileSinkClient::new(speedtest_tx, "noop"),
            FileSinkClient::new(data_transfer_tx, "noop"),
            FileSinkClient::new(subscriber_location_tx, "noop"),
            FileSinkClient::new(radio_threshold_tx, "noop"),
            FileSinkClient::new(invalidated_threshold_tx, "noop"),
            FileSinkClient::new(coverage_obj_tx, "noop"),
            FileSinkClient::new(sp_boosted_tx, "noop"),
            FileSinkClient::new(subscriber_mapping_tx, "test_file_sink"),
            FileSinkClient::new(hex_usage_stat_tx, "hex_usage_test_file_sink"),
            FileSinkClient::new(radio_usage_stat_tx, "radio_usage_test_file_sink"),
            FileSinkClient::new(unique_connections_tx, "noop"),
            FileSinkClient::new(subscriber_mapping_activity_tx, "noop"),
            FileSinkClient::new(ban_tx, "noop"),
            Network::MainNet,
            socket_addr,
            api_token,
            MockAuthorizationClient,
        );

        grpc_server.run(listener).await
    });

    let client = TestClient::new(
        socket_addr,
        key_pair,
        token.to_string(),
        subscriber_mapping_rx,
        hex_usage_stat_rx,
        radio_usage_stat_rx,
        unique_connections_rx,
        ban_rx,
        data_transfer_rx,
    )
    .await;

    Ok((client, trigger))
}

pub struct TestClient {
    client: PocMobileClient<Channel>,
    key_pair: Arc<Keypair>,
    authorization: MetadataValue<Ascii>,
    subscriber_mapping_file_sink_rx:
        Receiver<file_store::file_sink::Message<SubscriberVerifiedMappingEventIngestReportV1>>,
    hex_usage_stats_file_sink_rx:
        Receiver<file_store::file_sink::Message<HexUsageStatsIngestReportV1>>,
    radio_usage_stats_file_sink_rx:
        Receiver<file_store::file_sink::Message<RadioUsageStatsIngestReportV1>>,
    unique_connections_file_sink_rx:
        Receiver<file_store::file_sink::Message<UniqueConnectionsIngestReportV1>>,
    ban_file_sink_rx: Receiver<file_store::file_sink::Message<BanIngestReportV1>>,
    data_transfer_rx: Receiver<file_store::file_sink::Message<DataTransferSessionIngestReportV1>>,
}

impl TestClient {
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        socket_addr: SocketAddr,
        key_pair: Keypair,
        api_token: String,
        subscriber_mapping_file_sink_rx: Receiver<
            file_store::file_sink::Message<SubscriberVerifiedMappingEventIngestReportV1>,
        >,
        hex_usage_stats_file_sink_rx: Receiver<
            file_store::file_sink::Message<HexUsageStatsIngestReportV1>,
        >,
        radio_usage_stats_file_sink_rx: Receiver<
            file_store::file_sink::Message<RadioUsageStatsIngestReportV1>,
        >,
        unique_connections_file_sink_rx: Receiver<
            file_store::file_sink::Message<UniqueConnectionsIngestReportV1>,
        >,
        ban_file_sink_rx: Receiver<file_store::file_sink::Message<BanIngestReportV1>>,
        data_transfer_rx: Receiver<
            file_store::file_sink::Message<DataTransferSessionIngestReportV1>,
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
            subscriber_mapping_file_sink_rx,
            hex_usage_stats_file_sink_rx,
            radio_usage_stats_file_sink_rx,
            unique_connections_file_sink_rx,
            ban_file_sink_rx,
            data_transfer_rx,
        }
    }

    pub async fn data_transfer_recv(mut self) -> anyhow::Result<DataTransferSessionIngestReportV1> {
        match timeout(Duration::from_secs(2), self.data_transfer_rx.recv()).await {
            Ok(Some(msg)) => match msg {
                file_store::file_sink::Message::Data(_, data) => Ok(data),
                file_store::file_sink::Message::Commit(_) => bail!("got Commit"),
                file_store::file_sink::Message::Rollback(_) => bail!("got Rollback"),
            },
            Ok(None) => bail!("got none"),
            Err(reason) => bail!("got error {reason}"),
        }
    }

    pub fn is_data_transfer_rx_empty(&mut self) -> anyhow::Result<bool> {
        match self.data_transfer_rx.try_recv() {
            Ok(_) => Ok(false),
            Err(TryRecvError::Empty) => Ok(true),
            Err(err) => bail!(err),
        }
    }

    pub async fn unique_connection_recv(
        mut self,
    ) -> anyhow::Result<UniqueConnectionsIngestReportV1> {
        match timeout(
            Duration::from_secs(2),
            self.unique_connections_file_sink_rx.recv(),
        )
        .await
        {
            Ok(Some(msg)) => match msg {
                file_store::file_sink::Message::Data(_, data) => Ok(data),
                file_store::file_sink::Message::Commit(_) => bail!("got Commit"),
                file_store::file_sink::Message::Rollback(_) => bail!("got Rollback"),
            },
            Ok(None) => bail!("got none"),
            Err(reason) => bail!("got error {reason}"),
        }
    }

    pub async fn subscriber_mapping_recv(
        mut self,
    ) -> anyhow::Result<SubscriberVerifiedMappingEventIngestReportV1> {
        match timeout(
            Duration::from_secs(2),
            self.subscriber_mapping_file_sink_rx.recv(),
        )
        .await
        {
            Ok(Some(msg)) => match msg {
                file_store::file_sink::Message::Commit(_) => bail!("got Commit"),
                file_store::file_sink::Message::Rollback(_) => bail!("got Rollback"),
                file_store::file_sink::Message::Data(_, data) => Ok(data),
            },
            Ok(None) => bail!("got none"),
            Err(reason) => bail!("got error {reason}"),
        }
    }

    pub async fn hex_usage_recv(mut self) -> anyhow::Result<HexUsageStatsIngestReportV1> {
        match timeout(
            Duration::from_secs(2),
            self.hex_usage_stats_file_sink_rx.recv(),
        )
        .await
        {
            Ok(Some(msg)) => match msg {
                file_store::file_sink::Message::Commit(_) => bail!("got Commit"),
                file_store::file_sink::Message::Rollback(_) => bail!("got Rollback"),
                file_store::file_sink::Message::Data(_, data) => Ok(data),
            },
            Ok(None) => bail!("got none"),
            Err(reason) => bail!("got error {reason}"),
        }
    }

    pub async fn radio_usage_recv(mut self) -> anyhow::Result<RadioUsageStatsIngestReportV1> {
        match timeout(
            Duration::from_secs(2),
            self.radio_usage_stats_file_sink_rx.recv(),
        )
        .await
        {
            Ok(Some(msg)) => match msg {
                file_store::file_sink::Message::Commit(_) => bail!("got Commit"),
                file_store::file_sink::Message::Rollback(_) => bail!("got Rollback"),
                file_store::file_sink::Message::Data(_, data) => Ok(data),
            },
            Ok(None) => bail!("got none"),
            Err(reason) => bail!("got error {reason}"),
        }
    }

    pub async fn ban_recv(mut self) -> anyhow::Result<BanIngestReportV1> {
        match timeout(Duration::from_secs(2), self.ban_file_sink_rx.recv()).await {
            Ok(Some(msg)) => match msg {
                file_store::file_sink::Message::Commit(_) => bail!("got Commit"),
                file_store::file_sink::Message::Rollback(_) => bail!("got Rollback"),
                file_store::file_sink::Message::Data(_, data) => Ok(data),
            },
            Ok(None) => bail!("got none"),
            Err(reason) => bail!("got error {reason}"),
        }
    }

    pub async fn submit_ban(&mut self, hotspot_pubkey: Vec<u8>) -> anyhow::Result<BanRespV1> {
        use helium_proto::services::poc_mobile::BanType;
        let mut req = BanReqV1 {
            hotspot_pubkey,
            timestamp_ms: Utc::now().timestamp_millis() as u64,
            ban_pubkey: self.key_pair.public_key().into(),
            signature: vec![],
            ban_action: Some(BanAction::Ban(BanDetailsV1 {
                hotspot_serial: "test-serial".to_string(),
                message: "test ban".to_string(),
                reason: BanReason::LocationGaming.into(),
                ban_type: BanType::All.into(),
                expiration_timestamp_ms: 0,
            })),
        };

        req.signature = self.key_pair.sign(&req.encode_to_vec()).expect("sign");

        let mut request = Request::new(req);
        let metadata = request.metadata_mut();

        metadata.insert("authorization", self.authorization.clone());

        let response = self.client.submit_ban(request).await?;

        Ok(response.into_inner())
    }

    pub async fn submit_unique_connections(
        &mut self,
        pubkey: Vec<u8>,
        start_timestamp: DateTime<Utc>,
        end_timestamp: DateTime<Utc>,
        unique_connections: u64,
    ) -> anyhow::Result<UniqueConnectionsRespV1> {
        let mut req = UniqueConnectionsReqV1 {
            pubkey,
            start_timestamp: start_timestamp.timestamp_millis() as u64,
            end_timestamp: end_timestamp.timestamp_millis() as u64,
            unique_connections,
            timestamp: 0,
            carrier_key: self.key_pair.public_key().into(),
            signature: vec![],
        };

        req.signature = self.key_pair.sign(&req.encode_to_vec()).expect("sign");

        let mut request = Request::new(req);
        let metadata = request.metadata_mut();

        metadata.insert("authorization", self.authorization.clone());

        let res = self.client.submit_unique_connections(request).await?;

        Ok(res.into_inner())
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

    pub async fn submit_hex_usage_req(
        &mut self,
        hex: u64,
        service_provider_user_count: u64,
        disco_mapping_user_count: u64,
        offload_user_count: u64,
        service_provider_transfer_bytes: u64,
        offload_transfer_bytes: u64,
    ) -> anyhow::Result<HexUsageStatsResV1> {
        let mut req = HexUsageStatsReqV1 {
            hex,
            service_provider_user_count,
            disco_mapping_user_count,
            offload_user_count,
            service_provider_transfer_bytes,
            offload_transfer_bytes,
            epoch_start_timestamp: 0,
            epoch_end_timestamp: 0,
            timestamp: 0,
            carrier_mapping_key: self.key_pair.public_key().to_vec(),
            signature: vec![],
        };

        req.signature = self.key_pair.sign(&req.encode_to_vec()).expect("sign");

        let mut request = Request::new(req);
        let metadata = request.metadata_mut();

        metadata.insert("authorization", self.authorization.clone());

        let res = self.client.submit_hex_usage_stats_report(request).await?;

        Ok(res.into_inner())
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn submit_radio_usage_req(
        &mut self,
        hotspot_pubkey: PublicKeyBinary,
        service_provider_user_count: u64,
        disco_mapping_user_count: u64,
        offload_user_count: u64,
        service_provider_transfer_bytes: u64,
        offload_transfer_bytes: u64,
        carrier_transfer_info: Vec<RadioUsageCarrierTransferInfo>,
    ) -> anyhow::Result<RadioUsageStatsResV1> {
        let mut req = RadioUsageStatsReqV1 {
            hotspot_pubkey: hotspot_pubkey.into(),
            cbsd_id: String::default(),
            service_provider_user_count,
            disco_mapping_user_count,
            offload_user_count,
            service_provider_transfer_bytes,
            offload_transfer_bytes,
            epoch_start_timestamp: 0,
            epoch_end_timestamp: 0,
            timestamp: 0,
            carrier_transfer_info,
            carrier_mapping_key: self.key_pair.public_key().to_vec(),
            signature: vec![],
        };

        req.signature = self.key_pair.sign(&req.encode_to_vec()).expect("sign");

        let mut request = Request::new(req);
        let metadata = request.metadata_mut();

        metadata.insert("authorization", self.authorization.clone());

        let res = self.client.submit_radio_usage_stats_report(request).await?;

        Ok(res.into_inner())
    }

    pub async fn submit_cell_heartbeat(
        &mut self,
        keypair: &Keypair,
        cbsd_id: &str,
    ) -> anyhow::Result<CellHeartbeatRespV1> {
        let mut heartbeat = CellHeartbeatReqV1 {
            pub_key: keypair.public_key().into(),
            hotspot_type: "unknown".to_string(),
            cell_id: 1,
            timestamp: Utc::now().timestamp() as u64,
            lat: 0.0,
            lon: 0.0,
            operation_mode: true,
            cbsd_category: "unknown".to_string(),
            cbsd_id: cbsd_id.to_owned(),
            signature: vec![],
            coverage_object: vec![1, 2, 3, 4],
        };

        heartbeat.signature = keypair.sign(&heartbeat.encode_to_vec()).expect("sign");

        let mut request = Request::new(heartbeat);
        let metadata = request.metadata_mut();

        metadata.insert("authorization", self.authorization.clone());

        let res = self.client.submit_cell_heartbeat(request).await?;

        Ok(res.into_inner())
    }

    pub async fn submit_data_transfer(
        &mut self,
        keypair: &Keypair,
        technology: DataTransferRadioAccessTechnology,
    ) -> anyhow::Result<DataTransferSessionRespV1> {
        let mut data_transfer = DataTransferSessionReqV1 {
            data_transfer_usage: Some(DataTransferEvent {
                pub_key: keypair.public_key().into(),
                upload_bytes: 0,
                download_bytes: 0,
                radio_access_technology: technology as i32,
                event_id: "event-1".to_string(),
                payer: vec![1, 2, 3, 4],
                timestamp: Utc::now().timestamp() as u64,
                signature: vec![],
            }),
            reward_cancelled: false,
            pub_key: keypair.public_key().into(),
            signature: vec![],
            rewardable_bytes: 0,
            carrier_id_v2: CarrierIdV2::Carrier9 as i32,
        };

        data_transfer.signature = keypair.sign(&data_transfer.encode_to_vec())?;

        let mut request = Request::new(data_transfer);
        let metadata = request.metadata_mut();

        metadata.insert("authorization", self.authorization.clone());

        let res = self.client.submit_data_transfer_session(request).await?;

        Ok(res.into_inner())
    }
}

pub fn generate_keypair() -> Keypair {
    Keypair::generate(KeyTag::default(), &mut OsRng)
}
