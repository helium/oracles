use std::{net::SocketAddr, str::FromStr, sync::Arc, time::Duration};

use anyhow::bail;
use backon::{ExponentialBuilder, Retryable};
use chrono::{DateTime, TimeZone, Utc};
use file_store::{
    file_sink::FileSinkClient,
    mobile_ban::{
        proto::{BanAction, BanDetailsV1, BanIngestReportV1, BanReqV1, BanRespV1},
        BanReason,
    },
};
use helium_crypto::{KeyTag, Keypair, Network, PublicKeyBinary, Sign};
use helium_proto::{
    services::{
        mobile_config::NetworkKeyRole,
        poc_mobile::{
            CarrierIdV2, CellHeartbeatReqV1, CellHeartbeatRespV1, Client as PocMobileClient,
            DataTransferEvent, DataTransferRadioAccessTechnology,
            DataTransferSessionIngestReportV1, DataTransferSessionReqV1, DataTransferSessionRespV1,
            EnabledCarriersInfoReportV1, EnabledCarriersInfoReqV1, EnabledCarriersInfoRespV1,
            HexUsageStatsIngestReportV1, HexUsageStatsReqV1, HexUsageStatsResV1,
            RadioUsageCarrierTransferInfo, RadioUsageStatsIngestReportV1, RadioUsageStatsReqV1,
            RadioUsageStatsResV1, SubscriberVerifiedMappingEventIngestReportV1,
            SubscriberVerifiedMappingEventReqV1, SubscriberVerifiedMappingEventResV1,
            UniqueConnectionsIngestReportV1, UniqueConnectionsReqV1, UniqueConnectionsRespV1,
        },
    },
    Message,
};
use mobile_config::client::{authorization_client::AuthorizationVerifier, ClientError};
use rand::rngs::OsRng;
use tokio::{
    net::TcpListener,
    sync::mpsc::{error::TryRecvError, Receiver},
    time::timeout,
};
use tonic::{
    async_trait,
    metadata::{Ascii, MetadataValue},
    transport::Channel,
    Request,
};
use triggered::Trigger;

use ingest_mobile::server::GrpcServer;

const PUBKEY1: &str = "113HRxtzxFbFUjDEJJpyeMRZRtdAW38LAUnB5mshRwi6jt7uFbt";

#[tokio::test]
async fn submit_enabled_carriers_info_valid() -> anyhow::Result<()> {
    let keypair = generate_keypair();
    let (mut client, trigger) = setup_mobile().await?;
    client
        .submit_enabled_carriers_info(
            &keypair,
            PUBKEY1,
            vec![CarrierIdV2::Carrier0],
            Utc::now().timestamp_millis() as u64,
        )
        .await?;

    let report = client.enabled_carriers_info_recv().await?;
    let inner_report = report.report.expect("inner report");

    assert_eq!(
        PublicKeyBinary::from(inner_report.hotspot_pubkey).to_string(),
        PUBKEY1
    );
    assert_eq!(
        inner_report.enabled_carriers,
        vec![CarrierIdV2::Carrier0 as i32]
    );

    trigger.trigger();
    Ok(())
}

#[tokio::test]
async fn submit_enabled_carriers_info_expired() -> anyhow::Result<()> {
    let keypair = generate_keypair();
    let (mut client, trigger) = setup_mobile().await?;
    let res = client
        .submit_enabled_carriers_info(
            &keypair,
            PUBKEY1,
            vec![CarrierIdV2::Carrier0],
            Utc::now().timestamp_millis() as u64 - (610 * 1000), // 11 min
        )
        .await;

    let binding = res.unwrap_err();
    let err = binding.downcast_ref::<tonic::Status>().unwrap();
    assert_eq!(
        err.message(),
        "The message is expired. It is generated more than 600 seconds ago"
    );
    trigger.trigger();
    Ok(())
}

#[tokio::test]
async fn submit_ban() -> anyhow::Result<()> {
    let (mut client, trigger) = setup_mobile().await?;

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
    let (mut client, trigger) = setup_mobile().await?;

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
    let (mut client, trigger) = setup_mobile().await?;

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
    let (mut client, trigger) = setup_mobile().await?;

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
    let (mut client, trigger) = setup_mobile().await?;

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
    let (mut client, _trigger) = setup_mobile().await.unwrap();

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
    let (mut client, trigger) = setup_mobile().await?;

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
    let (mut client, trigger) = setup_mobile().await?;

    let keypair = generate_keypair();

    client
        .submit_data_transfer(&keypair, DataTransferRadioAccessTechnology::Eutran)
        .await?;

    assert!(client.is_data_transfer_rx_empty()?);

    trigger.trigger();
    Ok(())
}

pub fn generate_keypair() -> Keypair {
    Keypair::generate(KeyTag::default(), &mut OsRng)
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
    let (enabled_carriers_tx, enabled_carriers_rx) = tokio::sync::mpsc::channel(10);

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
            FileSinkClient::new(enabled_carriers_tx, "enabled_carriers_sink"),
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
        enabled_carriers_rx,
    )
    .await;

    Ok((client, trigger))
}

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
    enabled_carriers_rx: Receiver<file_store::file_sink::Message<EnabledCarriersInfoReportV1>>,
}

impl TestClient {
    #[expect(clippy::too_many_arguments)]
    async fn new(
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
        enabled_carriers_rx: Receiver<file_store::file_sink::Message<EnabledCarriersInfoReportV1>>,
    ) -> TestClient {
        let client = (|| PocMobileClient::connect(format!("http://{socket_addr}")))
            .retry(&ExponentialBuilder::default())
            .await
            .expect("client connect");

        TestClient {
            client,
            key_pair: Arc::new(key_pair),
            authorization: format!("Bearer {api_token}").try_into().unwrap(),
            subscriber_mapping_file_sink_rx,
            hex_usage_stats_file_sink_rx,
            radio_usage_stats_file_sink_rx,
            unique_connections_file_sink_rx,
            ban_file_sink_rx,
            data_transfer_rx,
            enabled_carriers_rx,
        }
    }

    async fn data_transfer_recv(mut self) -> anyhow::Result<DataTransferSessionIngestReportV1> {
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

    fn is_data_transfer_rx_empty(&mut self) -> anyhow::Result<bool> {
        match self.data_transfer_rx.try_recv() {
            Ok(_) => Ok(false),
            Err(TryRecvError::Empty) => Ok(true),
            Err(err) => bail!(err),
        }
    }

    async fn unique_connection_recv(mut self) -> anyhow::Result<UniqueConnectionsIngestReportV1> {
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

    async fn subscriber_mapping_recv(
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

    async fn hex_usage_recv(mut self) -> anyhow::Result<HexUsageStatsIngestReportV1> {
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

    async fn radio_usage_recv(mut self) -> anyhow::Result<RadioUsageStatsIngestReportV1> {
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

    async fn ban_recv(mut self) -> anyhow::Result<BanIngestReportV1> {
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

    async fn enabled_carriers_info_recv(mut self) -> anyhow::Result<EnabledCarriersInfoReportV1> {
        match timeout(Duration::from_secs(2), self.enabled_carriers_rx.recv()).await {
            Ok(Some(msg)) => match msg {
                file_store::file_sink::Message::Commit(_) => bail!("got Commit"),
                file_store::file_sink::Message::Rollback(_) => bail!("got Rollback"),
                file_store::file_sink::Message::Data(_, data) => Ok(data),
            },
            Ok(None) => bail!("got none"),
            Err(reason) => bail!("got error {reason}"),
        }
    }

    async fn submit_ban(&mut self, hotspot_pubkey: Vec<u8>) -> anyhow::Result<BanRespV1> {
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

    async fn submit_unique_connections(
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

    async fn submit_verified_subscriber_mapping_event(
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

    async fn submit_hex_usage_req(
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
    async fn submit_radio_usage_req(
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

    async fn submit_cell_heartbeat(
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

    async fn submit_data_transfer(
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

    async fn submit_enabled_carriers_info(
        &mut self,
        keypair: &Keypair,
        hotspot_pubkey: &str,
        enabled_carriers: Vec<CarrierIdV2>,
        request_timestamp: u64,
    ) -> anyhow::Result<EnabledCarriersInfoRespV1> {
        let hotspot_pubkey = PublicKeyBinary::from_str(hotspot_pubkey)?;

        let mut carrier_req = EnabledCarriersInfoReqV1 {
            hotspot_pubkey: hotspot_pubkey.into(),
            enabled_carriers: enabled_carriers.into_iter().map(|v| v.into()).collect(),
            firmware_version: "v11".to_string(),
            timestamp_ms: request_timestamp,
            signer_pubkey: keypair.public_key().into(),
            signature: vec![],
        };
        carrier_req.signature = keypair.sign(&carrier_req.encode_to_vec()).expect("sign");
        let mut request = Request::new(carrier_req);
        let metadata = request.metadata_mut();

        metadata.insert("authorization", self.authorization.clone());

        let res = self.client.submit_enabled_carriers_info(request).await?;
        Ok(res.into_inner())
    }
}
