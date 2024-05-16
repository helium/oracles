use std::net::{SocketAddr, TcpListener};

use backon::{ExponentialBuilder, Retryable};
use file_store::file_sink::{FileSinkClient, Message as SinkMessage};
use helium_crypto::{KeyTag, Keypair, Network, PublicKey, Sign};
use helium_proto::services::poc_lora::{
    lora_stream_request_v1::Request as StreamRequest,
    lora_stream_response_v1::Response as StreamResponse, poc_lora_client::PocLoraClient,
    LoraBeaconIngestReportV1, LoraBeaconReportReqV1, LoraStreamRequestV1, LoraStreamResponseV1,
    LoraStreamSessionInitV1, LoraStreamSessionOfferV1, LoraWitnessIngestReportV1,
    LoraWitnessReportReqV1,
};
use ingest::server_iot::GrpcServer;
use prost::Message;
use rand::rngs::OsRng;
use task_manager::TaskManager;
use tokio::{sync::mpsc::error::TryRecvError, task::LocalSet, time::timeout};
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use tonic::{transport::Channel, Streaming};

#[tokio::test]
async fn initialize_session_and_send_beacon_and_witness() {
    let (beacon_client, mut beacons) = create_file_sink();
    let (witness_client, mut witnesses) = create_file_sink();
    let addr = get_socket_addr().expect("socket addr");

    LocalSet::new()
        .run_until(async move {
            tokio::task::spawn_local(async move {
                let server = create_test_server(addr, beacon_client, witness_client, None, None);
                TaskManager::builder()
                    .add_task(server)
                    .build()
                    .start()
                    .await
            });

            let pub_key = generate_keypair();
            let session_key = generate_keypair();

            let mut client = connect_and_stream(addr).await;
            let offer = client.receive_offer().await;

            client
                .send_init(
                    offer,
                    pub_key.public_key(),
                    session_key.public_key(),
                    &pub_key,
                )
                .await;

            client.send_beacon(pub_key.public_key(), &session_key).await;

            let beacon_ingest_report = beacons.receive_beacon().await;
            assert_eq!(
                beacon_ingest_report.report.unwrap().pub_key,
                <Vec<u8>>::from(pub_key.public_key())
            );

            client
                .send_witness(pub_key.public_key(), &session_key)
                .await;

            let witness_ingest_report = witnesses.receive_witness().await;
            assert_eq!(
                witness_ingest_report.report.unwrap().pub_key,
                <Vec<u8>>::from(pub_key.public_key())
            );
        })
        .await;
}

#[tokio::test]
async fn stream_stops_after_incorrectly_signed_init_request() {
    let (beacon_client, _) = create_file_sink();
    let (witness_client, _) = create_file_sink();
    let addr = get_socket_addr().expect("socket addr");

    LocalSet::new()
        .run_until(async move {
            tokio::task::spawn_local(async move {
                let server = create_test_server(addr, beacon_client, witness_client, None, None);
                TaskManager::builder()
                    .add_task(server)
                    .build()
                    .start()
                    .await
            });

            let pub_key = generate_keypair();
            let session_key = generate_keypair();

            let mut client = connect_and_stream(addr).await;
            let offer = client.receive_offer().await;

            client
                .send_init(
                    offer,
                    pub_key.public_key(),
                    session_key.public_key(),
                    // should be signed by pub_key
                    &session_key,
                )
                .await;

            client.assert_closed().await;
        })
        .await;
}

#[tokio::test]
async fn stream_stops_after_incorrectly_signed_beacon() {
    let (beacon_client, beacons) = create_file_sink();
    let (witness_client, _) = create_file_sink();
    let addr = get_socket_addr().expect("socket addr");

    LocalSet::new()
        .run_until(async move {
            tokio::task::spawn_local(async move {
                let server = create_test_server(addr, beacon_client, witness_client, None, None);
                TaskManager::builder()
                    .add_task(server)
                    .build()
                    .start()
                    .await
            });

            let pub_key = generate_keypair();
            let session_key = generate_keypair();

            let mut client = connect_and_stream(addr).await;
            let offer = client.receive_offer().await;

            client
                .send_init(
                    offer,
                    pub_key.public_key(),
                    session_key.public_key(),
                    &pub_key,
                )
                .await;

            // Incorrectly signed by pub_key
            client.send_beacon(pub_key.public_key(), &pub_key).await;

            client.assert_closed().await;
            beacons.assert_no_messages();
        })
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stream_stops_after_incorrect_beacon_pubkey() {
    let (beacon_client, beacons) = create_file_sink();
    let (witness_client, _) = create_file_sink();
    let addr = get_socket_addr().expect("socket addr");

    LocalSet::new()
        .run_until(async move {
            tokio::task::spawn_local(async move {
                let server = create_test_server(addr, beacon_client, witness_client, None, None);
                TaskManager::builder()
                    .add_task(server)
                    .build()
                    .start()
                    .await
            });

            let pub_key = generate_keypair();
            let session_key = generate_keypair();

            let mut client = connect_and_stream(addr).await;
            let offer = client.receive_offer().await;

            client
                .send_init(
                    offer,
                    pub_key.public_key(),
                    session_key.public_key(),
                    &pub_key,
                )
                .await;

            // Incorrect pub_key sent
            let other_key = generate_keypair();
            client
                .send_beacon(other_key.public_key(), &session_key)
                .await;

            client.assert_closed().await;
            beacons.assert_no_messages();
        })
        .await;
}

#[tokio::test]
async fn stream_stops_after_incorrectly_signed_witness() {
    let (beacon_client, _) = create_file_sink();
    let (witness_client, witnesses) = create_file_sink();
    let addr = get_socket_addr().expect("socket addr");

    LocalSet::new()
        .run_until(async move {
            tokio::task::spawn_local(async move {
                let server = create_test_server(addr, beacon_client, witness_client, None, None);
                TaskManager::builder()
                    .add_task(server)
                    .build()
                    .start()
                    .await
            });

            let pub_key = generate_keypair();
            let session_key = generate_keypair();

            let mut client = connect_and_stream(addr).await;
            let offer = client.receive_offer().await;

            client
                .send_init(
                    offer,
                    pub_key.public_key(),
                    session_key.public_key(),
                    &pub_key,
                )
                .await;

            // Incorrectly signed by pub_key
            client.send_witness(pub_key.public_key(), &pub_key).await;

            client.assert_closed().await;
            witnesses.assert_no_messages();
        })
        .await;
}

#[tokio::test]
async fn stream_stops_after_incorrect_witness_pubkey() {
    let (beacon_client, _) = create_file_sink();
    let (witness_client, witnesses) = create_file_sink();
    let addr = get_socket_addr().expect("socket addr");

    LocalSet::new()
        .run_until(async move {
            tokio::task::spawn_local(async move {
                let server = create_test_server(addr, beacon_client, witness_client, None, None);
                TaskManager::builder()
                    .add_task(server)
                    .build()
                    .start()
                    .await
            });

            let pub_key = generate_keypair();
            let session_key = generate_keypair();

            let mut client = connect_and_stream(addr).await;
            let offer = client.receive_offer().await;

            client
                .send_init(
                    offer,
                    pub_key.public_key(),
                    session_key.public_key(),
                    &pub_key,
                )
                .await;

            // Incorrect pub_key
            let other_key = generate_keypair();
            client
                .send_witness(other_key.public_key(), &session_key)
                .await;

            client.assert_closed().await;
            witnesses.assert_no_messages();
        })
        .await;
}

#[tokio::test]
async fn stream_stop_if_client_attempts_to_initiliaze_2nd_session() {
    let (beacon_client, mut beacons) = create_file_sink();
    let (witness_client, _) = create_file_sink();
    let addr = get_socket_addr().expect("socket addr");

    LocalSet::new()
        .run_until(async move {
            tokio::task::spawn_local(async move {
                let server = create_test_server(addr, beacon_client, witness_client, None, None);
                TaskManager::builder()
                    .add_task(server)
                    .build()
                    .start()
                    .await
            });

            let pub_key = generate_keypair();
            let session_key = generate_keypair();

            let mut client = connect_and_stream(addr).await;
            let offer = client.receive_offer().await;

            client
                .send_init(
                    offer.clone(),
                    pub_key.public_key(),
                    session_key.public_key(),
                    &pub_key,
                )
                .await;

            client.send_beacon(pub_key.public_key(), &session_key).await;

            let beacon_ingest_report = beacons.receive_beacon().await;
            assert_eq!(
                beacon_ingest_report.report.unwrap().pub_key,
                <Vec<u8>>::from(pub_key.public_key())
            );

            client
                .send_init(
                    offer,
                    pub_key.public_key(),
                    session_key.public_key(),
                    &pub_key,
                )
                .await;

            client.assert_closed().await;
        })
        .await;
}

#[tokio::test]
async fn stream_stops_if_init_not_sent_within_timeout() {
    let (beacon_client, _) = create_file_sink();
    let (witness_client, _) = create_file_sink();
    let addr = get_socket_addr().expect("socket addr");

    LocalSet::new()
        .run_until(async move {
            tokio::task::spawn_local(async move {
                let server =
                    create_test_server(addr, beacon_client, witness_client, Some(500), None);
                TaskManager::builder()
                    .add_task(server)
                    .build()
                    .start()
                    .await
            });

            let mut client = connect_and_stream(addr).await;
            let _offer = client.receive_offer().await;

            client.assert_closed().await;
        })
        .await;
}

#[tokio::test]
async fn stream_stops_on_session_timeout() {
    let (beacon_client, mut beacons) = create_file_sink();
    let (witness_client, _) = create_file_sink();
    let addr = get_socket_addr().expect("socket addr");

    LocalSet::new()
        .run_until(async move {
            tokio::task::spawn_local(async move {
                let server =
                    create_test_server(addr, beacon_client, witness_client, Some(500), Some(900));
                TaskManager::builder()
                    .add_task(server)
                    .build()
                    .start()
                    .await
            });

            let mut client = connect_and_stream(addr).await;
            let offer = client.receive_offer().await;

            let pub_key = generate_keypair();
            let session_key = generate_keypair();

            client
                .send_init(
                    offer,
                    pub_key.public_key(),
                    session_key.public_key(),
                    &pub_key,
                )
                .await;

            client.send_beacon(pub_key.public_key(), &session_key).await;

            let beacon_ingest_report = beacons.receive_beacon().await;
            assert_eq!(
                beacon_ingest_report.report.unwrap().pub_key,
                <Vec<u8>>::from(pub_key.public_key())
            );

            client.assert_closed().await;
        })
        .await;
}

struct MockFileSinkReceiver {
    receiver: tokio::sync::mpsc::Receiver<SinkMessage>,
}

impl MockFileSinkReceiver {
    async fn receive(&mut self) -> SinkMessage {
        match timeout(seconds(2), self.receiver.recv()).await {
            Ok(Some(msg)) => msg,
            Ok(None) => panic!("server closed connection while waiting for message"),
            Err(_) => panic!("timeout while waiting for message"),
        }
    }

    fn assert_no_messages(mut self) {
        let Err(TryRecvError::Empty) = self.receiver.try_recv() else {
            panic!("receiver should have been empty")
        };
    }

    async fn receive_beacon(&mut self) -> LoraBeaconIngestReportV1 {
        match self.receive().await {
            SinkMessage::Data(_, bytes) => LoraBeaconIngestReportV1::decode(bytes.as_slice())
                .expect("decode beacon ingest report"),
            _ => panic!("invalid beacon message"),
        }
    }

    async fn receive_witness(&mut self) -> LoraWitnessIngestReportV1 {
        match self.receive().await {
            SinkMessage::Data(_, bytes) => LoraWitnessIngestReportV1::decode(bytes.as_slice())
                .expect("decode witness ingest report"),
            _ => panic!("invalid witness message"),
        }
    }
}

fn create_file_sink() -> (FileSinkClient, MockFileSinkReceiver) {
    let (tx, rx) = tokio::sync::mpsc::channel(5);
    (
        FileSinkClient::new(tx, "metric"),
        MockFileSinkReceiver { receiver: rx },
    )
}

async fn connect_and_stream(socket_addr: SocketAddr) -> TestClient {
    let mut client = (|| PocLoraClient::connect(format!("http://{socket_addr}")))
        .retry(&ExponentialBuilder::default())
        .await
        .expect("client connect");

    let (stream_tx, stream_rx) = tokio::sync::mpsc::channel(5);
    let response = client
        .stream_requests(ReceiverStream::new(stream_rx))
        .await
        .expect("stream requests");

    TestClient {
        _client: client,
        stream_tx,
        in_stream: response.into_inner(),
    }
}

struct TestClient {
    _client: PocLoraClient<Channel>,
    stream_tx: tokio::sync::mpsc::Sender<LoraStreamRequestV1>,
    in_stream: Streaming<LoraStreamResponseV1>,
}

impl TestClient {
    async fn receive_offer(&mut self) -> LoraStreamSessionOfferV1 {
        match timeout(seconds(1), self.in_stream.next()).await {
            Ok(Some(Ok(LoraStreamResponseV1 {
                response: Some(StreamResponse::Offer(offer)),
            }))) => offer,
            Ok(None) => panic!("server closed stream waiting for offer"),
            Ok(_) => panic!("invalid offer received"),
            Err(_) => panic!("timeout exceeded waiting for offer"),
        }
    }

    async fn assert_closed(mut self) {
        let Ok(None) = timeout(seconds(1), self.in_stream.next()).await else {
            panic!("Should have received None to indicate server closed connection")
        };
    }

    async fn send_init(
        &mut self,
        offer: LoraStreamSessionOfferV1,
        pub_key: &PublicKey,
        session_key: &PublicKey,
        signing_key: &Keypair,
    ) {
        let mut init = LoraStreamSessionInitV1 {
            pub_key: pub_key.into(),
            nonce: offer.nonce,
            session_key: session_key.into(),
            signature: vec![],
        };

        init.signature = signing_key.sign(&init.encode_to_vec()).expect("sign");

        let request = LoraStreamRequestV1 {
            request: Some(StreamRequest::SessionInit(init)),
        };

        self.stream_tx
            .send(request)
            .await
            .expect("send init failed");
    }

    async fn send_beacon(&mut self, pub_key: &PublicKey, signing_key: &Keypair) {
        let mut report = LoraBeaconReportReqV1 {
            pub_key: pub_key.into(),
            local_entropy: vec![],
            remote_entropy: vec![],
            data: vec![],
            frequency: 0,
            channel: 0,
            datarate: 0,
            tx_power: 0,
            timestamp: 0,
            signature: vec![],
            tmst: 0,
        };

        report.signature = signing_key.sign(&report.encode_to_vec()).expect("sign");

        let request = LoraStreamRequestV1 {
            request: Some(StreamRequest::BeaconReport(report)),
        };

        self.stream_tx
            .send(request)
            .await
            .expect("send beacon failed");
    }

    async fn send_witness(&mut self, pub_key: &PublicKey, signing_key: &Keypair) {
        let mut report = LoraWitnessReportReqV1 {
            pub_key: pub_key.into(),
            data: vec![],
            timestamp: 0,
            tmst: 0,
            signal: 0,
            snr: 0,
            frequency: 0,
            datarate: 0,
            signature: vec![],
        };

        report.signature = signing_key.sign(&report.encode_to_vec()).expect("sign");

        let request = LoraStreamRequestV1 {
            request: Some(StreamRequest::WitnessReport(report)),
        };

        self.stream_tx
            .send(request)
            .await
            .expect("send witness failed");
    }
}

fn create_test_server(
    socket_addr: SocketAddr,
    beacon_file_sink: FileSinkClient,
    witness_file_sink: FileSinkClient,
    offer_timeout: Option<u64>,
    timeout: Option<u64>,
) -> GrpcServer {
    let offer_timeout = offer_timeout.unwrap_or(5000);
    let timeout = timeout.unwrap_or(30 * 60000);
    GrpcServer {
        beacon_report_sink: beacon_file_sink,
        witness_report_sink: witness_file_sink,
        required_network: Network::MainNet,
        address: socket_addr,
        session_key_offer_timeout: std::time::Duration::from_millis(offer_timeout),
        session_key_timeout: std::time::Duration::from_millis(timeout),
    }
}

fn generate_keypair() -> Keypair {
    Keypair::generate(KeyTag::default(), &mut OsRng)
}

fn seconds(s: u64) -> std::time::Duration {
    std::time::Duration::from_secs(s)
}

fn get_socket_addr() -> anyhow::Result<SocketAddr> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    Ok(listener.local_addr()?)
}
