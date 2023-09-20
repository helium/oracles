use crate::Settings;
use anyhow::{Error, Result};
use chrono::{Duration, Utc};
use file_store::{
    file_sink::{self, FileSinkClient},
    file_upload,
    traits::MsgVerify,
    FileType,
};
use futures::{
    future::{LocalBoxFuture, TryFutureExt},
    Stream, StreamExt,
};
use helium_crypto::{Network, PublicKey};
use helium_proto::services::poc_lora::{
    self, lora_stream_request_v1::Request as StreamRequest,
    lora_stream_response_v1::Response as StreamResponse, LoraBeaconIngestReportV1,
    LoraBeaconReportReqV1, LoraBeaconReportRespV1, LoraStreamRequestV1, LoraStreamResponseV1,
    LoraStreamSessionInitV1, LoraStreamSessionOfferV1, LoraWitnessIngestReportV1,
    LoraWitnessReportReqV1, LoraWitnessReportRespV1,
};
use std::{convert::TryFrom, net::SocketAddr, path::Path};
use task_manager::{ManagedTask, TaskManager};
use tokio::{sync::mpsc::Sender, time::Instant};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{transport, Request, Response, Status, Streaming};

pub type GrpcResult<T> = std::result::Result<Response<T>, Status>;
pub type GrpcStreamResult<T> = ReceiverStream<Result<T, Status>>;
pub type VerifyResult<T> = std::result::Result<T, Status>;

type Nonce = [u8; 32];

#[derive(Debug)]
struct StreamState {
    beacon_report_sink: FileSinkClient,
    witness_report_sink: FileSinkClient,
    required_network: Network,
    pub_key_bytes: Option<Vec<u8>>,
    session_key: Option<PublicKey>,
    timeout: Instant,
    nonce: Option<Nonce>,
    session_key_offer_timeout: std::time::Duration,
    session_key_timeout: std::time::Duration,
}

impl StreamState {
    fn new(server: &GrpcServer) -> StreamState {
        StreamState {
            beacon_report_sink: server.beacon_report_sink.clone(),
            witness_report_sink: server.witness_report_sink.clone(),
            required_network: server.required_network,
            pub_key_bytes: None,
            session_key: None,
            timeout: Instant::now(),
            nonce: None,
            session_key_offer_timeout: server.session_key_offer_timeout,
            session_key_timeout: server.session_key_timeout,
        }
    }

    async fn send_offer(&mut self, tx: &Sender<Result<LoraStreamResponseV1, Status>>) -> bool {
        let nonce: Nonce = rand::random();
        self.nonce = Some(nonce);
        self.timeout = Instant::now() + self.session_key_offer_timeout;

        tx.send(Ok(LoraStreamResponseV1 {
            response: Some(StreamResponse::Offer(LoraStreamSessionOfferV1 {
                nonce: nonce.into(),
            })),
        }))
        .await
        .is_ok()
    }

    fn initialize(&mut self, init: LoraStreamSessionInitV1) -> Result<(), Status> {
        if self
            .nonce
            .as_ref()
            .map(|n| init.nonce == n)
            .unwrap_or(false)
        {
            self.session_key = Some(
                PublicKey::try_from(init.session_key)
                    .map_err(|_| Status::invalid_argument("invalid public key"))?,
            );
            self.pub_key_bytes = Some(init.pub_key);
            self.nonce = None;
            self.timeout = Instant::now() + self.session_key_timeout;

            Ok(())
        } else {
            Err(Status::invalid_argument("invalid nonce in session init"))
        }
    }

    async fn handle_message(&mut self, message: LoraStreamRequestV1) -> Result<(), Status> {
        let timestamp = Utc::now().timestamp_millis() as u64;
        match message.request {
            Some(StreamRequest::BeaconReport(report)) => {
                handle_beacon_report(
                    &self.beacon_report_sink,
                    timestamp,
                    report,
                    self.session_key.as_ref(),
                    self.pub_key_bytes.as_deref(),
                )
                .await
            }
            Some(StreamRequest::WitnessReport(report)) => {
                handle_witness_report(
                    &self.witness_report_sink,
                    timestamp,
                    report,
                    self.session_key.as_ref(),
                    self.pub_key_bytes.as_deref(),
                )
                .await
            }
            Some(StreamRequest::SessionInit(init)) => verify_public_key(&init.pub_key)
                .and_then(|pk| verify_network(self.required_network, pk))
                .and_then(|pk| verify_signature(Some(&pk), init))
                .and_then(|init| self.initialize(init)),
            None => Err(Status::aborted("stream closed")),
        }
    }

    async fn run(
        mut self,
        tx: Sender<Result<LoraStreamResponseV1, Status>>,
        in_stream: impl Stream<Item = Result<LoraStreamRequestV1, Status>>,
    ) {
        if !self.send_offer(&tx).await {
            return;
        }

        let mut in_stream = std::pin::pin!(in_stream);

        loop {
            tokio::select! {
                _ = tokio::time::sleep_until(self.timeout) => {
                    break;
                }
                message = in_stream.next() => {
                    match message {
                        Some(Ok(message)) => {
                            if let Err(err) = self.handle_message(message).await {
                                tracing::info!(?err, "error while handling message during stream_requests");
                                break;
                            }
                        }
                        Some(Err(err)) => {
                            tracing::debug!(?err, "error in streaming requests");
                            break;
                        }
                        None => {
                            //stream closed
                            break;
                        }
                    }
                }
            }
        }
    }
}

#[derive(Clone)]
pub struct GrpcServer {
    beacon_report_sink: FileSinkClient,
    witness_report_sink: FileSinkClient,
    required_network: Network,
    address: SocketAddr,
    session_key_offer_timeout: std::time::Duration,
    session_key_timeout: std::time::Duration,
}

impl ManagedTask for GrpcServer {
    fn start_task(
        self: Box<Self>,
        shutdown: triggered::Listener,
    ) -> LocalBoxFuture<'static, anyhow::Result<()>> {
        let address = self.address;
        Box::pin(async move {
            let grpc_server = transport::Server::builder()
                .layer(poc_metrics::request_layer!("ingest_server_iot_connection"))
                .add_service(poc_lora::Server::new(*self))
                .serve(address)
                .map_err(Error::from);

            tokio::select! {
                _ = shutdown => {
                    tracing::warn!("grpc server is shutting down");
                    Ok(())
                }
                _ = grpc_server => {
                    Err(anyhow::anyhow!("grpc server stopping running"))
                }
            }
        })
    }
}

fn verify_public_key(bytes: &[u8]) -> VerifyResult<PublicKey> {
    PublicKey::try_from(bytes).map_err(|_| Status::invalid_argument("invalid public key"))
}

fn verify_network(required_network: Network, public_key: PublicKey) -> VerifyResult<PublicKey> {
    if required_network == public_key.network {
        Ok(public_key)
    } else {
        Err(Status::invalid_argument("invalid network"))
    }
}

fn verify_signature<E>(public_key: Option<&PublicKey>, event: E) -> VerifyResult<E>
where
    E: MsgVerify,
{
    public_key
        .ok_or_else(|| Status::invalid_argument("signing public key not available"))
        .and_then(|public_key| {
            event
                .verify(public_key)
                .map(|_| event)
                .map_err(|_| Status::invalid_argument("invalid signature"))
        })
}

async fn handle_beacon_report(
    file_sink: &FileSinkClient,
    timestamp: u64,
    report: LoraBeaconReportReqV1,
    signing_key: Option<&PublicKey>,
    expected_pubkey_bytes: Option<&[u8]>,
) -> Result<(), Status> {
    let ingest_report = verify_signature(signing_key, report)
        .and_then(|report| {
            expected_pubkey_bytes
                .map(|bytes| bytes == report.pub_key)
                .unwrap_or(true)
                .then_some(report)
                .ok_or_else(|| Status::invalid_argument("incorrect pub_key"))
        })
        .map(|report| LoraBeaconIngestReportV1 {
            received_timestamp: timestamp,
            report: Some(report),
        })?;

    _ = file_sink.write(ingest_report, []).await;

    Ok(())
}

async fn handle_witness_report(
    file_sink: &FileSinkClient,
    timestamp: u64,
    report: LoraWitnessReportReqV1,
    session_key: Option<&PublicKey>,
    expected_pubkey_bytes: Option<&[u8]>,
) -> Result<(), Status> {
    let ingest_report = verify_signature(session_key, report)
        .and_then(|report| {
            expected_pubkey_bytes
                .map(|bytes| bytes == report.pub_key)
                .unwrap_or(true)
                .then_some(report)
                .ok_or_else(|| Status::invalid_argument("incorrect pub_key"))
        })
        .map(|report| LoraWitnessIngestReportV1 {
            received_timestamp: timestamp,
            report: Some(report),
        })?;

    _ = file_sink.write(ingest_report, []).await;

    Ok(())
}

#[tonic::async_trait]
impl poc_lora::PocLora for GrpcServer {
    async fn submit_lora_beacon(
        &self,
        request: Request<LoraBeaconReportReqV1>,
    ) -> GrpcResult<LoraBeaconReportRespV1> {
        let timestamp: u64 = Utc::now().timestamp_millis() as u64;
        let event = request.into_inner();

        let pub_key = verify_public_key(&event.pub_key)
            .and_then(|pk| verify_network(self.required_network, pk))?;

        handle_beacon_report(
            &self.beacon_report_sink,
            timestamp,
            event,
            Some(&pub_key),
            None,
        )
        .await?;

        let id = timestamp.to_string();
        Ok(Response::new(LoraBeaconReportRespV1 { id }))
    }

    async fn submit_lora_witness(
        &self,
        request: Request<LoraWitnessReportReqV1>,
    ) -> GrpcResult<LoraWitnessReportRespV1> {
        let timestamp: u64 = Utc::now().timestamp_millis() as u64;
        let event = request.into_inner();

        let pub_key = verify_public_key(&event.pub_key)
            .and_then(|pk| verify_network(self.required_network, pk))?;

        handle_witness_report(
            &self.witness_report_sink,
            timestamp,
            event,
            Some(&pub_key),
            None,
        )
        .await?;

        let id = timestamp.to_string();
        Ok(Response::new(LoraWitnessReportRespV1 { id }))
    }

    type stream_requestsStream = GrpcStreamResult<LoraStreamResponseV1>;
    async fn stream_requests(
        &self,
        request: Request<Streaming<LoraStreamRequestV1>>,
    ) -> GrpcResult<Self::stream_requestsStream> {
        let in_stream = request.into_inner();

        let stream_state = StreamState::new(self);
        let (tx, rx) = tokio::sync::mpsc::channel(5);
        tokio::spawn(stream_state.run(tx, in_stream));

        Ok(Response::new(GrpcStreamResult::new(rx)))
    }
}

pub async fn grpc_server(settings: &Settings) -> Result<()> {
    let grpc_addr = settings.listen_addr()?;

    // Initialize uploader
    let (file_upload, file_upload_server) =
        file_upload::FileUpload::from_settings_tm(&settings.output).await?;

    let store_base_path = Path::new(&settings.cache);

    // iot beacon reports
    let (beacon_report_sink, beacon_report_sink_server) = file_sink::FileSinkBuilder::new(
        FileType::IotBeaconIngestReport,
        store_base_path,
        concat!(env!("CARGO_PKG_NAME"), "_beacon_report"),
    )
    .file_upload(Some(file_upload.clone()))
    .roll_time(Duration::minutes(5))
    .create()
    .await?;

    // iot witness reports
    let (witness_report_sink, witness_report_sink_server) = file_sink::FileSinkBuilder::new(
        FileType::IotWitnessIngestReport,
        store_base_path,
        concat!(env!("CARGO_PKG_NAME"), "_witness_report"),
    )
    .file_upload(Some(file_upload.clone()))
    .roll_time(Duration::minutes(5))
    .create()
    .await?;

    let grpc_server = GrpcServer {
        beacon_report_sink,
        witness_report_sink,
        required_network: settings.network,
        address: grpc_addr,
        session_key_offer_timeout: settings.session_key_offer_timeout(),
        session_key_timeout: settings.session_key_timeout(),
    };

    tracing::info!(
        "grpc listening on {grpc_addr} and server mode {:?}",
        settings.mode
    );

    TaskManager::builder()
        .add_task(file_upload_server)
        .add_task(beacon_report_sink_server)
        .add_task(witness_report_sink_server)
        .add_task(grpc_server)
        .start()
        .await
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use file_store::file_sink::Message as SinkMessage;
    use helium_crypto::{KeyTag, KeyType, Keypair, Sign};
    use prost::Message;
    use rand::rngs::OsRng;

    use super::*;

    fn create_stream_state(
        session_key_offer_timeout: Option<u64>,
        session_key_timeout: Option<u64>,
    ) -> (
        StreamState,
        tokio::sync::mpsc::Receiver<SinkMessage>,
        tokio::sync::mpsc::Receiver<SinkMessage>,
    ) {
        let session_key_offer_timeout = session_key_offer_timeout.unwrap_or(1000);
        let session_key_timeout = session_key_timeout.unwrap_or(5000);

        let (beacon_tx, beacon_rx) = tokio::sync::mpsc::channel(5);
        let (witness_tx, witness_rx) = tokio::sync::mpsc::channel(5);

        let beacon_report_sink = FileSinkClient {
            sender: beacon_tx,
            metric: "beacons",
        };

        let witness_report_sink = FileSinkClient {
            sender: witness_tx,
            metric: "witnesses",
        };

        let grpc_server = GrpcServer {
            beacon_report_sink,
            witness_report_sink,
            required_network: Network::MainNet,
            session_key_offer_timeout: std::time::Duration::from_millis(session_key_offer_timeout),
            session_key_timeout: std::time::Duration::from_millis(session_key_timeout),
            address: SocketAddr::from_str("0.0.0.0:8080").expect("socket addr"),
        };

        (StreamState::new(&grpc_server), beacon_rx, witness_rx)
    }

    fn generate_keypair() -> Keypair {
        Keypair::generate(
            KeyTag {
                network: Network::MainNet,
                key_type: KeyType::Ed25519,
            },
            &mut OsRng,
        )
    }

    async fn send_init_request(
        tx: &tokio::sync::mpsc::Sender<Result<LoraStreamRequestV1, Status>>,
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

        tx.send(Ok(request)).await.expect("send init");
    }

    async fn send_beacon_report(
        tx: &tokio::sync::mpsc::Sender<Result<LoraStreamRequestV1, Status>>,
        pub_key: &PublicKey,
        signing_key: &Keypair,
    ) {
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

        tx.send(Ok(request)).await.expect("send beacon");
    }

    async fn send_witness_report(
        tx: &tokio::sync::mpsc::Sender<Result<LoraStreamRequestV1, Status>>,
        pub_key: &PublicKey,
        signing_key: &Keypair,
    ) {
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

        tx.send(Ok(request)).await.expect("send witness");
    }

    async fn receive<T>(rx: &mut tokio::sync::mpsc::Receiver<T>) -> Option<T> {
        tokio::select! {
            _ = tokio::time::sleep(std::time::Duration::from_secs(2)) => {
                panic!("did not receive message within 2 seconds")
            }
            message = rx.recv() => {
                message
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn initialize_session_and_send_beacon_and_witness() {
        let pub_key = generate_keypair();
        let session_key = generate_keypair();
        let (stream_state, mut file_sink_beacon_rx, mut file_sink_witness_rx) =
            create_stream_state(None, None);
        let (stream_tx, mut stream_rx) = tokio::sync::mpsc::channel(1);
        let (request_tx, request_rx) = tokio::sync::mpsc::channel(1);

        let _handle = tokio::spawn(stream_state.run(stream_tx, ReceiverStream::new(request_rx)));

        let Some(Ok(LoraStreamResponseV1 {
            response: Some(StreamResponse::Offer(offer)),
        })) = receive(&mut stream_rx).await
        else {
            panic!("invalid offer")
        };

        send_init_request(
            &request_tx,
            offer,
            pub_key.public_key(),
            session_key.public_key(),
            &pub_key,
        )
        .await;

        send_beacon_report(&request_tx, pub_key.public_key(), &session_key).await;

        let Some(SinkMessage::Data(_, bytes)) = receive(&mut file_sink_beacon_rx).await else {
            panic!("invalid sink message")
        };

        let beacon_ingest_report = LoraBeaconIngestReportV1::decode(bytes.as_slice())
            .expect("decode beacon ingest report");

        assert_eq!(
            beacon_ingest_report.report.unwrap().pub_key,
            <Vec<u8>>::from(pub_key.public_key())
        );

        send_witness_report(&request_tx, pub_key.public_key(), &session_key).await;

        let Some(SinkMessage::Data(_, bytes)) = receive(&mut file_sink_witness_rx).await else {
            panic!("invalid sink message")
        };

        let witness_ingest_report = LoraWitnessIngestReportV1::decode(bytes.as_slice())
            .expect("decode witness ingest report");

        assert_eq!(
            witness_ingest_report.report.unwrap().pub_key,
            <Vec<u8>>::from(pub_key.public_key())
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn stream_stops_after_incorrectly_signed_init_request() {
        let pub_key = generate_keypair();
        let session_key = generate_keypair();
        let (stream_state, _, _) = create_stream_state(None, None);
        let (stream_tx, mut stream_rx) = tokio::sync::mpsc::channel(1);
        let (request_tx, request_rx) = tokio::sync::mpsc::channel(1);

        let handle = tokio::spawn(stream_state.run(stream_tx, ReceiverStream::new(request_rx)));

        let Some(Ok(LoraStreamResponseV1 {
            response: Some(StreamResponse::Offer(offer)),
        })) = receive(&mut stream_rx).await
        else {
            panic!("invalid offer")
        };

        send_init_request(
            &request_tx,
            offer,
            pub_key.public_key(),
            session_key.public_key(),
            // Should be signed with pub_key
            &session_key,
        )
        .await;

        let _ = tokio::time::timeout(std::time::Duration::from_secs(1), handle)
            .await
            .expect("stream should have closed due to incorrectly signed init request");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn stream_stops_after_incorrectly_signed_beacon() {
        let pub_key = generate_keypair();
        let session_key = generate_keypair();
        let (stream_state, mut file_sink_beacons, _) = create_stream_state(None, None);
        let (stream_tx, mut stream_rx) = tokio::sync::mpsc::channel(1);
        let (request_tx, request_rx) = tokio::sync::mpsc::channel(1);

        let handle = tokio::spawn(stream_state.run(stream_tx, ReceiverStream::new(request_rx)));

        let Some(Ok(LoraStreamResponseV1 {
            response: Some(StreamResponse::Offer(offer)),
        })) = receive(&mut stream_rx).await
        else {
            panic!("invalid offer")
        };

        send_init_request(
            &request_tx,
            offer,
            pub_key.public_key(),
            session_key.public_key(),
            &pub_key,
        )
        .await;

        //Incorrectly signed with pub_key
        send_beacon_report(&request_tx, pub_key.public_key(), &pub_key).await;

        let _ = tokio::time::timeout(std::time::Duration::from_secs(1), handle)
            .await
            .expect("stream should have closed due to incorrectly signed beacon request");

        assert!(file_sink_beacons.recv().await.is_none());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn stream_stops_after_incorrect_beacon_pubkey() {
        let pub_key = generate_keypair();
        let session_key = generate_keypair();
        let (stream_state, mut file_sink_beacons, _) = create_stream_state(None, None);
        let (stream_tx, mut stream_rx) = tokio::sync::mpsc::channel(1);
        let (request_tx, request_rx) = tokio::sync::mpsc::channel(1);

        let handle = tokio::spawn(stream_state.run(stream_tx, ReceiverStream::new(request_rx)));

        let Some(Ok(LoraStreamResponseV1 {
            response: Some(StreamResponse::Offer(offer)),
        })) = receive(&mut stream_rx).await
        else {
            panic!("invalid offer")
        };

        send_init_request(
            &request_tx,
            offer,
            pub_key.public_key(),
            session_key.public_key(),
            &pub_key,
        )
        .await;

        //Incorrect pub_key sent
        send_beacon_report(&request_tx, session_key.public_key(), &session_key).await;

        let _ = tokio::time::timeout(std::time::Duration::from_secs(1), handle)
            .await
            .expect("stream should have closed due to incorrect beacon pub_key");

        assert!(file_sink_beacons.recv().await.is_none());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn stream_stops_after_incorrectly_signed_witness() {
        let pub_key = generate_keypair();
        let session_key = generate_keypair();
        let (stream_state, _, mut file_sink_witnesses) = create_stream_state(None, None);
        let (stream_tx, mut stream_rx) = tokio::sync::mpsc::channel(1);
        let (request_tx, request_rx) = tokio::sync::mpsc::channel(1);

        let handle = tokio::spawn(stream_state.run(stream_tx, ReceiverStream::new(request_rx)));

        let Some(Ok(LoraStreamResponseV1 {
            response: Some(StreamResponse::Offer(offer)),
        })) = receive(&mut stream_rx).await
        else {
            panic!("invalid offer")
        };

        send_init_request(
            &request_tx,
            offer,
            pub_key.public_key(),
            session_key.public_key(),
            &pub_key,
        )
        .await;

        //Incorrectly signed with pub_key
        send_witness_report(&request_tx, pub_key.public_key(), &pub_key).await;

        let _ = tokio::time::timeout(std::time::Duration::from_secs(1), handle)
            .await
            .expect("stream should have closed due to incorrectly signed witness request");

        assert!(file_sink_witnesses.recv().await.is_none());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn stream_stops_after_incorrect_witness_pubkey() {
        let pub_key = generate_keypair();
        let session_key = generate_keypair();
        let (stream_state, _, mut file_sink_witnesses) = create_stream_state(None, None);
        let (stream_tx, mut stream_rx) = tokio::sync::mpsc::channel(1);
        let (request_tx, request_rx) = tokio::sync::mpsc::channel(1);

        let handle = tokio::spawn(stream_state.run(stream_tx, ReceiverStream::new(request_rx)));

        let Some(Ok(LoraStreamResponseV1 {
            response: Some(StreamResponse::Offer(offer)),
        })) = receive(&mut stream_rx).await
        else {
            panic!("invalid offer")
        };

        send_init_request(
            &request_tx,
            offer,
            pub_key.public_key(),
            session_key.public_key(),
            &pub_key,
        )
        .await;

        //Incorrectly signed with pub_key
        send_witness_report(&request_tx, session_key.public_key(), &session_key).await;

        let _ = tokio::time::timeout(std::time::Duration::from_secs(1), handle)
            .await
            .expect("stream should have closed due to incorrect witness pub_key");

        assert!(file_sink_witnesses.recv().await.is_none());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn stream_stop_if_client_attempts_to_initiliaze_2nd_session() {
        let pub_key = generate_keypair();
        let session_key = generate_keypair();
        let (stream_state, mut file_sink_beacon_rx, _) = create_stream_state(None, None);
        let (stream_tx, mut stream_rx) = tokio::sync::mpsc::channel(1);
        let (request_tx, request_rx) = tokio::sync::mpsc::channel(1);

        let handle = tokio::spawn(stream_state.run(stream_tx, ReceiverStream::new(request_rx)));

        let Some(Ok(LoraStreamResponseV1 {
            response: Some(StreamResponse::Offer(offer)),
        })) = receive(&mut stream_rx).await
        else {
            panic!("invalid offer")
        };

        send_init_request(
            &request_tx,
            offer.clone(),
            pub_key.public_key(),
            session_key.public_key(),
            &pub_key,
        )
        .await;

        send_beacon_report(&request_tx, pub_key.public_key(), &session_key).await;

        let Some(SinkMessage::Data(_, bytes)) = receive(&mut file_sink_beacon_rx).await else {
            panic!("invalid sink message")
        };

        let beacon_ingest_report = LoraBeaconIngestReportV1::decode(bytes.as_slice())
            .expect("decode beacon ingest report");

        assert_eq!(
            beacon_ingest_report.report.unwrap().pub_key,
            <Vec<u8>>::from(pub_key.public_key())
        );

        send_init_request(
            &request_tx,
            offer,
            pub_key.public_key(),
            session_key.public_key(),
            &pub_key,
        )
        .await;

        let _ = tokio::time::timeout(std::time::Duration::from_secs(1), handle)
            .await
            .expect("stream should have closed due to attempt to create 2nd session");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn stream_stops_if_init_not_sent_within_timeout() {
        let (stream_state, _, _) = create_stream_state(Some(500), None);
        let (stream_tx, mut stream_rx) = tokio::sync::mpsc::channel(1);
        let (_request_tx, request_rx) = tokio::sync::mpsc::channel(1);

        let handle = tokio::spawn(stream_state.run(stream_tx, ReceiverStream::new(request_rx)));

        let Some(Ok(LoraStreamResponseV1 {
            response: Some(StreamResponse::Offer(_offer)),
        })) = receive(&mut stream_rx).await
        else {
            panic!("invalid offer")
        };

        let _ = tokio::time::timeout(std::time::Duration::from_secs(1), handle)
            .await
            .expect("stream should have closed due to no init being sent within timeout");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn stream_stops_on_session_timeout() {
        let pub_key = generate_keypair();
        let session_key = generate_keypair();
        let (stream_state, mut file_sink_beacon_rx, _) = create_stream_state(None, Some(2000));
        let (stream_tx, mut stream_rx) = tokio::sync::mpsc::channel(1);
        let (request_tx, request_rx) = tokio::sync::mpsc::channel(1);

        let handle = tokio::spawn(stream_state.run(stream_tx, ReceiverStream::new(request_rx)));

        let Some(Ok(LoraStreamResponseV1 {
            response: Some(StreamResponse::Offer(offer)),
        })) = receive(&mut stream_rx).await
        else {
            panic!("invalid offer")
        };

        send_init_request(
            &request_tx,
            offer,
            pub_key.public_key(),
            session_key.public_key(),
            &pub_key,
        )
        .await;

        send_beacon_report(&request_tx, pub_key.public_key(), &session_key).await;

        let Some(SinkMessage::Data(_, bytes)) = receive(&mut file_sink_beacon_rx).await else {
            panic!("invalid sink message")
        };

        let beacon_ingest_report = LoraBeaconIngestReportV1::decode(bytes.as_slice())
            .expect("decode beacon ingest report");

        assert_eq!(
            beacon_ingest_report.report.unwrap().pub_key,
            <Vec<u8>>::from(pub_key.public_key())
        );

        let _ = tokio::time::timeout(std::time::Duration::from_millis(2500), handle)
            .await
            .expect("stream should have closed due to session timeout");
    }
}
