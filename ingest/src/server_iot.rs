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
                    let pub_key = self.pub_key_bytes.map(|b| bs58::encode(&b).into_string()).unwrap_or("".to_string());
                    tracing::debug!(?pub_key, "stream request timed out");
                    break;
                }
                message = in_stream.next() => {
                    match message {
                        Some(Ok(message)) => {
                            if let Err(err) = self.handle_message(message).await {
                                let pub_key = self.pub_key_bytes.map(|b| bs58::encode(&b).into_string()).unwrap_or("".to_string());
                                tracing::info!(?pub_key, ?err, "error while handling message during stream_requests");
                                break;
                            }
                        }
                        Some(Err(err)) => {
                            let pub_key = self.pub_key_bytes.map(|b| bs58::encode(&b).into_string()).unwrap_or("".to_string());
                            tracing::debug!(?pub_key, ?err, "error in streaming requests");
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

#[derive(Clone, Debug)]
pub struct GrpcServer {
    pub beacon_report_sink: FileSinkClient,
    pub witness_report_sink: FileSinkClient,
    pub required_network: Network,
    pub address: SocketAddr,
    pub session_key_offer_timeout: std::time::Duration,
    pub session_key_timeout: std::time::Duration,
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
    // Initialize uploader
    let (file_upload, file_upload_server) =
        file_upload::FileUpload::from_settings_tm(&settings.output).await?;

    let store_base_path = Path::new(&settings.cache);

    // iot beacon reports
    let (beacon_report_sink, beacon_report_sink_server) = file_sink::FileSinkBuilder::new(
        FileType::IotBeaconIngestReport,
        store_base_path,
        file_upload.clone(),
        concat!(env!("CARGO_PKG_NAME"), "_beacon_report"),
    )
    .roll_time(Duration::minutes(5))
    .create()
    .await?;

    // iot witness reports
    let (witness_report_sink, witness_report_sink_server) = file_sink::FileSinkBuilder::new(
        FileType::IotWitnessIngestReport,
        store_base_path,
        file_upload.clone(),
        concat!(env!("CARGO_PKG_NAME"), "_witness_report"),
    )
    .roll_time(Duration::minutes(5))
    .create()
    .await?;

    let grpc_addr = settings.listen;
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
        .build()
        .start()
        .await
}
