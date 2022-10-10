use crate::{error::DecodeError, required_network, Error, EventId, Result};
use chrono::Utc;
use file_store::traits::MsgVerify;
use file_store::{file_sink, file_sink_write, file_upload, FileType};
use futures_util::TryFutureExt;
use helium_crypto::{Network, PublicKey};
use helium_proto::services::poc_lora::{
    self, LoraBeaconIngestReportV1, LoraBeaconReportReqV1, LoraBeaconReportRespV1,
    LoraWitnessIngestReportV1, LoraWitnessReportReqV1, LoraWitnessReportRespV1,
};
use std::convert::TryFrom;
use std::env;
use std::{net::SocketAddr, path::Path, str::FromStr};
use tonic::{transport, Request, Response, Status};

pub type GrpcResult<T> = std::result::Result<Response<T>, Status>;

pub struct GrpcServer {
    lora_beacon_report_tx: file_sink::MessageSender,
    lora_witness_report_tx: file_sink::MessageSender,
    required_network: Network,
}

impl GrpcServer {
    fn new(
        lora_beacon_report_tx: file_sink::MessageSender,
        lora_witness_report_tx: file_sink::MessageSender,
    ) -> Result<Self> {
        Ok(Self {
            lora_beacon_report_tx,
            lora_witness_report_tx,
            required_network: required_network()?,
        })
    }

    fn verify_network(&self, public_key: &PublicKey) -> GrpcResult<()> {
        if self.required_network == public_key.network {
            Ok(Response::new(()))
        } else {
            Err(Status::invalid_argument("invalid network"))
        }
    }
}

#[tonic::async_trait]
impl poc_lora::PocLora for GrpcServer {
    async fn submit_lora_beacon(
        &self,
        request: Request<LoraBeaconReportReqV1>,
    ) -> GrpcResult<LoraBeaconReportRespV1> {
        let timestamp: u64 = Utc::now().timestamp_millis() as u64;
        let event = request.into_inner();
        let report = LoraBeaconIngestReportV1 {
            received_timestamp: timestamp,
            report: Some(event.clone()),
        };

        let public_key = PublicKey::try_from(event.pub_key.as_ref())
            .map_err(|_| Status::invalid_argument("invalid public key"))?;

        self.verify_network(&public_key)?;

        event
            .verify(&public_key)
            .map_err(|_| Status::invalid_argument("invalid signature"))?;

        let event_id = EventId::from(&event);
        let _ = file_sink_write!(
            "beacon_report",
            &self.lora_beacon_report_tx,
            report,
            format!("event_id:{:?}", event_id.to_string())
        )
        .await;
        // Encode event digest, encode and return as the id
        Ok(Response::new(event_id.into()))
    }

    async fn submit_lora_witness(
        &self,
        request: Request<LoraWitnessReportReqV1>,
    ) -> GrpcResult<LoraWitnessReportRespV1> {
        let timestamp: u64 = Utc::now().timestamp_millis() as u64;
        let event = request.into_inner();
        let report = LoraWitnessIngestReportV1 {
            received_timestamp: timestamp,
            report: Some(event.clone()),
        };

        let public_key = PublicKey::try_from(event.pub_key.as_ref())
            .map_err(|_| Status::invalid_argument("invalid public key"))?;

        self.verify_network(&public_key)?;

        event
            .verify(&public_key)
            .map_err(|_| Status::invalid_argument("invalid signature"))?;

        let event_id = EventId::from(&event);
        let _ = file_sink_write!(
            "witness_report",
            &self.lora_witness_report_tx,
            report,
            format!("event_id:{:?}", event_id.to_string())
        )
        .await;
        // Encode event digest, encode and return as the id
        Ok(Response::new(event_id.into()))
    }
}

pub async fn grpc_server(shutdown: triggered::Listener, server_mode: String) -> Result {
    let grpc_addr: SocketAddr = env::var("GRPC_SOCKET_ADDR")
        .map_or_else(
            |_| SocketAddr::from_str("0.0.0.0:9081"),
            |str| SocketAddr::from_str(&str),
        )
        .map_err(DecodeError::from)?;

    // Initialize uploader
    let (file_upload_tx, file_upload_rx) = file_upload::message_channel();
    let file_upload =
        file_upload::FileUpload::from_env_with_prefix("INGESTOR", file_upload_rx).await?;

    let store_path =
        std::env::var("INGEST_STORE").unwrap_or_else(|_| String::from("/var/data/ingestor"));
    let store_base_path = Path::new(&store_path);

    // lora beacon reports
    let (lora_beacon_report_tx, lora_beacon_report_rx) = file_sink::message_channel(50);
    let mut lora_beacon_report_sink = file_sink::FileSinkBuilder::new(
        FileType::LoraBeaconIngestReport,
        store_base_path,
        lora_beacon_report_rx,
    )
    .deposits(Some(file_upload_tx.clone()))
    .create()
    .await?;

    // lora witness reports
    let (lora_witness_report_tx, lora_witness_report_rx) = file_sink::message_channel(50);
    let mut lora_witness_report_sink = file_sink::FileSinkBuilder::new(
        FileType::LoraWitnessIngestReport,
        store_base_path,
        lora_witness_report_rx,
    )
    .deposits(Some(file_upload_tx.clone()))
    .create()
    .await?;

    let grpc_server = GrpcServer::new(lora_beacon_report_tx, lora_witness_report_tx)?;

    tracing::info!(
        "grpc listening on {} and server mode {}",
        grpc_addr,
        server_mode
    );

    let server = transport::Server::builder()
        .layer(poc_metrics::request_layer!("ingest_server_lora_connection"))
        .add_service(poc_lora::Server::new(grpc_server))
        .serve_with_shutdown(grpc_addr, shutdown.clone())
        .map_err(Error::from);

    tokio::try_join!(
        server,
        lora_beacon_report_sink.run(&shutdown).map_err(Error::from),
        lora_witness_report_sink.run(&shutdown).map_err(Error::from),
        file_upload.run(&shutdown).map_err(Error::from),
    )
    .map(|_| ())
}
