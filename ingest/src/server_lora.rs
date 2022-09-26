use crate::{error::DecodeError, Error, EventId, Result};
use chrono::Utc;
use file_store::traits::MsgVerify;
use file_store::{file_sink, file_upload, FileType};
use futures_util::TryFutureExt;
use helium_crypto::PublicKey;
use helium_proto::services::poc_lora::{
    self, LoraBeaconIngestReportV1, LoraBeaconReportReqV1, LoraBeaconReportRespV1,
    LoraWitnessIngestReportV1, LoraWitnessReportReqV1, LoraWitnessReportRespV1,
};
use std::convert::TryFrom;
use std::env;
use std::{net::SocketAddr, path::Path, str::FromStr};
use tonic::{metadata::MetadataValue, transport, Request, Response, Status};

pub type GrpcResult<T> = std::result::Result<Response<T>, Status>;

pub struct GrpcServer {
    lora_beacon_report_tx: file_sink::MessageSender,
    lora_witness_report_tx: file_sink::MessageSender,
}

#[tonic::async_trait]
impl poc_lora::PocLora for GrpcServer {
    async fn submit_lora_beacon(
        &self,
        request: Request<LoraBeaconReportReqV1>,
    ) -> GrpcResult<LoraBeaconReportRespV1> {
        let timestamp: i64 = Utc::now().timestamp();
        let event = request.into_inner();
        let report = LoraBeaconIngestReportV1 {
            received_timestamp: u64::try_from(timestamp).unwrap(),
            report: Some(event.clone()),
        };
        let public_key = PublicKey::try_from(event.pub_key.as_ref())
            .map_err(|_| Status::invalid_argument("invalid public key"))?;
        event
            .verify(&public_key)
            .map_err(|_| Status::invalid_argument("invalid signature"))?;

        let event_id = EventId::from(&event);
        match file_sink::write(&self.lora_beacon_report_tx, report).await {
            Ok(_) => tracing::debug!(
                "successfully processed beacon report, returning response {}",
                event_id.to_string()
            ),
            Err(err) => tracing::error!("failed to store lora beacon report: {err:?}"),
        }
        // Encode event digest, encode and return as the id
        Ok(Response::new(event_id.into()))
    }

    async fn submit_lora_witness(
        &self,
        request: Request<LoraWitnessReportReqV1>,
    ) -> GrpcResult<LoraWitnessReportRespV1> {
        let timestamp: i64 = Utc::now().timestamp();
        let event = request.into_inner();
        let report = LoraWitnessIngestReportV1 {
            received_timestamp: u64::try_from(timestamp).unwrap(),
            report: Some(event.clone()),
        };
        let public_key = PublicKey::try_from(event.pub_key.as_ref())
            .map_err(|_| Status::invalid_argument("invalid public key"))?;
        event
            .verify(&public_key)
            .map_err(|_| Status::invalid_argument("invalid signature"))?;

        let event_id = EventId::from(&event);
        match file_sink::write(&self.lora_witness_report_tx, report).await {
            Ok(_) => tracing::debug!(
                "successfully processed witness report, returning response {}",
                event_id.to_string()
            ),
            Err(err) => tracing::error!("failed to store lora witness report: {err:?}"),
        }
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
    let file_upload = file_upload::FileUpload::from_env(file_upload_rx).await?;

    let store_path = dotenv::var("INGEST_STORE")?;
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

    let grpc_server = GrpcServer {
        lora_beacon_report_tx,
        lora_witness_report_tx,
    };

    let api_token = dotenv::var("API_TOKEN").map(|token| {
        format!("Bearer {}", token)
            .parse::<MetadataValue<_>>()
            .unwrap()
    })?;

    tracing::info!(
        "grpc listening on {} and server mode {}",
        grpc_addr,
        server_mode
    );

    let server = transport::Server::builder()
        .add_service(poc_lora::Server::with_interceptor(
            grpc_server,
            move |req: Request<()>| match req.metadata().get("authorization") {
                Some(t) if api_token == t => Ok(req),
                _ => Err(Status::unauthenticated("No valid auth token")),
            },
        ))
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
