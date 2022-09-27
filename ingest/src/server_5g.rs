use crate::{error::DecodeError, Error, EventId, Result};
use chrono::Utc;
use file_store::traits::MsgVerify;
use file_store::{file_sink, file_upload, FileType};
use futures_util::TryFutureExt;
use helium_crypto::PublicKey;
use helium_proto::services::poc_mobile::{
    self, CellHeartbeatIngestReportV1, CellHeartbeatReqV1, CellHeartbeatRespV1,
    SpeedtestIngestReportV1, SpeedtestReqV1, SpeedtestRespV1,
};
use std::env;
use std::{net::SocketAddr, path::Path, str::FromStr};
use tonic::{metadata::MetadataValue, transport, Request, Response, Status};

pub type GrpcResult<T> = std::result::Result<Response<T>, Status>;

pub struct GrpcServer {
    heartbeat_tx: file_sink::MessageSender,
    speedtest_tx: file_sink::MessageSender,
}

#[tonic::async_trait]
impl poc_mobile::PocMobile for GrpcServer {
    async fn submit_speedtest(
        &self,
        request: Request<SpeedtestReqV1>,
    ) -> GrpcResult<SpeedtestRespV1> {
        let timestamp = Utc::now().timestamp_millis() as u64;
        let event = request.into_inner();
        let public_key = PublicKey::try_from(event.pub_key.as_ref())
            .map_err(|_| Status::invalid_argument("invalid public key"))?;
        event
            .verify(&public_key)
            .map_err(|_| Status::invalid_argument("invalid signature"))?;
        // Encode event digest, encode and return as the id
        let event_id = EventId::from(&event);

        let report = SpeedtestIngestReportV1 {
            report: Some(event),
            received_timestamp: timestamp,
        };

        match file_sink::write(&self.speedtest_tx, report).await {
            Ok(_) => (),
            Err(err) => tracing::error!("failed to store speedtest: {err:?}"),
        }

        metrics::increment_counter!("ingest_server_speedtest_count");
        Ok(Response::new(event_id.into()))
    }

    async fn submit_cell_heartbeat(
        &self,
        request: Request<CellHeartbeatReqV1>,
    ) -> GrpcResult<CellHeartbeatRespV1> {
        let timestamp = Utc::now().timestamp_millis() as u64;
        let event = request.into_inner();
        let public_key = PublicKey::try_from(event.pub_key.as_slice())
            .map_err(|_| Status::invalid_argument("invalid public key"))?;
        event
            .verify(&public_key)
            .map_err(|_| Status::invalid_argument("invalid signature"))?;
        let event_id = EventId::from(&event);

        let report = CellHeartbeatIngestReportV1 {
            report: Some(event),
            received_timestamp: timestamp,
        };

        match file_sink::write(&self.heartbeat_tx, report).await {
            Ok(_) => (),
            Err(err) => tracing::error!("failed to store heartbeat: {err:?}"),
        }

        metrics::increment_counter!("ingest_server_heartbeat_count");
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

    let store_path = std::env::var("INGEST_STORE")?;
    let store_base_path = Path::new(&store_path);

    let (heartbeat_tx, heartbeat_rx) = file_sink::message_channel(50);
    let mut heartbeat_sink = file_sink::FileSinkBuilder::new(
        FileType::CellHeartbeatIngestReport,
        store_base_path,
        heartbeat_rx,
    )
    .deposits(Some(file_upload_tx.clone()))
    .create()
    .await?;

    // speedtests
    let (speedtest_tx, speedtest_rx) = file_sink::message_channel(50);
    let mut speedtest_sink = file_sink::FileSinkBuilder::new(
        FileType::CellSpeedtestIngestReport,
        store_base_path,
        speedtest_rx,
    )
    .deposits(Some(file_upload_tx.clone()))
    .create()
    .await?;

    let grpc_server = GrpcServer {
        speedtest_tx,
        heartbeat_tx,
    };
    let api_token = std::env::var("API_TOKEN").map(|token| {
        format!("Bearer {}", token)
            .parse::<MetadataValue<_>>()
            .unwrap()
    })?;

    tracing::info!(
        "grpc listening on {} and server mode {}",
        grpc_addr,
        server_mode
    );

    //TODO start a service with either the poc mobile or poc lora endpoints only - not both
    //     use _server_mode (set above ) to decide
    let server = transport::Server::builder()
        .layer(poc_metrics::ActiveRequestsLayer::new(
            "ingest_server_grpc_connection_count",
        ))
        .add_service(poc_mobile::Server::with_interceptor(
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
        heartbeat_sink.run(&shutdown).map_err(Error::from),
        speedtest_sink.run(&shutdown).map_err(Error::from),
        file_upload.run(&shutdown).map_err(Error::from),
    )
    .map(|_| ())
}
