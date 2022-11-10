use crate::{Error, EventId, Result, Settings};
use chrono::Utc;
use file_store::traits::MsgVerify;
use file_store::{file_sink, file_sink_write, file_upload, FileType};
use futures_util::TryFutureExt;
use helium_crypto::{Network, PublicKey};
use helium_proto::services::poc_mobile::{
    self, CellHeartbeatIngestReportV1, CellHeartbeatReqV1, CellHeartbeatRespV1,
    SpeedtestIngestReportV1, SpeedtestReqV1, SpeedtestRespV1,
};
use std::path::Path;
use tonic::{metadata::MetadataValue, transport, Request, Response, Status};

pub type GrpcResult<T> = std::result::Result<Response<T>, Status>;

pub struct GrpcServer {
    heartbeat_report_tx: file_sink::MessageSender,
    speedtest_report_tx: file_sink::MessageSender,
    required_network: Network,
}

impl GrpcServer {
    fn new(
        heartbeat_report_tx: file_sink::MessageSender,
        speedtest_report_tx: file_sink::MessageSender,
        required_network: Network,
    ) -> Result<Self> {
        Ok(Self {
            heartbeat_report_tx,
            speedtest_report_tx,
            required_network,
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
impl poc_mobile::PocMobile for GrpcServer {
    async fn submit_speedtest(
        &self,
        request: Request<SpeedtestReqV1>,
    ) -> GrpcResult<SpeedtestRespV1> {
        let timestamp = Utc::now().timestamp_millis() as u64;
        let event = request.into_inner();

        let public_key = PublicKey::try_from(event.pub_key.as_ref())
            .map_err(|_| Status::invalid_argument("invalid public key"))?;

        self.verify_network(&public_key)?;

        event
            .verify(&public_key)
            .map_err(|_| Status::invalid_argument("invalid signature"))?;
        // Encode event digest, encode and return as the id
        let event_id = EventId::from(&event);

        let report = SpeedtestIngestReportV1 {
            report: Some(event),
            received_timestamp: timestamp,
        };

        _ = file_sink_write!("speedtest_report", &self.speedtest_report_tx, report).await;
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

        self.verify_network(&public_key)?;

        event
            .verify(&public_key)
            .map_err(|_| Status::invalid_argument("invalid signature"))?;
        let event_id = EventId::from(&event);

        let report = CellHeartbeatIngestReportV1 {
            report: Some(event),
            received_timestamp: timestamp,
        };

        _ = file_sink_write!("heartbeat_report", &self.heartbeat_report_tx, report).await;
        metrics::increment_counter!("ingest_server_heartbeat_count");
        // Encode event digest, encode and return as the id
        Ok(Response::new(event_id.into()))
    }
}

pub async fn grpc_server(shutdown: triggered::Listener, settings: &Settings) -> Result {
    let grpc_addr = settings.listen_addr()?;

    // Initialize uploader
    let (file_upload_tx, file_upload_rx) = file_upload::message_channel();
    let file_upload =
        file_upload::FileUpload::from_settings(&settings.output, file_upload_rx).await?;

    let store_base_path = Path::new(&settings.cache);

    let (heartbeat_report_tx, heartbeat_report_rx) = file_sink::message_channel(50);
    let mut heartbeat_report_sink = file_sink::FileSinkBuilder::new(
        FileType::CellHeartbeatIngestReport,
        store_base_path,
        heartbeat_report_rx,
    )
    .deposits(Some(file_upload_tx.clone()))
    .create()
    .await?;

    // speedtests
    let (speedtest_report_tx, speedtest_report_rx) = file_sink::message_channel(50);
    let mut speedtest_report_sink = file_sink::FileSinkBuilder::new(
        FileType::CellSpeedtestIngestReport,
        store_base_path,
        speedtest_report_rx,
    )
    .deposits(Some(file_upload_tx.clone()))
    .create()
    .await?;

    let grpc_server = GrpcServer::new(heartbeat_report_tx, speedtest_report_tx, settings.network)?;

    let api_token = settings
        .token
        .as_ref()
        .map(|token| {
            format!("Bearer {}", token)
                .parse::<MetadataValue<_>>()
                .unwrap()
        })
        .ok_or_else(|| Error::not_found("expected api token in settings"))?;

    tracing::info!(
        "grpc listening on {grpc_addr} and server mode {:?}",
        settings.mode
    );

    //TODO start a service with either the poc mobile or poc lora endpoints only - not both
    //     use _server_mode (set above ) to decide
    let server = transport::Server::builder()
        .layer(poc_metrics::request_layer!("ingest_server_grpc_connection"))
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
        heartbeat_report_sink.run(&shutdown).map_err(Error::from),
        speedtest_report_sink.run(&shutdown).map_err(Error::from),
        file_upload.run(&shutdown).map_err(Error::from),
    )
    .map(|_| ())
}
