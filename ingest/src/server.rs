use crate::{env_var, error::DecodeError, Error, EventId, PublicKey, Result};
use futures_util::TryFutureExt;
use helium_proto::services::poc_mobile::{
    self, CellHeartbeatReqV1, CellHeartbeatRespV1, SpeedtestReqV1, SpeedtestRespV1,
};
use poc_store::MsgVerify;
use poc_store::{file_sink, file_upload, FileType};
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
        let event = request.into_inner();
        let public_key = PublicKey::try_from(&event.pub_key)
            .map_err(|_| Status::invalid_argument("invalid public key"))?;
        event
            .verify(&public_key)
            .map_err(|_| Status::invalid_argument("invalid signature"))?;
        // Encode event digest, encode and return as the id
        let event_id = EventId::from(&event);
        match file_sink::write(&self.speedtest_tx, event).await {
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
        let event = request.into_inner();
        let public_key = PublicKey::try_from(&event.pub_key)
            .map_err(|_| Status::invalid_argument("invalid public key"))?;
        event
            .verify(&public_key)
            .map_err(|_| Status::invalid_argument("invalid signature"))?;
        let event_id = EventId::from(&event);
        match file_sink::write(&self.heartbeat_tx, event).await {
            Ok(_) => (),
            Err(err) => tracing::error!("failed to store heartbeat: {err:?}"),
        }

        metrics::increment_counter!("ingest_server_heartbeat_count");
        // Encode event digest, encode and return as the id
        Ok(Response::new(event_id.into()))
    }
}

pub async fn grpc_server(shutdown: triggered::Listener) -> Result {
    let grpc_addr: SocketAddr = env_var("GRPC_SOCKET_ADDR")?
        .map_or_else(
            || SocketAddr::from_str("0.0.0.0:9081"),
            |str| SocketAddr::from_str(&str),
        )
        .map_err(DecodeError::from)?;

    // Initialize uploader
    let (file_upload_tx, file_upload_rx) = file_upload::message_channel();
    let file_upload = file_upload::FileUpload::from_env(file_upload_rx).await?;

    let store_path = dotenv::var("INGEST_STORE")?;
    let store_base_path = Path::new(&store_path);

    // heartbeats
    let (heartbeat_tx, heartbeat_rx) = file_sink::message_channel(50);
    let mut heartbeat_sink =
        file_sink::FileSinkBuilder::new(FileType::CellHeartbeat, store_base_path, heartbeat_rx)
            .deposits(Some(file_upload_tx.clone()))
            .create()
            .await?;

    // speedtests
    let (speedtest_tx, speedtest_rx) = file_sink::message_channel(50);
    let mut speedtest_sink =
        file_sink::FileSinkBuilder::new(FileType::CellSpeedtest, store_base_path, speedtest_rx)
            .deposits(Some(file_upload_tx.clone()))
            .create()
            .await?;

    let poc_mobile = GrpcServer {
        speedtest_tx,
        heartbeat_tx,
    };
    let api_token = dotenv::var("API_TOKEN").map(|token| {
        format!("Bearer {}", token)
            .parse::<MetadataValue<_>>()
            .unwrap()
    })?;

    tracing::info!("grpc listening on {}", grpc_addr);

    let server = transport::Server::builder()
        .layer(poc_common::ActiveRequestsLayer::new(
            "ingest_server_grpc_connection_count",
        ))
        .add_service(poc_mobile::Server::with_interceptor(
            poc_mobile,
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
