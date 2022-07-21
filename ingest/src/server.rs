use crate::{
    env_var, heartbeat::CellHeartbeat, speedtest::CellSpeedtest, Error, EventId, Result,
    CELL_HEARTBEAT_PREFIX, CELL_SPEEDTEST_PREFIX,
};
use axum::{
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use futures_util::TryFutureExt;
use helium_proto::services::poc_mobile::{
    self, CellHeartbeatReqV1, CellHeartbeatRespV1, SpeedtestReqV1, SpeedtestRespV1,
};
use poc_store::{FileSink, FileSinkBuilder};
use serde_json::{json, Value};
use std::{net::SocketAddr, path::Path, str::FromStr};
use tokio::sync::Mutex;
use tonic::{metadata::MetadataValue, transport, Request, Response, Status};
use tower_http::{auth::RequireAuthorizationLayer, trace::TraceLayer};

async fn empty_handler() {}
fn api_error<E>(err: E) -> (StatusCode, String)
where
    E: std::error::Error,
    Error: From<E>,
{
    Error::from(err).into()
}

pub async fn api_server(shutdown: triggered::Listener) -> Result {
    let api_addr = env_var::<SocketAddr>(
        "API_SOCKET_ADDR",
        SocketAddr::from_str("0.0.0.0:9080").expect("socket address"),
    )?;
    let api_token = dotenv::var("API_TOKEN")?;

    // build our application with some routes
    let app = Router::new()
        // health
        .route("/health", get(empty_handler))
        // attach events
        .route(
            "/cell/speedtest",
            post(create_cell_speedtest).layer(RequireAuthorizationLayer::bearer(&api_token)),
        )
        // heartbeats
        .route(
            "/cell/heartbeats",
            post(create_cell_heartbeat).layer(RequireAuthorizationLayer::bearer(&api_token)),
        )
        .layer(TraceLayer::new_for_http());
    tracing::info!("api listening on {}", api_addr);

    axum::Server::bind(&api_addr)
        .serve(app.into_make_service())
        .with_graceful_shutdown(async move {
            shutdown.await;
            tracing::info!("stopping server")
        })
        .map_err(Error::from)
        .await
}

async fn create_cell_speedtest(
    Json(event): Json<CellSpeedtest>,
) -> std::result::Result<Json<Value>, (StatusCode, String)> {
    EventId::try_from(event)
        .map(|id| json!({ "id": id }))
        .map(Json)
        .map_err(api_error)
}

async fn create_cell_heartbeat(
    Json(event): Json<CellHeartbeat>,
) -> std::result::Result<Json<Value>, (StatusCode, String)> {
    EventId::try_from(event)
        .map(|id| json!({ "id": id }))
        .map(Json)
        .map_err(api_error)
}

pub type GrpcResult<T> = std::result::Result<Response<T>, Status>;

pub struct GrpcServer {
    heartbeat_sink: Mutex<FileSink>,
    speedtest_sink: Mutex<FileSink>,
}

impl GrpcServer {
    pub async fn from_env() -> Result<Self> {
        let store_path = dotenv::var("INGEST_STORE")?;
        let base_path = Path::new(&store_path);
        let heartbeat_sink = FileSinkBuilder::new(CELL_HEARTBEAT_PREFIX, base_path)
            .create()
            .await?;
        let speedtest_sink = FileSinkBuilder::new(CELL_SPEEDTEST_PREFIX, base_path)
            .create()
            .await?;
        Ok(Self {
            heartbeat_sink: Mutex::new(heartbeat_sink),
            speedtest_sink: Mutex::new(speedtest_sink),
        })
    }
}

#[tonic::async_trait]
impl poc_mobile::PocMobile for GrpcServer {
    async fn submit_speedtest(
        &self,
        request: Request<SpeedtestReqV1>,
    ) -> GrpcResult<SpeedtestRespV1> {
        // TODO: Signature verify speedtest_req
        let event = request.into_inner();
        // Encode event digest, encode and return as the id
        let event_id = EventId::from(&event);
        {
            let mut sink = self.speedtest_sink.lock().await;
            match (*sink).write(event).await {
                Ok(_) => (),
                Err(err) => tracing::error!("failed to store heartbeat: {err:?}"),
            }
        }
        Ok(Response::new(event_id.into()))
    }

    async fn submit_cell_heartbeat(
        &self,
        request: Request<CellHeartbeatReqV1>,
    ) -> GrpcResult<CellHeartbeatRespV1> {
        // TODO: Signature verify heartbeat_req
        let event = request.into_inner();
        let event_id = EventId::from(&event);
        {
            let mut sink = self.heartbeat_sink.lock().await;
            match (*sink).write(event).await {
                Ok(_) => (),
                Err(err) => tracing::error!("failed to store heartbeat: {err:?}"),
            }
        }
        // Encode event digest, encode and return as the id
        Ok(Response::new(event_id.into()))
    }
}

pub async fn grpc_server(shutdown: triggered::Listener) -> Result {
    let grpc_addr = env_var::<SocketAddr>(
        "GRPC_SOCKET_ADDR",
        SocketAddr::from_str("0.0.0.0:9081").expect("socket address"),
    )?;

    let poc_mobile = GrpcServer::from_env().await?;
    let api_token = dotenv::var("API_TOKEN").map(|token| {
        format!("Bearer {}", token)
            .parse::<MetadataValue<_>>()
            .unwrap()
    })?;

    tracing::info!("grpc listening on {}", grpc_addr);

    transport::Server::builder()
        .add_service(poc_mobile::Server::with_interceptor(
            poc_mobile,
            move |req: Request<()>| match req.metadata().get("authorization") {
                Some(t) if api_token == t => Ok(req),
                _ => Err(Status::unauthenticated("No valid auth token")),
            },
        ))
        .serve_with_shutdown(grpc_addr, shutdown)
        .map_err(Error::from)
        .await
}
