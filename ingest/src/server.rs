use crate::{env_var, Error, EventId, Result, DEFAULT_STORE_ROLLOVER_SECS};
use axum::{
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use futures_util::TryFutureExt;
use helium_proto::services::poc_mobile::{
    self, CellHeartbeatReqV1, CellHeartbeatRespV1, SpeedtestReqV1, SpeedtestRespV1,
};
use poc_store::{heartbeat::CellHeartbeat, speedtest::CellSpeedtest};
use poc_store::{FileSink, FileSinkBuilder, FileType};
use serde_json::{json, Value};
use std::{net::SocketAddr, path::Path, str::FromStr, sync::Arc, time::Duration};
use tokio::{sync::Mutex, time};
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
    sinks: FileSinks,
}

#[derive(Clone)]
pub struct FileSinks {
    heartbeat_sink: Arc<Mutex<FileSink>>,
    speedtest_sink: Arc<Mutex<FileSink>>,
}

impl FileSinks {
    pub async fn with_base_path(base_path: &Path) -> Result<Self> {
        let heartbeat_sink = FileSinkBuilder::new(FileType::CellHeartbeat, base_path)
            .create()
            .await?;
        let speedtest_sink = FileSinkBuilder::new(FileType::CellSpeedtest, base_path)
            .create()
            .await?;
        Ok(Self {
            heartbeat_sink: Arc::new(Mutex::new(heartbeat_sink)),
            speedtest_sink: Arc::new(Mutex::new(speedtest_sink)),
        })
    }

    async fn shutdown(&self) {
        (*self.heartbeat_sink.lock().await).shutdown().await;
        (*self.speedtest_sink.lock().await).shutdown().await;
    }

    async fn maybe_roll(&self, duration: &Duration) -> Result {
        (*self.heartbeat_sink.lock().await)
            .maybe_roll(duration)
            .await?;
        (*self.speedtest_sink.lock().await)
            .maybe_roll(duration)
            .await?;
        Ok(())
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
            let mut sink = self.sinks.speedtest_sink.lock().await;
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
            let mut sink = self.sinks.heartbeat_sink.lock().await;
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

    let store_path = dotenv::var("INGEST_STORE")?;
    let store_base_path = Path::new(&store_path);
    let sinks = FileSinks::with_base_path(store_base_path).await?;

    let poc_mobile = GrpcServer {
        sinks: sinks.clone(),
    };
    let api_token = dotenv::var("API_TOKEN").map(|token| {
        format!("Bearer {}", token)
            .parse::<MetadataValue<_>>()
            .unwrap()
    })?;

    tracing::info!("grpc listening on {}", grpc_addr);

    let server = transport::Server::builder()
        .add_service(poc_mobile::Server::with_interceptor(
            poc_mobile,
            move |req: Request<()>| match req.metadata().get("authorization") {
                Some(t) if api_token == t => Ok(req),
                _ => Err(Status::unauthenticated("No valid auth token")),
            },
        ))
        .serve_with_shutdown(grpc_addr, shutdown)
        .map_err(Error::from);

    // Initialize time based rollover interval
    let store_roll_time = Duration::from_secs(env_var::<u64>(
        "STORE_ROLLOVER_SECS",
        DEFAULT_STORE_ROLLOVER_SECS,
    )?);
    let mut rollover_timer = time::interval(store_roll_time);
    rollover_timer.set_missed_tick_behavior(time::MissedTickBehavior::Delay);

    tokio::pin!(server);

    loop {
        tokio::select! {
            result = &mut server => {
                sinks.shutdown().await;
                return result
            },
            _ = rollover_timer.tick() => match sinks.maybe_roll(&store_roll_time).await {
                Ok(()) => (),
                Err(err) => {
                    tracing::error!("failed to roll file sinks: {err:?}");
                    return Err(err)
                }
            }
        }
    }
}
