use crate::{
    api::{attach_events, gateways, heartbeats, speedtests},
    datetime_from_epoch, CellHeartbeat, CellSpeedtest, Error, PublicKey, Result, Uuid,
};
use axum::{
    extract::Extension,
    routing::{get, post},
    Router,
};
use futures_util::TryFutureExt;
use helium_proto::services::poc_mobile::{
    self, CellHeartbeatReqV1, CellHeartbeatRespV1, SpeedtestReqV1, SpeedtestRespV1,
};
use sqlx::{Pool, Postgres};
use std::{io, net::SocketAddr};
use tonic::{metadata::MetadataValue, transport, Request, Response, Status};
use tower_http::{auth::RequireAuthorizationLayer, trace::TraceLayer};

pub async fn api_server(pool: Pool<Postgres>, shutdown: triggered::Listener) -> Result {
    let api_addr = dotenv::var("API_SOCKET_ADDR").and_then(|v| {
        v.parse::<SocketAddr>().map_err(|_| {
            dotenv::Error::Io(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid api socket address",
            ))
        })
    })?;
    let api_token = dotenv::var("API_TOKEN")?;
    let api_ro_token = dotenv::var("API_RO_TOKEN")?;

    // build our application with some routes
    let app = Router::new()
        .route(
            "/cell/attach-events/:id",
            get(attach_events::get_cell_attach_event),
        )
        .route(
            "/cell/attach-events",
            post(attach_events::create_cell_attach_event)
                .layer(RequireAuthorizationLayer::bearer(&api_token)),
        )
        // heartbeats
        .route(
            "/cell/heartbeats/hotspots/:id/last",
            get(heartbeats::get_hotspot_last_cell_heartbeat)
                .layer(RequireAuthorizationLayer::bearer(&api_ro_token)),
        )
        .route("/cell/heartbeats/:id", get(heartbeats::get_cell_hearbeat))
        .route(
            "/cell/heartbeats",
            post(heartbeats::create_cell_heartbeat)
                .layer(RequireAuthorizationLayer::bearer(&api_token)),
        )
        .route(
            "/cell/heartbeats/hotspots/:id",
            get(heartbeats::get_hotspot_cell_heartbeats)
                .layer(RequireAuthorizationLayer::bearer(&api_ro_token)),
        )
        // speedtests
        .route("/cell/speedtests/:id", get(speedtests::get_cell_speedtest))
        .route(
            "/cell/speedtests",
            post(speedtests::create_cell_speedtest)
                .layer(RequireAuthorizationLayer::bearer(&api_token)),
        )
        .route(
            "/cell/speedtests/hotspots/:id/last",
            get(speedtests::get_hotspot_last_cell_speedtest)
                .layer(RequireAuthorizationLayer::bearer(&api_ro_token)),
        )
        .route(
            "/cell/speedtests/hotspots/:id",
            get(speedtests::get_hotspot_cell_speedtests)
                .layer(RequireAuthorizationLayer::bearer(&api_ro_token)),
        )
        // hotspots
        .route(
            "/hotspots",
            get(gateways::get_gateways).layer(RequireAuthorizationLayer::bearer(&api_ro_token)),
        )
        .route(
            "/hotspots/:pubkey",
            get(gateways::get_gateway).layer(RequireAuthorizationLayer::bearer(&api_ro_token)),
        )
        .layer(TraceLayer::new_for_http())
        .layer(Extension(pool.clone()));
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

pub type GrpcResult<T> = std::result::Result<Response<T>, Status>;

#[derive(Clone)]
pub struct GrpcServer {
    pool: Pool<Postgres>,
}

impl GrpcServer {
    pub fn new(pool: Pool<Postgres>) -> Self {
        Self { pool }
    }
}

impl TryFrom<SpeedtestReqV1> for CellSpeedtest {
    type Error = Error;
    fn try_from(v: SpeedtestReqV1) -> Result<Self> {
        Ok(Self {
            pubkey: PublicKey::try_from(v.pub_key.as_ref())?,
            serial: v.serial,
            timestamp: datetime_from_epoch(v.timestamp as i64),
            upload_speed: v.upload_speed as i64,
            download_speed: v.download_speed as i64,
            latency: v.latency as i32,
            id: Uuid::nil(),
            created_at: None,
        })
    }
}

impl TryFrom<CellHeartbeatReqV1> for CellHeartbeat {
    type Error = Error;
    fn try_from(v: CellHeartbeatReqV1) -> Result<Self> {
        Ok(Self {
            pubkey: PublicKey::try_from(v.pub_key.as_ref())?,
            hotspot_type: v.hotspot_type,
            cell_id: v.cell_id as i32,
            timestamp: datetime_from_epoch(v.timestamp as i64),
            lon: v.lon,
            lat: v.lat,
            operation_mode: v.operation_mode,
            cbsd_category: v.cbsd_category,
            cbsd_id: v.cbsd_id,

            id: Uuid::nil(),
            created_at: None,
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
        let event = CellSpeedtest::try_from(request.into_inner())
            .map_err(|_err| Status::internal("Failed to decode event"))?;
        let id = event
            .insert_into(&self.pool)
            .await
            .map_err(|_err| Status::internal("Failed to insert event"))?;
        Ok(Response::new(SpeedtestRespV1 { id: id.to_string() }))
    }

    async fn submit_cell_heartbeat(
        &self,
        request: Request<CellHeartbeatReqV1>,
    ) -> GrpcResult<CellHeartbeatRespV1> {
        // TODO: Signature verify heartbeat_req
        let event = CellHeartbeat::try_from(request.into_inner())
            .map_err(|_err| Status::internal("Failed to decode event"))?;
        let id = event
            .insert_into(&self.pool)
            .await
            .map_err(|_err| Status::internal("Failed to insert event"))?;
        Ok(Response::new(CellHeartbeatRespV1 { id: id.to_string() }))
    }
}

pub async fn grpc_server(pool: Pool<Postgres>, shutdown: triggered::Listener) -> Result {
    let grpc_addr = dotenv::var("GRPC_SOCKET_ADDR").and_then(|v| {
        v.parse::<SocketAddr>().map_err(|_| {
            dotenv::Error::Io(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid grpc socket address",
            ))
        })
    })?;

    let poc_mobile = GrpcServer::new(pool);
    let api_token = dotenv::var("API_TOKEN").map(|token| {
        format!("Bearer {}", token)
            .parse::<MetadataValue<_>>()
            .unwrap()
    })?;

    tracing::info!("grpc listening on {}", grpc_addr);

    transport::Server::builder()
        .add_service(poc_mobile::Server::with_interceptor(
            poc_mobile,
            move |req: Request<()>| match req.metadata().get("Authorization") {
                Some(t) if api_token == t => Ok(req),
                _ => Err(Status::unauthenticated("No valid auth token")),
            },
        ))
        .serve_with_shutdown(grpc_addr, shutdown)
        .map_err(Error::from)
        .await
}
