use crate::{
    api::{
        attach_event,
        gateway::{self, Gateway},
        heartbeat,
    },
    datetime_from_epoch, Error, EventId, PublicKey, Result,
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
            "/cell/attach-events",
            post(attach_event::create_cell_attach_event)
                .layer(RequireAuthorizationLayer::bearer(&api_token)),
        )
        // heartbeats
        .route(
            "/cell/heartbeats",
            post(heartbeat::create_cell_heartbeat)
                .layer(RequireAuthorizationLayer::bearer(&api_token)),
        )
        // hotspots
        .route(
            "/hotspots",
            get(gateway::get_gateways).layer(RequireAuthorizationLayer::bearer(&api_ro_token)),
        )
        .route(
            "/hotspots/:pubkey",
            get(gateway::get_gateway).layer(RequireAuthorizationLayer::bearer(&api_ro_token)),
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

fn decode_pubkey(pubkey: &[u8]) -> std::result::Result<PublicKey, Status> {
    PublicKey::try_from(pubkey).map_err(|_err| Status::internal("Failed to decode public key"))
}

#[tonic::async_trait]
impl poc_mobile::PocMobile for GrpcServer {
    async fn submit_speedtest(
        &self,
        request: Request<SpeedtestReqV1>,
    ) -> GrpcResult<SpeedtestRespV1> {
        // TODO: Signature verify speedtest_req
        let event = request.into_inner();
        Gateway::update_last_speedtest(
            &self.pool,
            &decode_pubkey(&event.pub_key)?,
            &datetime_from_epoch(event.timestamp as i64),
        )
        .await
        // Encode event digest, encode and return as the id
        .map(EventId::from)
        .map(|id| Response::new(id.into()))
        .map_err(Status::from)
    }

    async fn submit_cell_heartbeat(
        &self,
        request: Request<CellHeartbeatReqV1>,
    ) -> GrpcResult<CellHeartbeatRespV1> {
        // TODO: Signature verify heartbeat_req
        let event = request.into_inner();
        let pubkey = decode_pubkey(&event.pub_key)?;
        Gateway::update_last_heartbeat(
            &self.pool,
            &pubkey,
            &datetime_from_epoch(event.timestamp as i64),
        )
        .await
        // Encode event digest, encode and return as the id
        .map(EventId::from)
        .map(|id| Response::new(id.into()))
        .map_err(Status::from)
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
