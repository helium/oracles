use crate::{
    entropy_generator::{Entropy, MessageReceiver, ENTROPY_TICK_TIME},
    multiplex_service::MultiplexService,
};
use axum::{
    extract::Extension,
    headers::{CacheControl, HeaderMap, HeaderMapExt},
    http::StatusCode,
    response::IntoResponse,
    routing::get,
    Json, Router,
};
use chrono::Utc;
use helium_proto::{
    services::poc_entropy::{EntropyReqV1, PocEntropy, Server as GrpcServer},
    EntropyReportV1,
};
use std::net::SocketAddr;
use tokio::{sync::watch, time::Duration};

struct EntropyServer {
    entropy_watch: MessageReceiver,
}

#[tonic::async_trait]
impl PocEntropy for EntropyServer {
    async fn entropy(
        &self,
        _request: tonic::Request<EntropyReqV1>,
    ) -> Result<tonic::Response<EntropyReportV1>, tonic::Status> {
        let entropy = &*self.entropy_watch.borrow();
        metrics::increment_counter!("entropy_server_get_count");
        Ok(tonic::Response::new(entropy.into()))
    }
}

pub struct ApiServer {
    pub socket_addr: SocketAddr,
    service: MultiplexService<Router, GrpcServer<EntropyServer>>,
}

impl ApiServer {
    pub async fn new(
        socket_addr: SocketAddr,
        entropy_watch: MessageReceiver,
    ) -> anyhow::Result<Self> {
        let rest = Router::new()
            // health
            .route("/health", get(empty_handler))
            // entropy
            .route("/entropy", get(get_entropy))
            .layer(poc_metrics::request_layer!("entropy_request"))
            .layer(Extension(entropy_watch.clone()));

        let grpc = GrpcServer::new(EntropyServer { entropy_watch });

        let service = MultiplexService::new(rest, grpc);

        Ok(Self {
            socket_addr,
            service,
        })
    }

    pub async fn run(self, shutdown: &triggered::Listener) -> anyhow::Result<()> {
        tracing::info!(listen = self.socket_addr.to_string(), "starting");
        axum::Server::bind(&self.socket_addr)
            .serve(tower::make::Shared::new(self.service))
            .with_graceful_shutdown(shutdown.clone())
            .await?;
        tracing::info!("stopping api server");
        Ok(())
    }
}

async fn get_entropy(
    Extension(entropy_watch): Extension<watch::Receiver<Entropy>>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let entropy = &*entropy_watch.borrow();
    let json = serde_json::to_value(entropy)
        .map_err(|e| (http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    metrics::increment_counter!("entropy_server_get_count");
    let mut headers = HeaderMap::new();
    let remaining_age =
        (ENTROPY_TICK_TIME.as_secs() as i64 - (Utc::now().timestamp() - entropy.timestamp)).max(0);
    headers
        .typed_insert(CacheControl::new().with_max_age(Duration::from_secs(remaining_age as u64)));
    Ok((headers, Json(json)))
}

async fn empty_handler() {}
