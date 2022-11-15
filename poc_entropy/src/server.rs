use crate::{
    entropy_generator::{Entropy, MessageReceiver, ENTROPY_TICK_TIME},
    Error, Result,
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
use futures_util::TryFutureExt;
use std::net::SocketAddr;
use tokio::{sync::watch, time::Duration};
use tower_http::trace::TraceLayer;

pub struct ApiServer {
    pub socket_addr: SocketAddr,
    app: Router,
}

impl ApiServer {
    pub async fn new(socket_addr: SocketAddr, entropy_watch: MessageReceiver) -> Result<Self> {
        let app = Router::new()
            // health
            .route("/health", get(empty_handler))
            // entropy
            .route("/entropy", get(get_entropy))
            .layer(poc_metrics::request_layer!("entropy_request"))
            .layer(TraceLayer::new_for_http())
            .layer(Extension(entropy_watch));

        Ok(Self { socket_addr, app })
    }

    pub async fn run(self, shutdown: &triggered::Listener) -> Result {
        tracing::info!("starting api server");
        let result = axum::Server::bind(&self.socket_addr)
            .serve(self.app.into_make_service())
            .with_graceful_shutdown(shutdown.clone())
            .map_err(Error::from)
            .await;
        tracing::info!("stopping api server");
        result
    }
}

async fn get_entropy(
    Extension(entropy_watch): Extension<watch::Receiver<Entropy>>,
) -> std::result::Result<impl IntoResponse, (StatusCode, String)> {
    let entropy = &*entropy_watch.borrow();
    let json = serde_json::to_value(entropy).map_err(api_error)?;
    metrics::increment_counter!("entropy_server_get_count");
    let mut headers = HeaderMap::new();
    let remaining_age =
        (ENTROPY_TICK_TIME.as_secs() as i64 - (Utc::now().timestamp() - entropy.timestamp)).max(0);
    headers
        .typed_insert(CacheControl::new().with_max_age(Duration::from_secs(remaining_age as u64)));
    Ok((headers, Json(json)))
}

async fn empty_handler() {}

/// Utility function for mapping any error into an api error
pub fn api_error<E>(err: E) -> (StatusCode, String)
where
    E: std::error::Error,
    Error: From<E>,
{
    Error::from(err).into()
}
