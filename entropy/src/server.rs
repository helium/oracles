use crate::{
    entropy_generator::{Entropy, MessageReceiver},
    Error, Result,
};
use axum::{extract::Extension, http::StatusCode, routing::get, Json, Router};
use futures_util::TryFutureExt;
use serde_json::Value;
use std::{io, net::SocketAddr};
use tokio::sync::watch;
use tower_http::trace::TraceLayer;

pub struct ApiServer {
    pub socket_addr: SocketAddr,
    app: Router,
}

impl ApiServer {
    pub async fn from_env(entropy_watch: MessageReceiver) -> Result<Self> {
        let socket_addr = dotenv::var("API_SOCKET_ADDR").and_then(|v| {
            v.parse::<SocketAddr>().map_err(|_| {
                dotenv::Error::Io(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "invalid api socket address",
                ))
            })
        })?;

        metrics::describe_histogram!(
            "ingest_server_get_entropy_duration",
            "Refers to the duration of fetching cached entropy"
        );

        let app = Router::new()
            // health
            .route("/health", get(empty_handler))
            // entropy
            .route("/entropy", get(get_entropy))
            .layer(poc_metrics::ActiveRequestsLayer::new(
                "entropy_server_connection_count",
            ))
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
) -> std::result::Result<Json<Value>, (StatusCode, String)> {
    let entropy = &*entropy_watch.borrow();
    let json = serde_json::to_value(entropy).map_err(api_error)?;
    Ok(Json(json))
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
