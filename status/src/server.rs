use crate::{
    error::DecodeError,
    gateway::{After, Gateway},
    mk_db_pool, Error, Result,
};
use axum::{
    extract::{Extension, Path, Query},
    http::StatusCode,
    routing::get,
    Json, Router,
};
use futures::TryFutureExt;
use helium_crypto::PublicKey;
use poc_metrics::record_duration;
use serde_json::Value;
use sqlx::PgPool;
use std::net::SocketAddr;
use tower_http::{auth::RequireAuthorizationLayer, trace::TraceLayer};

pub struct Server {
    socket_addr: SocketAddr,
    app: Router,
}

impl Server {
    pub async fn from_env() -> Result<Self> {
        let socket_addr_env =
            std::env::var("API_SOCKET_ADDR").unwrap_or_else(|_| String::from("0.0.0.0:9080"));
        let socket_addr: SocketAddr = socket_addr_env.parse().map_err(DecodeError::from)?;

        let api_ro_token = std::env::var("API_RO_TOKEN")?;
        let pool = mk_db_pool(10).await?;

        metrics::describe_histogram!(
            "status_server_get_gateway_duration",
            "Refers to the duration of fetching a single gateway"
        );
        metrics::describe_histogram!(
            "status_server_get_gateways_duration",
            "Refers to the duration of fetching a list of gateways"
        );

        let app = Router::new()
            // health
            .route("/health", get(empty_handler))
            // hotspots
            .route(
                "/hotspots",
                get(get_gateways).layer(RequireAuthorizationLayer::bearer(&api_ro_token)),
            )
            .route(
                "/hotspots/:pubkey",
                get(get_gateway).layer(RequireAuthorizationLayer::bearer(&api_ro_token)),
            )
            .layer(poc_metrics::ActiveRequestsLayer::new(
                "status_server_connection_count",
            ))
            .layer(TraceLayer::new_for_http())
            .layer(Extension(pool));

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

async fn empty_handler() {}

pub async fn get_gateway(
    Path(pubkey): Path<PublicKey>,
    Extension(pool): Extension<PgPool>,
) -> std::result::Result<Json<Value>, (StatusCode, String)> {
    record_duration!("status_server_get_gateway_duration", {
        let event = Gateway::get(&pool, &pubkey).await.map_err(api_error)?;
        if let Some(event) = event {
            let json = serde_json::to_value(event).map_err(api_error)?;
            Ok(Json(json))
        } else {
            Err(Error::not_found(format!("Gateway {pubkey} not found")).into())
        }
    })
}

pub async fn get_gateways(
    Query(after): Query<After>,
    Extension(pool): Extension<PgPool>,
) -> std::result::Result<Json<Value>, (StatusCode, String)> {
    record_duration!("status_server_get_gateways_duration", {
        let gateways = Gateway::list(&pool, &after).await.map_err(api_error)?;
        let json = serde_json::to_value(gateways).map_err(api_error)?;
        Ok(Json(json))
    })
}

/// Utility function for mapping any error into an api error
pub fn api_error<E>(err: E) -> (StatusCode, String)
where
    E: std::error::Error,
    Error: From<E>,
{
    Error::from(err).into()
}
