use axum::{
    extract::Extension,
    routing::{get, post},
    Router,
};
use poc5g_server::{
    api::{attach_events, heartbeats},
    Result,
};
use sqlx::postgres::PgPoolOptions;
use std::{io, net::SocketAddr, time::Duration};
use tower_http::auth::RequireAuthorizationLayer;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> Result {
    dotenv::dotenv()?;
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new(
            dotenv::var("RUST_LOG").unwrap_or_else(|_| "poc5g_server=debug".into()),
        ))
        .with(tracing_subscriber::fmt::layer())
        .init();

    let db_connection_str = dotenv::var("DATABASE_URL")?;
    let addr = dotenv::var("SOCKET_ADDR").and_then(|v| {
        v.parse::<SocketAddr>().map_err(|_| {
            dotenv::Error::Io(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid socket address",
            ))
        })
    })?;
    let api_token = dotenv::var("API_TOKEN")?;
    let api_ro_token = dotenv::var("API_RO_TOKEN")?;

    let pool = PgPoolOptions::new()
        .max_connections(10)
        .connect_timeout(Duration::from_secs(3))
        .connect(&db_connection_str)
        .await?;

    sqlx::migrate!().run(&pool).await?;

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
        .route("/cell/heartbeats/:id", get(heartbeats::get_cell_hearbeat))
        .route(
            "/cell/heartbeats",
            post(heartbeats::create_cell_heartbeat)
                .layer(RequireAuthorizationLayer::bearer(&api_token)),
        )
        .route(
            "/cell/heartbeats/hotspots/:id/last",
            get(heartbeats::get_hotspot_last_cell_heartbeat)
                .layer(RequireAuthorizationLayer::bearer(&api_ro_token)),
        )
        .route(
            "/cell/heartbeats/hotspots/:id",
            get(heartbeats::get_hotspot_cell_heartbeats)
                .layer(RequireAuthorizationLayer::bearer(&api_ro_token)),
        )
        .layer(Extension(pool));

    // run it with hyper
    tracing::debug!("listening on {}", addr);
    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await?;
    Ok(())
}
