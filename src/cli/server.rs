use crate::{
    api::{attach_events, gateways, heartbeats, speedtests},
    cli, Error, Follower, Result,
};
use axum::{
    extract::Extension,
    routing::{get, post},
    Router,
};
use futures_util::TryFutureExt;
use std::{io, net::SocketAddr};
use tokio::signal;
use tower_http::{auth::RequireAuthorizationLayer, trace::TraceLayer};

/// Starts the server
#[derive(Debug, clap::Args)]
pub struct Cmd {}

impl Cmd {
    pub async fn run(&self) -> Result {
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
        let follower_uri = dotenv::var("FOLLOWER_URI")?;

        let pool = cli::mk_db_pool(10).await?;
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

        // configure shutdown trigger
        let (shutdown_trigger, shutdown_listener) = triggered::trigger();

        tokio::spawn(async move {
            let _ = signal::ctrl_c().await;
            shutdown_trigger.trigger()
        });

        tracing::info!("listening on {}", addr);

        // api server
        let server_shutdown = shutdown_listener.clone();
        let server = axum::Server::bind(&addr)
            .serve(app.into_make_service())
            .with_graceful_shutdown(async move {
                server_shutdown.await;
                tracing::info!("stopping server")
            })
            .map_err(Error::from);

        // chain follower
        let mut follower = Follower::new(follower_uri.try_into()?, pool.clone())?;

        tokio::try_join!(server, follower.run(shutdown_listener.clone()))?;

        Ok(())
    }
}
