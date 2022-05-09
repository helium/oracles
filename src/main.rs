use axum::{
    async_trait,
    extract::{Extension, FromRequest, Path, RequestParts},
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use poc5g_server::{CellAttachEvent, Result, Uuid};
use serde_json::{json, Value};
use sqlx::postgres::{PgPool, PgPoolOptions};
use std::result::Result as StdResult;
use std::{net::SocketAddr, time::Duration};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> Result {
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new(
            std::env::var("RUST_LOG").unwrap_or_else(|_| "poc5g_server=debug".into()),
        ))
        .with(tracing_subscriber::fmt::layer())
        .init();

    let db_connection_str = std::env::var("DATABASE_URL")?;

    let pool = PgPoolOptions::new()
        .max_connections(10)
        .connect_timeout(Duration::from_secs(3))
        .connect(&db_connection_str)
        .await?;

    sqlx::migrate!().run(&pool).await?;

    // build our application with some routes
    let app = Router::new()
        .route("/cell/attach-events/:id", get(get_cell_attach_event))
        .route("/cell/attach-events", post(create_cell_attach_event))
        .layer(Extension(pool));

    // run it with hyper
    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
    tracing::debug!("listening on {}", addr);
    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await?;
    Ok(())
}

// A custom extractor that grabs a connection from the pool
struct DatabaseConnection(sqlx::pool::PoolConnection<sqlx::Postgres>);

#[async_trait]
impl<B> FromRequest<B> for DatabaseConnection
where
    B: Send,
{
    type Rejection = (StatusCode, String);

    async fn from_request(req: &mut RequestParts<B>) -> StdResult<Self, Self::Rejection> {
        let Extension(pool) = Extension::<PgPool>::from_request(req)
            .await
            .map_err(internal_error)?;

        let conn = pool.acquire().await.map_err(internal_error)?;

        Ok(Self(conn))
    }
}

async fn create_cell_attach_event(
    Json(event): Json<CellAttachEvent>,
    DatabaseConnection(mut conn): DatabaseConnection,
) -> StdResult<Json<Value>, (StatusCode, String)> {
    event
        .insert_into(&mut conn)
        .await
        .map(|id: Uuid| {
            json!({
                "id": id,
            })
        })
        .map(Json)
        .map_err(internal_error)
}

async fn get_cell_attach_event(
    Path(id): Path<Uuid>,
    DatabaseConnection(mut conn): DatabaseConnection,
) -> StdResult<Json<Value>, (StatusCode, String)> {
    let event = CellAttachEvent::get(&mut conn, &id)
        .await
        .map_err(internal_error)?;
    if let Some(event) = event {
        let json = serde_json::to_value(event).map_err(internal_error)?;
        Ok(Json(json))
    } else {
        Err(not_found_error())
    }
}

/// Utility function for mapping any error into a `500 Internal Server Error`
/// response.
fn internal_error<E>(err: E) -> (StatusCode, String)
where
    E: std::error::Error,
{
    (StatusCode::INTERNAL_SERVER_ERROR, err.to_string())
}

/// Utility function returning a not found error
fn not_found_error() -> (StatusCode, String) {
    (StatusCode::NOT_FOUND, "not found".to_string())
}
