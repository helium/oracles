pub mod attach_event;
pub mod gateway;
pub mod heartbeat;
pub mod server;
pub mod speedtest;

use crate::Error;
use axum::{
    async_trait,
    extract::{Extension, FromRequest, RequestParts},
    http::StatusCode,
};
use sqlx::postgres::PgPool;

/// Utility function for mapping any error into an api error
pub fn api_error<E>(err: E) -> (StatusCode, String)
where
    E: std::error::Error,
    Error: From<E>,
{
    Error::from(err).into()
}

// A custom extractor that grabs a connection from the pool
pub struct DatabaseConnection(sqlx::pool::PoolConnection<sqlx::Postgres>);

#[async_trait]
impl<B> FromRequest<B> for DatabaseConnection
where
    B: Send,
{
    type Rejection = (StatusCode, String);

    async fn from_request(req: &mut RequestParts<B>) -> std::result::Result<Self, Self::Rejection> {
        let Extension(pool) = Extension::<PgPool>::from_request(req)
            .await
            .map_err(api_error)?;

        let conn = pool.acquire().await.map_err(api_error)?;

        Ok(Self(conn))
    }
}
