pub mod attach_event;
pub mod gateway;
pub mod heartbeat;
pub mod server;
pub mod speedtest;

use crate::{follower::FollowerService, Error};
use axum::{
    async_trait,
    extract::{Extension, FromRequest, RequestParts},
    http::StatusCode,
};

/// Utility function for mapping any error into an api error
pub fn api_error<E>(err: E) -> (StatusCode, String)
where
    E: std::error::Error,
    Error: From<E>,
{
    Error::from(err).into()
}

pub struct Follower(FollowerService);

#[async_trait]
impl<B> FromRequest<B> for Follower
where
    B: Send,
{
    type Rejection = (StatusCode, String);

    async fn from_request(req: &mut RequestParts<B>) -> std::result::Result<Self, Self::Rejection> {
        let Extension(follower) = Extension::<FollowerService>::from_request(req)
            .await
            .map_err(api_error)?;

        Ok(Self(follower))
    }
}
