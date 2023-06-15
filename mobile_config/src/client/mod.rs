pub mod authorization_client;
pub mod entity_client;
pub mod gateway_client;
mod settings;

use std::time::Duration;

pub use gateway_client::GatewayClient;
pub use settings::Settings;

const CACHE_EVICTION_FREQUENCY: Duration = Duration::from_secs(60 * 60);

#[derive(thiserror::Error, Debug)]
pub enum ClientError {
    #[error("error signing request {0}")]
    SigningError(#[from] helium_crypto::Error),
    #[error("grpc error response {0}")]
    GrpcError(#[from] tonic::Status),
    #[error("error verifying response signature {0}")]
    VerificationError(#[from] file_store::Error),
}
