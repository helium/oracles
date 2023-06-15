use helium_crypto::PublicKey;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Response, Status};

pub mod admin_service;
pub mod authorization_service;
pub mod client;
pub mod gateway_info;
pub mod gateway_service;
pub mod key_cache;
pub mod settings;
pub mod telemetry;

pub use client::{GatewayClient, Settings as ClientSettings};

pub type GrpcResult<T> = Result<Response<T>, Status>;
pub type GrpcStreamResult<T> = ReceiverStream<Result<T, Status>>;

pub fn verify_public_key(bytes: &[u8]) -> Result<PublicKey, Status> {
    PublicKey::try_from(bytes).map_err(|_| Status::invalid_argument("invalid public key"))
}
