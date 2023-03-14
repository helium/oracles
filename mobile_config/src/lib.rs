use tokio_stream::wrappers::ReceiverStream;
use tonic::{Response, Status};

pub mod admin_service;
pub mod client;
pub mod hotspot_metadata;
pub mod hotspot_service;
pub mod key_cache;
pub mod router_service;
pub mod settings;

pub use client::{Client, Settings as ClientSettings};

pub type GrpcResult<T> = Result<Response<T>, Status>;
pub type GrpcStreamResult<T> = ReceiverStream<Result<T, Status>>;
