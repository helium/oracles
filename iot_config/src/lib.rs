pub mod admin;
pub mod admin_service;
pub mod client;
pub mod gateway_info;
pub mod gateway_service;
pub mod lora_field;
pub mod org;
pub mod org_service;
pub mod region_map;
pub mod route;
pub mod route_service;
pub mod session_key;
pub mod session_key_service;
pub mod settings;
pub mod telemetry;

pub use admin_service::AdminService;
pub use client::{Client, Settings as ClientSettings};
pub use gateway_service::GatewayService;
use lora_field::{LoraField, NetIdField};
pub use org_service::OrgService;
pub use route_service::RouteService;
pub use session_key_service::SessionKeyFilterService;
pub use settings::Settings;

use helium_crypto::PublicKey;
use tokio::sync::broadcast;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Response, Status};

pub type GrpcResult<T> = Result<Response<T>, Status>;
pub type GrpcStreamResult<T> = ReceiverStream<Result<T, Status>>;
pub type GrpcStreamRequest<T> = tonic::Request<tonic::Streaming<T>>;

pub const HELIUM_NET_ID: NetIdField = LoraField(0x000024);
pub const BROADCAST_CHANNEL_QUEUE: usize = 1024;

pub fn update_channel<T: Clone>() -> broadcast::Sender<T> {
    let (update_tx, _) = broadcast::channel(BROADCAST_CHANNEL_QUEUE);
    update_tx
}

pub async fn broadcast_update<T>(
    message: T,
    sender: broadcast::Sender<T>,
) -> Result<(), broadcast::error::SendError<T>> {
    while !enqueue_update(sender.len()) {
        tokio::time::sleep(tokio::time::Duration::from_millis(25)).await
    }
    sender.send(message).map(|_| ())
}

fn enqueue_update(queue_size: usize) -> bool {
    // enqueue the message for broadcast if
    // the current queue is <= 80% full
    (queue_size * 100) / BROADCAST_CHANNEL_QUEUE <= 80
}

pub fn verify_public_key(bytes: &[u8]) -> Result<PublicKey, Status> {
    PublicKey::try_from(bytes)
        .map_err(|_| Status::invalid_argument(format!("invalid public key: {bytes:?}")))
}
