pub mod admin;
pub mod admin_service;
pub mod gateway_service;
pub mod lora_field;
pub mod org;
pub mod org_service;
pub mod region_map;
pub mod route;
pub mod route_service;
pub mod session_key_service;
pub mod settings;

pub use admin_service::AdminService;
pub use gateway_service::GatewayService;
use lora_field::{LoraField, NetIdField};
pub use org_service::OrgService;
pub use route_service::RouteService;
pub use session_key_service::SessionKeyFilterService;
pub use settings::Settings;

use tokio_stream::wrappers::ReceiverStream;
use tonic::{Response, Status};

pub type GrpcResult<T> = Result<Response<T>, Status>;
pub type GrpcStreamResult<T> = ReceiverStream<Result<T, Status>>;
pub type GrpcStreamRequest<T> = tonic::Request<tonic::Streaming<T>>;

pub const HELIUM_NET_ID: NetIdField = LoraField(0x000024);
