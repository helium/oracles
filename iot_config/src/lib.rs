pub mod gateway_service;
pub mod lora_field;
pub mod org;
pub mod org_service;
pub mod region;
pub mod route;
pub mod route_service;
pub mod session_key_service;
pub mod settings;

pub use gateway_service::GatewayService;
pub use org_service::OrgService;
pub use route_service::RouteService;
pub use session_key_service::SessionKeyFilterService;
pub use settings::Settings;

use tokio_stream::wrappers::ReceiverStream;
use tonic::{Response, Status};

pub type GrpcResult<T> = Result<Response<T>, Status>;
pub type GrpcStreamResult<T> = ReceiverStream<Result<T, Status>>;
