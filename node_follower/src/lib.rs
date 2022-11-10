pub mod error;
pub mod follower_service;
pub mod gateway_resp;
pub mod txn_service;
pub use error::{Error, Result};
mod settings;

use futures::stream::BoxStream;
pub use settings::Settings;

pub type Stream<T> = BoxStream<'static, T>;
pub type GatewayInfoStream = Stream<gateway_resp::GatewayInfo>;
