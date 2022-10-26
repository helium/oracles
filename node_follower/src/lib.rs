use futures::stream::BoxStream;
use std::time::Duration;

pub mod error;
pub mod follower_service;
pub mod gateway_resp;
pub mod txn_service;
pub use error::{Error, Result};

pub(crate) const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
pub(crate) const RPC_TIMEOUT: Duration = Duration::from_secs(5);
pub(crate) const DEFAULT_STREAM_BATCH_SIZE: u32 = 1000;
pub(crate) const DEFAULT_URI: &str = "http://127.0.0.1:8080";

pub type Stream<T> = BoxStream<'static, T>;

pub type GatewayInfoStream = Stream<gateway_resp::GatewayInfo>;
