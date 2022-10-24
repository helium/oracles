use std::time::Duration;

pub mod error;
pub mod follower_service;
pub mod gateway_resp;
pub mod txn_service;
pub use error::{Error, Result};

pub(crate) const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
pub(crate) const RPC_TIMEOUT: Duration = Duration::from_secs(5);
pub(crate) const DEFAULT_URI: &str = "http://127.0.0.1:8080";
