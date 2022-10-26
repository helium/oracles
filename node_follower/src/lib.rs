use futures::stream::BoxStream;
use std::time::Duration;

pub mod error;
pub mod follower_service;
pub mod gateway_resp;
pub mod txn_service;
pub use error::{Error, Result};
mod settings;

pub use settings::Settings;
