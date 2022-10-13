pub mod attach_event;
mod error;
mod event_id;
mod imsi;
pub mod server_5g;
pub mod server_lora;

pub use error::{Error, Result};
pub use event_id::EventId;
pub use imsi::Imsi;

use helium_crypto::Network;
use std::{env, str::FromStr};

pub fn required_network() -> Result<Network> {
    env::var("REQUIRED_NETWORK")
        .map_or_else(|_| Ok(Network::MainNet), |value| Network::from_str(&value))
        .map_err(Error::from)
}
