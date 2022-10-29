pub mod attach_event;
mod error;
mod event_id;
mod imsi;
pub mod server_5g;
pub mod server_lora;
pub mod settings;

pub use error::{Error, Result};
pub use event_id::EventId;
pub use imsi::Imsi;
pub use settings::{Mode, Settings};

use helium_crypto::Network;
use std::{env, str::FromStr};

pub fn required_network() -> Result<Network> {
    env::var("REQUIRED_NETWORK")
        .map_or_else(|_| Ok(Network::MainNet), |value| Network::from_str(&value))
        .map_err(Error::from)
}
