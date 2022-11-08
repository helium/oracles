use crate::error::{Error, Result};
use config::{Config, Environment, File};
use serde::Deserialize;
use std::{io, path::Path, str::FromStr};

#[derive(Clone, Debug, Deserialize)]
pub struct Calculations {
    /// How much to credit a gateway's owner for providing the network.
    /// This value gets multiplied by the count of IoT packets that
    /// traversed the associated gateways.
    pub rewards_multiplier: u64,
    pub rewards_units: Units,

    /// How much to debit from a gateway's owner for using the network.
    /// This value gets multiplied by the count of IoT packets that
    /// from devices addressing the associated OUI's destinations.
    pub debits_multiplier: u64,
    pub debits_units: Units,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all="UPPERCASE")]
pub enum Units {
    DC,                         // Data Credits
    HNT,                        // in units of Bones
    IOT,
    MOBILE,
}

#[derive(Clone, Debug, Deserialize)]
pub struct Settings {
    /// RUST_LOG compatible setings string.
    #[serde(default="default_log")]
    pub log: String,
    pub calculations: Calculations,
    pub ingest: file_store::Settings,
    // FIXME pub follower: file_store::Settings,
    pub rewards: file_store::Settings,
    pub debits: file_store::Settings,
    // FIXME pub metrics: file_store::Settings,
}

/// Env vars have same name as entries in the settings.toml file
/// but in uppercase and prefixed as specified by this Rust var's
/// value.
pub const ENV_VAR_PREFIX: &str = "IOT_PACKET_VERIFIER";

pub const DC: &str = "DC";
pub const HNT: &str = "HNT";
pub const IOT: &str = "IOT";
pub const MOBILE: &str = "MOBILE";

pub fn default_log() -> String {
    "iot_packet_verifier=debug,file_store=info".to_string()
}

impl Settings {
    /// Load settings from a given path.  Settings may be loaded via
    /// optional path and be overridden using environment variables.
    ///
    /// Env vars have same name as entries in the settings.toml file
    /// but in uppercase and prefixed as specified by the value of
    /// const &str `ENV_VAR_PREFIX`.
    pub fn new<P: AsRef<Path>>(path: Option<P>) -> Result<Self> {
        let mut builder = Config::builder();

        if let Some(file) = path {
            // Add optional settings file
            builder = builder
                .add_source(File::with_name(&file.as_ref().to_string_lossy()).required(false));
        }
        // Add settings from env vars
        builder
            .add_source(Environment::with_prefix(ENV_VAR_PREFIX).separator("_"))
            .build()
            .and_then(|config| config.try_deserialize())
            .map_err(Error::from)
    }
}

impl FromStr for Units {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self> {
        let result = match s.to_uppercase().as_str() {
            DC => Self::DC,
            HNT => Self::HNT,
            IOT => Self::IOT,
            MOBILE => Self::MOBILE,
            _ => return Err(Error::from(io::Error::from(io::ErrorKind::InvalidInput)))
        };
        Ok(result)
    }
}
