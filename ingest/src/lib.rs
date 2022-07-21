pub mod attach_event;
mod error;
mod event_id;
pub mod heartbeat;
mod imsi;
mod public_key;
pub mod server;
pub mod speedtest;

pub use error::{Error, Result};
pub use event_id::EventId;
pub use imsi::Imsi;
pub use public_key::PublicKey;

pub const CELL_HEARTBEAT_PREFIX: &str = "cell_heartbeat";
pub const CELL_SPEEDTEST_PREFIX: &str = "cell_speedtest";

use chrono::{DateTime, NaiveDateTime, Utc};
use std::io;

pub fn datetime_from_epoch(secs: i64) -> DateTime<Utc> {
    DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(secs, 0), Utc)
}

pub fn env_var<T>(key: &str, default: T) -> Result<T>
where
    T: std::str::FromStr,
    <T as std::str::FromStr>::Err: std::fmt::Debug,
{
    match dotenv::var(key) {
        Ok(v) => v
            .parse::<T>()
            .map_err(|_err| Error::from(io::Error::from(io::ErrorKind::InvalidInput))),
        Err(dotenv::Error::EnvVar(std::env::VarError::NotPresent)) => Ok(default),
        Err(err) => Err(Error::from(err)),
    }
}
