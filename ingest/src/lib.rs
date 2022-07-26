pub mod attach_event;
mod error;
mod event_id;
mod imsi;
pub mod server;

pub use error::{Error, Result};
pub use event_id::EventId;
pub use imsi::Imsi;
pub use poc_store::public_key::PublicKey;

pub const DEFAULT_STORE_ROLLOVER_SECS: u64 = 30 * 60;

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
