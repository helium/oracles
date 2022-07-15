pub mod api;
mod cell_type;
pub mod cli;
mod error;
mod event_id;
mod follower;
mod imsi;
pub mod maker;
mod public_key;
pub mod rewards;
pub mod store;
mod uuid;

pub use cell_type::CellType;
pub use error::{Error, Result};
pub use event_id::EventId;
pub use follower::Follower;
pub use imsi::Imsi;
pub use public_key::PublicKey;
pub use uuid::Uuid;

use chrono::{DateTime, NaiveDateTime, Utc};
use std::io;

pub fn datetime_from_epoch(secs: i64) -> DateTime<Utc> {
    DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(secs, 0), Utc)
}

fn env_var<T>(key: &str, default: T) -> Result<T>
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
