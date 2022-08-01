pub mod attach_event;
mod error;
mod event_id;
mod imsi;
pub mod server;

pub use error::{Error, Result};
pub use event_id::EventId;
pub use imsi::Imsi;
pub use poc_store::public_key::PublicKey;

use chrono::{DateTime, NaiveDateTime, Utc};

pub fn datetime_from_epoch(secs: i64) -> DateTime<Utc> {
    DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(secs, 0), Utc)
}

use std::env;

pub fn env_var(key: &str) -> Result<Option<String>> {
    match env::var(key) {
        Ok(v) => Ok(Some(v)),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(err) => Err(Error::from(err)),
    }
}
