use std::time::{SystemTime, UNIX_EPOCH};

use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};
use helium_crypto::Keypair;

pub mod docker;
pub mod hotspot;

trait TimestampToDateTime {
    fn to_datetime(&self) -> DateTime<Utc>;
}
impl TimestampToDateTime for u64 {
    fn to_datetime(&self) -> DateTime<Utc> {
        // Convert the u64 timestamp in milliseconds to NaiveDateTime
        let naive = NaiveDateTime::from_timestamp_millis(*self as i64).expect("Invalid timestamp");

        // Convert NaiveDateTime to DateTime<Utc> using Utc timestamp
        Utc.from_utc_datetime(&naive)
    }
}

pub fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

pub fn keypair_to_bs58(keypair: &Keypair) -> String {
    bs58::encode(keypair.public_key().to_vec()).into_string()
}

pub fn hours_ago(hours: i64) -> u64 {
    chrono::Duration::hours(hours).num_milliseconds() as u64
}
