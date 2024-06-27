use anyhow::Result;
use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};
use helium_crypto::{KeyTag, Keypair};
use rand::rngs::OsRng;
use sqlx::postgres::PgPoolOptions;
use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

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

pub fn hours_ago(hours: i64) -> u64 {
    chrono::Duration::hours(hours).num_milliseconds() as u64
}

pub fn generate_keypair() -> Keypair {
    Keypair::generate(KeyTag::default(), &mut OsRng)
}

pub async fn load_pcs_keypair() -> Result<Arc<Keypair>> {
    let data = std::fs::read("tests/pc_keypair.bin").map_err(helium_crypto::Error::from)?;
    let pcs_keypair = Arc::new(helium_crypto::Keypair::try_from(&data[..])?);
    Ok(pcs_keypair)
}

pub async fn get_rewards(address: String) -> Result<Option<u64>> {
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect("postgres://postgres:postgres@localhost/mobile_index")
        .await?;

    let row: Option<(i64,)> = sqlx::query_as("SELECT rewards FROM reward_index WHERE address = $1")
        .bind(address)
        .fetch_optional(&pool)
        .await?;

    pool.close().await;

    Ok(row.map(|r| r.0 as u64))
}
