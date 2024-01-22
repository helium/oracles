use chrono::{DateTime, Utc};
use sqlx::FromRow;

#[derive(Debug, Clone, FromRow)]
pub struct BoostedHexActivation {
    #[sqlx(try_from = "i64")]
    pub location: u64,
    pub activation_ts: DateTime<Utc>,
    pub boosted_hex_pubkey: String,
    pub boost_config_pubkey: String,
}
