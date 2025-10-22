use chrono::{DateTime, Utc};

#[derive(Debug, Clone)]
pub struct BoostedHexActivation {
    pub location: u64,
    pub activation_ts: DateTime<Utc>,
    pub boosted_hex_pubkey: String,
    pub boost_config_pubkey: String,
}
