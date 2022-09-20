use crate::Imsi;
use chrono::{DateTime, Utc};
use helium_crypto::PublicKey;
use serde::Deserialize;

#[derive(Deserialize)]
pub struct CellAttachEvent {
    pub imsi: Imsi,
    #[serde(alias = "publicAddress")]
    pub pubkey: PublicKey,
    #[serde(alias = "iso_timestamp")]
    pub timestamp: DateTime<Utc>,
}
