use crate::{Imsi, Uuid};
use chrono::{DateTime, Utc};
use helium_crypto::PublicKey;
use serde::Deserialize;

#[derive(sqlx::FromRow, Deserialize)]
pub struct Attach {
    #[serde(skip_deserializing)]
    pub uuid: Uuid,
    pub imsi: Imsi,
    pub pubkey: PublicKey,
    pub timetamp: DateTime<Utc>,

    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}
