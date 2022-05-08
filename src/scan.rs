use crate::Uuid;
use chrono::{DateTime, Utc};

#[derive(sqlx::FromRow)]
pub struct Scan {
    pub uuid: Uuid,
    pub uplink: Option<Uuid>,
    pub cid: u32,
    pub freq: u32,
    pub lte: bool,
    pub mcc: u32,
    pub mnc: u32,
    pub rsrp: u32,
    pub rsrq: u32,

    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}
