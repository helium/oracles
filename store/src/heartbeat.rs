use crate::{datetime_from_epoch, Error, PublicKey, Result};
use chrono::{DateTime, Utc};
use helium_proto::services::poc_mobile::CellHeartbeatReqV1;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct CellHeartbeat {
    #[serde(alias = "pubKey")]
    pub pubkey: PublicKey,
    pub hotspot_type: String,
    pub cell_id: u32,
    pub timestamp: DateTime<Utc>,
    #[serde(alias = "longitude")]
    pub lon: f64,
    #[serde(alias = "latitude")]
    pub lat: f64,
    pub operation_mode: bool,
    pub cbsd_category: String,
    pub cbsd_id: String,
}

impl TryFrom<CellHeartbeatReqV1> for CellHeartbeat {
    type Error = Error;
    fn try_from(v: CellHeartbeatReqV1) -> Result<Self> {
        Ok(Self {
            pubkey: PublicKey::try_from(v.pub_key)?,
            hotspot_type: v.hotspot_type,
            cell_id: v.cell_id,
            timestamp: datetime_from_epoch(v.timestamp),
            lon: v.lon,
            lat: v.lat,
            operation_mode: v.operation_mode,
            cbsd_category: v.cbsd_category,
            cbsd_id: v.cbsd_id,
        })
    }
}

impl From<CellHeartbeat> for CellHeartbeatReqV1 {
    fn from(v: CellHeartbeat) -> Self {
        Self {
            pub_key: v.pubkey.to_vec(),
            hotspot_type: v.hotspot_type,
            cell_id: v.cell_id,
            timestamp: v.timestamp.timestamp() as u64,
            lon: v.lon,
            lat: v.lat,
            operation_mode: v.operation_mode,
            cbsd_category: v.cbsd_category,
            cbsd_id: v.cbsd_id,
            signature: vec![],
        }
    }
}
