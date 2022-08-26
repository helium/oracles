use crate::{datetime_from_epoch, Error, PublicKey, Result};
use chrono::{DateTime, Utc};
use helium_proto::services::poc_mobile::SpeedtestReqV1;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
pub struct CellSpeedtest {
    #[serde(alias = "pubKey")]
    pub pubkey: PublicKey,
    pub serial: String,
    pub timestamp: DateTime<Utc>,
    #[serde(alias = "uploadSpeed")]
    pub upload_speed: u64,
    #[serde(alias = "downloadSpeed")]
    pub download_speed: u64,
    pub latency: u32,
}

impl TryFrom<SpeedtestReqV1> for CellSpeedtest {
    type Error = Error;
    fn try_from(v: SpeedtestReqV1) -> Result<Self> {
        Ok(Self {
            pubkey: PublicKey::try_from(v.pub_key)?,
            serial: v.serial,
            timestamp: datetime_from_epoch(v.timestamp),
            upload_speed: v.upload_speed,
            download_speed: v.download_speed,
            latency: v.latency,
        })
    }
}

impl TryFrom<CellSpeedtest> for SpeedtestReqV1 {
    type Error = Error;
    fn try_from(v: CellSpeedtest) -> Result<Self> {
        Ok(SpeedtestReqV1 {
            pub_key: v.pubkey.to_vec(),
            serial: v.serial,
            timestamp: v.timestamp.timestamp() as u64,
            upload_speed: v.upload_speed,
            download_speed: v.download_speed,
            latency: v.latency,
            signature: vec![],
        })
    }
}
