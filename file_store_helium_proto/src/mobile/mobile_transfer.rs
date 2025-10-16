use chrono::{DateTime, Utc};
use file_store_shared::{traits::MsgDecode, Error, Result};
use helium_crypto::PublicKeyBinary;
use helium_proto::services::packet_verifier as proto;
use serde::Serialize;

use crate::traits::TimestampDecode;

#[derive(Serialize, Clone)]
pub struct ValidDataTransferSession {
    pub pub_key: PublicKeyBinary,
    pub payer: PublicKeyBinary,
    pub upload_bytes: u64,
    pub download_bytes: u64,
    pub rewardable_bytes: u64,
    pub num_dcs: u64,
    pub first_timestamp: DateTime<Utc>,
    pub last_timestamp: DateTime<Utc>,
    pub burn_timestamp: DateTime<Utc>,
}

impl MsgDecode for ValidDataTransferSession {
    type Msg = proto::ValidDataTransferSession;
}

impl TryFrom<proto::ValidDataTransferSession> for ValidDataTransferSession {
    type Error = Error;
    fn try_from(v: proto::ValidDataTransferSession) -> Result<Self> {
        Ok(Self {
            payer: v.payer.into(),
            pub_key: v.pub_key.into(),
            upload_bytes: v.upload_bytes,
            download_bytes: v.download_bytes,
            rewardable_bytes: v.rewardable_bytes,
            num_dcs: v.num_dcs,
            first_timestamp: v.first_timestamp.to_timestamp_millis()?,
            last_timestamp: v.last_timestamp.to_timestamp_millis()?,
            burn_timestamp: v.burn_timestamp.to_timestamp_millis()?,
        })
    }
}
