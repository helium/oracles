use chrono::{DateTime, Utc};
use file_store::traits::{MsgDecode, TimestampDecode, TimestampDecodeError, TimestampEncode};
use helium_crypto::PublicKeyBinary;
use helium_proto::services::packet_verifier as proto;

use crate::mobile::data_transfer_multiplier::DataTransferMultiplier;
use serde::Serialize;

#[derive(thiserror::Error, Debug)]
pub enum ValidDataTransferError {
    #[error("invalid timestamp: {0}")]
    Timestamp(#[from] TimestampDecodeError),
}

#[derive(Serialize, Clone)]
pub struct ValidDataTransferSession {
    pub pub_key: PublicKeyBinary,
    pub payer: PublicKeyBinary,
    pub upload_bytes: u64,
    pub download_bytes: u64,
    pub rewardable_bytes: u64,
    pub num_dcs: u64,
    /// HIP-150: the multiplier `num_dcs` was derived with.
    ///
    /// `num_dcs` is the post-multiplier figure — what the payer actually burned,
    /// and what the reward path reads — so this is what makes the
    /// pre-multiplier count recoverable.
    pub multiplier: DataTransferMultiplier,
    pub first_timestamp: DateTime<Utc>,
    pub last_timestamp: DateTime<Utc>,
    pub burn_timestamp: DateTime<Utc>,
}

impl MsgDecode for ValidDataTransferSession {
    type Msg = proto::ValidDataTransferSession;
}

impl TryFrom<proto::ValidDataTransferSession> for ValidDataTransferSession {
    type Error = ValidDataTransferError;

    fn try_from(v: proto::ValidDataTransferSession) -> Result<Self, Self::Error> {
        Ok(Self {
            payer: v.payer.into(),
            pub_key: v.pub_key.into(),
            upload_bytes: v.upload_bytes,
            download_bytes: v.download_bytes,
            rewardable_bytes: v.rewardable_bytes,
            num_dcs: v.num_dcs,
            // Absent means the record predates HIP-150; every record written
            // since carries an explicit multiplier, `1` included.
            multiplier: v
                .multiplier
                .and_then(|m| m.value.parse().ok())
                .and_then(|d| DataTransferMultiplier::new(d).ok())
                .unwrap_or(DataTransferMultiplier::DEFAULT),
            first_timestamp: v.first_timestamp.to_timestamp_millis()?,
            last_timestamp: v.last_timestamp.to_timestamp_millis()?,
            burn_timestamp: v.burn_timestamp.to_timestamp_millis()?,
        })
    }
}

impl From<ValidDataTransferSession> for proto::ValidDataTransferSession {
    fn from(v: ValidDataTransferSession) -> Self {
        Self {
            pub_key: v.pub_key.into(),
            upload_bytes: v.upload_bytes,
            download_bytes: v.download_bytes,
            num_dcs: v.num_dcs,
            payer: v.payer.into(),
            first_timestamp: v.first_timestamp.encode_timestamp_millis(),
            last_timestamp: v.last_timestamp.encode_timestamp_millis(),
            rewardable_bytes: v.rewardable_bytes,
            burn_timestamp: v.burn_timestamp.encode_timestamp_millis(),
            // Always written, `1` included, so absent means only "predates
            // HIP-150" rather than being ambiguous with an unmultiplied session.
            multiplier: Some(v.multiplier.into()),
        }
    }
}
