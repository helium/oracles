//! Heartbeat storage

use crate::{cell_type::CellType, Result};
use chrono::{DateTime, NaiveDateTime, Utc};
use file_store::{file_sink, heartbeat::CellHeartbeat, FileStore};
use helium_crypto::PublicKey;
use helium_proto::services::poc_mobile as proto;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::ops::Range;

pub struct Shares {
    pub invalid_shares: Vec<Share>,
    pub valid_shares: Vec<Share>,
}

#[derive(Clone)]
pub struct Share {
    pub cbsd_id: String,
    pub pub_key: PublicKey,
    pub reward_weight: Decimal,
    pub timestamp: NaiveDateTime,
    pub cell_type: Option<CellType>,
    pub validity: proto::ShareValidity,
}

#[derive(Debug, thiserror::Error)]
pub enum ValidateHeartbeatsError {
    #[error("file store error: {0}")]
    FileStoreError(#[from] file_store::Error),
    #[error("database error: {0}")]
    DbError(#[from] sqlx::Error),
}

impl From<Share> for proto::Share {
    fn from(share: Share) -> Self {
        Self {
            timestamp: share.timestamp.timestamp() as u64,
            pub_key: share.pub_key.to_vec(),
            weight: crate::bones_to_u64(share.reward_weight),
            cell_type: share.cell_type.unwrap_or(CellType::Nova436H) as i32,
            cbsd_id: share.cbsd_id,
            validity: share.validity as i32,
        }
    }
}

impl Shares {
    fn new() -> Self {
        Self {
            valid_shares: Vec::new(),
            invalid_shares: Vec::new(),
        }
    }

    pub async fn validate_heartbeats(
        file_store: &FileStore,
        epoch: &Range<DateTime<Utc>>,
    ) -> Result<Self> {
        let shares = crate::ingest::new_heartbeat_reports(file_store, epoch)
            .await?
            .iter()
            .map(|ingest_heartbeat| to_share(ingest_heartbeat, epoch))
            .fold(Shares::new(), |mut acc, share| {
                if share.validity == proto::ShareValidity::Valid {
                    acc.valid_shares.push(share);
                } else {
                    acc.invalid_shares.push(share);
                }
                acc
            });

        Ok(shares)
    }

    pub async fn write(
        self,
        valid_shares_tx: &file_sink::MessageSender,
        invalid_shares_tx: &file_sink::MessageSender,
    ) -> Result {
        let Shares {
            invalid_shares,
            valid_shares,
        } = self;

        // Write out shares:
        file_sink::write(
            valid_shares_tx,
            proto::Shares {
                shares: valid_shares.into_iter().map(proto::Share::from).collect(),
            },
        )
        .await?;

        file_sink::write(
            invalid_shares_tx,
            proto::Shares {
                shares: invalid_shares.into_iter().map(proto::Share::from).collect(),
            },
        )
        .await?;

        Ok(())
    }
}

fn to_share(heartbeat: &CellHeartbeat, epoch: &Range<DateTime<Utc>>) -> Share {
    match validate_heartbeat(heartbeat, epoch) {
        Ok(cell_type) => Share {
            timestamp: heartbeat.timestamp.naive_utc(),
            pub_key: heartbeat.pubkey.clone(),
            reward_weight: cell_type.reward_weight(),
            cell_type: Some(cell_type),
            cbsd_id: heartbeat.cbsd_id.clone(),
            validity: proto::ShareValidity::Valid,
        },
        Err(validity) => Share {
            timestamp: heartbeat.timestamp.naive_utc(),
            cbsd_id: heartbeat.cbsd_id.clone(),
            pub_key: heartbeat.pubkey.clone(),
            reward_weight: dec!(0),
            cell_type: None,
            validity,
        },
    }
}

/// Validate a heartbeat in the given epoch.
fn validate_heartbeat(
    heartbeat: &CellHeartbeat,
    epoch: &Range<DateTime<Utc>>,
) -> std::result::Result<CellType, proto::ShareValidity> {
    let cell_type = match CellType::from_cbsd_id(&heartbeat.cbsd_id) {
        Some(ty) => ty,
        _ => return Err(proto::ShareValidity::BadCbsdId),
    };

    if !heartbeat.operation_mode {
        // TODO: Add invalid reason for false operation mode
        return Err(proto::ShareValidity::HeartbeatOutsideRange);
    }

    if !epoch.contains(&heartbeat.timestamp) {
        return Err(proto::ShareValidity::HeartbeatOutsideRange);
    }

    Ok(cell_type)
}
