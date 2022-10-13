//! Heartbeat storage

use crate::Result;
use chrono::{DateTime, Utc};
use file_store::heartbeat::CellHeartbeat;
use file_store::{file_sink, FileStore};
use helium_proto::services::poc_mobile as proto;
use std::ops::Range;

use crate::cell_type::CellType;

pub struct Shares {
    pub invalid_shares: Vec<proto::Share>,
    pub valid_shares: Vec<proto::Share>,
}

#[derive(Debug, thiserror::Error)]
pub enum ValidateHeartbeatsError {
    #[error("file store error: {0}")]
    FileStoreError(#[from] file_store::Error),
    #[error("database error: {0}")]
    DbError(#[from] sqlx::Error),
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
                if share.validity == proto::ShareValidity::Valid as i32 {
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
    ) -> Result<()> {
        // Validate the heartbeats in the current epoch
        let Shares {
            invalid_shares,
            valid_shares,
        } = self;

        // Write out shares:
        file_sink::write(
            valid_shares_tx,
            proto::Shares {
                shares: valid_shares,
            },
        )
        .await?;

        file_sink::write(
            invalid_shares_tx,
            proto::Shares {
                shares: invalid_shares,
            },
        )
        .await?;

        Ok(())
    }
}

fn to_share(heartbeat: &CellHeartbeat, epoch: &Range<DateTime<Utc>>) -> proto::Share {
    match validate_heartbeat(&heartbeat, epoch) {
        Ok(cell_type) => proto::Share {
            timestamp: heartbeat.timestamp.timestamp() as u64,
            pub_key: heartbeat.pubkey.to_vec(),
            weight: crate::bones_to_u64(cell_type.reward_weight()),
            cell_type: cell_type as i32,
            cbsd_id: heartbeat.cbsd_id,
            validity: proto::ShareValidity::Valid as i32,
        },
        Err(validity) => proto::Share {
            cbsd_id: heartbeat.cbsd_id,
            timestamp: heartbeat.timestamp.timestamp() as u64,
            pub_key: heartbeat.pubkey.to_vec(),
            weight: 0,
            cell_type: 0,
            validity: validity as i32,
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
