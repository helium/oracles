//! Heartbeat storage

use chrono::{DateTime, Utc};
use file_store::{
    heartbeat::{CellHeartbeat, CellHeartbeatIngestReport},
    traits::MsgDecode,
};
use file_store::{FileStore, FileType};
use futures::stream::{self, StreamExt};
use helium_proto::services::poc_mobile::{Share, ShareValidity};
use sqlx::{Postgres, Transaction};
use std::ops::Range;

use crate::cell_type::CellType;

pub struct Shares {
    pub invalid_shares: Vec<Share>,
    pub valid_shares: Vec<Share>,
}

#[derive(Debug, thiserror::Error)]
pub enum ValidateHeartbeatsError {
    #[error("file store error: {0}")]
    FileStoreError(#[from] file_store::Error),
    #[error("database error: {0}")]
    DbError(#[from] sqlx::Error),
}

impl Shares {
    pub async fn validate_heartbeats(
        exec: &mut Transaction<'_, Postgres>,
        file_store: &FileStore,
        epoch: &Range<DateTime<Utc>>,
    ) -> Result<Self, ValidateHeartbeatsError> {
        let mut invalid_shares = Vec::new();
        let mut valid_shares = Vec::new();

        let file_list = file_store
            .list_all(FileType::CellHeartbeatIngestReport, epoch.start, epoch.end)
            .await?;
        let mut stream = file_store.source(stream::iter(file_list.clone()).map(Ok).boxed());

        while let Some(Ok(msg)) = stream.next().await {
            let heartbeat = match CellHeartbeatIngestReport::decode(msg) {
                Ok(report) => report.report,
                Err(err) => {
                    tracing::error!("Could not decode cell heartbeat ingest report: {:?}", err);
                    continue;
                }
            };

            match validate_heartbeat(&heartbeat, epoch) {
                Ok(cell_type) => {
                    sqlx::query(
                        r#"
                    insert into heartbeats (id, weight, timestamp)
                    values ($1, $2, $3)
                    on conflict (id) do update set
                    weight = EXCLUDED.weight, timestamp = EXCLUDED.timestamp;
                    "#,
                    )
                    .bind(&heartbeat.pubkey)
                    .bind(cell_type.reward_weight())
                    .bind(heartbeat.timestamp.naive_utc())
                    .execute(&mut *exec)
                    .await?;
                    valid_shares.push(Share {
                        timestamp: heartbeat.timestamp.timestamp() as u64,
                        pub_key: heartbeat.pubkey.to_vec(),
                        weight: crate::bones_to_u64(cell_type.reward_weight()),
                        cell_type: cell_type as i32,
                        cbsd_id: heartbeat.cbsd_id,
                        validity: ShareValidity::Valid as i32,
                    });
                }
                Err(validity) => {
                    invalid_shares.push(Share {
                        cbsd_id: heartbeat.cbsd_id,
                        timestamp: heartbeat.timestamp.timestamp() as u64,
                        pub_key: heartbeat.pubkey.to_vec(),
                        weight: 0,
                        cell_type: 0,
                        validity: validity as i32,
                    });
                }
            }
        }

        Ok(Self {
            valid_shares,
            invalid_shares,
        })
    }
}

/// Validate a heartbeat in the given epoch.
fn validate_heartbeat(
    heartbeat: &CellHeartbeat,
    epoch: &Range<DateTime<Utc>>,
) -> Result<CellType, ShareValidity> {
    let cell_type = match CellType::from_cbsd_id(&heartbeat.cbsd_id) {
        Some(ty) => ty,
        _ => return Err(ShareValidity::BadCbsdId),
    };

    if !heartbeat.operation_mode {
        // TODO: Add invalid reason for false operation mode
        return Err(ShareValidity::HeartbeatOutsideRange);
    }

    if !epoch.contains(&heartbeat.timestamp) {
        return Err(ShareValidity::HeartbeatOutsideRange);
    }

    Ok(cell_type)
}
