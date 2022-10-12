//! Heartbeat storage

use chrono::{DateTime, NaiveDateTime, Utc};
use file_store::{
    heartbeat::{CellHeartbeat, CellHeartbeatIngestReport},
    traits::MsgDecode,
};
use file_store::{FileStore, FileType};
use futures::stream::{self, StreamExt, TryStreamExt};
use helium_crypto::PublicKey;
use helium_proto::services::poc_mobile::{Share, ShareValidity};
use rust_decimal::Decimal;
use sqlx::{Pool, Postgres, Transaction};
use std::collections::HashMap;
use std::ops::Range;

use crate::cell_type::CellType;

#[derive(sqlx::FromRow)]
pub struct Heartbeat {
    pub id: PublicKey,
    pub weight: Decimal,
    pub timestamp: NaiveDateTime,
}

#[derive(Clone)]
pub struct HeartbeatValue {
    pub weight: Decimal,
    pub timestamp: NaiveDateTime,
}

#[derive(Clone)]
pub struct Heartbeats {
    pub heartbeats: HashMap<PublicKey, HeartbeatValue>,
}

#[derive(Debug, thiserror::Error)]
pub enum ValidateHeartbeatsError {
    #[error("Database error: {0}")]
    DbError(#[from] sqlx::Error),
    #[error("File store error: {0}")]
    FileStoreError(#[from] file_store::Error),
}

impl Heartbeats {
    /// Constructs a new heartbeats collection, starting by polling every heartbeat
    /// since the end of the last rewardable period (`starting`).
    pub async fn new(pool: &Pool<Postgres>, starting: DateTime<Utc>) -> Result<Self, sqlx::Error> {
        let mut heartbeats = HashMap::new();
        let mut rows =
            sqlx::query_as::<_, Heartbeat>("SELECT * FROM heartbeats WHERE timestamp >= ?")
                .bind(starting)
                .fetch(pool);

        while let Some(Heartbeat {
            id,
            weight,
            timestamp,
        }) = rows.try_next().await?
        {
            heartbeats.insert(id, HeartbeatValue { weight, timestamp });
        }
        Ok(Self { heartbeats })
    }

    /// Clears the heartbeats in the collection and the database.
    pub async fn clear(&mut self, conn: &mut Transaction<'_, Postgres>) -> Result<(), sqlx::Error> {
        // TODO: should the truncation be bound to a given epoch?
        // It's not intended that any heartbeats will exists outside the
        // current epoch, but it might be better to code defensively.
        sqlx::query("TRUNCATE TABLE heartbeats;")
            .execute(conn)
            .await?;
        self.heartbeats.clear();
        Ok(())
    }

    /// Validates all of the heartbeats in a given epoch, updating all of the corresponding
    /// entries in the database.
    pub async fn validate_heartbeats(
        &mut self,
        conn: &mut Transaction<'_, Postgres>,
        epoch: &Range<DateTime<Utc>>,
        file_store: &FileStore,
    ) -> Result<Vec<Share>, ValidateHeartbeatsError> {
        let mut invalid_shares = Vec::new();

        let file_list = file_store
            .list_all(FileType::CellHeartbeatIngestReport, epoch.start, epoch.end)
            .await?;
        let mut stream = file_store.source(stream::iter(file_list.clone()).map(Ok).boxed());

        while let Some(Ok(msg)) = stream.next().await {
            let report = match CellHeartbeatIngestReport::decode(msg) {
                Ok(report) => report.report,
                Err(err) => {
                    tracing::error!("Could not decode cell heartbeat ingest report: {:?}", err);
                    continue;
                }
            };

            let validity = self.validate_heartbeat(epoch, &report);
            if validity != ShareValidity::Valid {
                invalid_shares.push(Share {
                    cbsd_id: report.cbsd_id,
                    timestamp: report.timestamp.timestamp() as u64,
                    pub_key: report.pubkey.to_vec(),
                    weight: 0,
                    cell_type: 0,
                    validity: validity as i32,
                });
            }
        }

        // Update the heartbeat values in the database
        self.clone().update(conn).await?;

        Ok(invalid_shares)
    }

    /// Validate a heartbeat. If the heartbeat is valid, it will be updated
    /// in the collection. Otherwise, an invalid reason will be returned.
    pub fn validate_heartbeat(
        &mut self,
        epoch: &Range<DateTime<Utc>>,
        heartbeat: &CellHeartbeat,
    ) -> ShareValidity {
        let cell_type = match CellType::from_cbsd_id(&heartbeat.cbsd_id) {
            Some(ty) => ty,
            _ => return ShareValidity::BadCbsdId,
        };

        if !heartbeat.operation_mode {
            // TODO: Add invalid reason for false operation mode
            return ShareValidity::HeartbeatOutsideRange;
        }

        if !epoch.contains(&heartbeat.timestamp) {
            return ShareValidity::HeartbeatOutsideRange;
        }

        self.heartbeats.insert(
            heartbeat.pubkey.clone(),
            HeartbeatValue {
                weight: cell_type.reward_weight(),
                timestamp: heartbeat.timestamp.naive_utc(),
            },
        );

        ShareValidity::Valid
    }

    /// Update all of the heartbeat values in the database.
    pub async fn update(self, exec: &mut Transaction<'_, Postgres>) -> Result<(), sqlx::Error> {
        for (pub_key, HeartbeatValue { weight, timestamp }) in self.heartbeats {
            sqlx::query(
                r#"
                    insert into heartbeats (id, weight, timestamp)
                    values ($1, $2, $3)
                    on conflict (id) do update set
                    weight = EXCLUDED.weight, timestamp = EXCLUDED.timestamp;
                    "#,
            )
            .bind(pub_key)
            .bind(weight)
            .bind(timestamp)
            .execute(&mut *exec)
            .await?;
        }

        Ok(())
    }
}
