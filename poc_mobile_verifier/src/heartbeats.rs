//! Heartbeat storage

use crate::{cell_type::CellType, Error, Result};
use chrono::{DateTime, NaiveDateTime, Utc};
use file_store::{
    file_sink,
    heartbeat::{CellHeartbeat, CellHeartbeatIngestReport},
    traits::MsgDecode,
    FileStore, FileType,
};
use futures::stream::{self, StreamExt};
use helium_crypto::PublicKey;
use helium_proto::services::poc_mobile as proto;
use rust_decimal::{Decimal, prelude::ToPrimitive};
use rust_decimal_macros::dec;
use std::{collections::HashMap, ops::Range};

#[derive(Clone)]
pub struct Heartbeat {
    pub hotspot_key: PublicKey,
    pub cbsd_id: String,
    pub reward_weight: Decimal,
    pub timestamp: NaiveDateTime,
    pub validity: proto::HeartbeatValidity,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct HeartbeatKey {
    hotspot_key: PublicKey,
    cbsd_id: String,
}

pub struct HeartbeatValue {
    reward_weight: Decimal,
    timestamp: NaiveDateTime,
}

pub struct Heartbeats {
    pub heartbeats: HashMap<HeartbeatKey, HeartbeatValue>,
}

impl Heartbeats {
    pub async fn validated(
        exec: impl sqlx::PgExecutor<'_>,
        starting: DateTime<Utc>,
    ) -> std::result::Result<Self, sqlx::Error> {
        #[derive(sqlx::FromRow)]
        pub struct HeartbeatRow {
            pub hotspot_key: PublicKey,
            pub cbsd_id: String,
            pub reward_weight: Decimal,
            pub timestamp: NaiveDateTime,
        }

        let heartbeats =
            sqlx::query_as::<_, HeartbeatRow>("SELECT * FROM heartbeats WHERE timestamp >= $1")
                .bind(starting)
                .fetch_all(exec)
                .await?
                .into_iter()
                .map(
                    |HeartbeatRow {
                         hotspot_key,
                         cbsd_id,
                         reward_weight,
                         timestamp,
                     }| {
                        (
                            HeartbeatKey {
                                hotspot_key,
                                cbsd_id,
                            },
                            HeartbeatValue {
                                reward_weight,
                                timestamp,
                            },
                        )
                    },
                )
                .collect();
        Ok(Self { heartbeats })
    }

    pub fn into_iter(self) -> impl IntoIterator<Item = Heartbeat> {
        self.heartbeats.into_iter().map(
            |(
                HeartbeatKey {
                    hotspot_key,
                    cbsd_id,
                },
                HeartbeatValue {
                    reward_weight,
                    timestamp,
                },
            )| Heartbeat {
                hotspot_key,
                cbsd_id,
                reward_weight,
                timestamp,
                validity: proto::HeartbeatValidity::Valid,
            },
        )
    }
}

impl FromIterator<Heartbeat> for Heartbeats {
    fn from_iter<T>(iter: T) -> Self
    where
        T: IntoIterator<Item = Heartbeat>,
    {
        let heartbeats = iter
            .into_iter()
            .flat_map(|hb| {
                if hb.validity != proto::HeartbeatValidity::Valid {
                    return None;
                }
                Some((
                    HeartbeatKey {
                        hotspot_key: hb.hotspot_key,
                        cbsd_id: hb.cbsd_id,
                    },
                    HeartbeatValue {
                        reward_weight: hb.reward_weight,
                        timestamp: hb.timestamp,
                    },
                ))
            })
            .collect();
        Self { heartbeats }
    }
}

#[derive(sqlx::FromRow)]
struct HeartbeatSaveResult {
    inserted: bool,
}

impl Heartbeat {
    // TODO: Convert this to a Stream of heartbeats
    pub async fn validate_heartbeats(
        file_store: &FileStore,
        epoch: &Range<DateTime<Utc>>,
    ) -> Result<Vec<Self>> {
        let mut heartbeats = Vec::new();
        let file_list = file_store
            .list_all(FileType::CellHeartbeatIngestReport, epoch.start, epoch.end)
            .await?;
        let mut stream = file_store.source(stream::iter(file_list).map(Ok).boxed());

        while let Some(Ok(msg)) = stream.next().await {
            let heartbeat_report = match CellHeartbeatIngestReport::decode(msg) {
                Ok(report) => report.report,
                Err(err) => {
                    tracing::error!("Could not decode cell heartbeat ingest report: {:?}", err);
                    continue;
                }
            };
            let (reward_weight, validity) = match validate_heartbeat(&heartbeat_report, epoch) {
                Ok(cell_type) => {
                    let reward_weight = cell_type.reward_weight();
                    (reward_weight, proto::HeartbeatValidity::Valid)
                }
                Err(validity) => (dec!(0), validity),
            };
            heartbeats.push(Heartbeat {
                hotspot_key: heartbeat_report.pubkey.clone(),
                reward_weight,
                cbsd_id: heartbeat_report.cbsd_id.clone(),
                timestamp: heartbeat_report.timestamp.naive_utc(),
                validity,
            });
        }

        Ok(heartbeats)
    }

    pub async fn write(&self, heartbeats_tx: &file_sink::MessageSender) -> file_store::Result {
        let cell_type = CellType::from_cbsd_id(&self.cbsd_id).unwrap_or(CellType::Nova436H) as i32;
        file_sink::write(
            heartbeats_tx,
            proto::Heartbeat {
                cbsd_id: self.cbsd_id.clone(),
                pub_key: self.hotspot_key.to_vec(),
                reward_multiplier: self.reward_weight.to_f32().unwrap_or(0.0),
                cell_type,
                validity: self.validity as i32,
                timestamp: self.timestamp.timestamp() as u64,
            },
        )
        .await?;
        Ok(())
    }

    pub async fn save(self, exec: impl sqlx::PgExecutor<'_>) -> Result<bool> {
        // If the heartbeat is not valid, do not save it
        if self.validity != proto::HeartbeatValidity::Valid {
            return Ok(false);
        }

        sqlx::query_as::<_, HeartbeatSaveResult>(
            r#"
            insert into heartbeats (hotspot_key, cbsd_id, reward_weight, timestamp)
            values ($1, $2, $3, $4)
            on conflict (hotspot_key, cbsd_id) do update set
            reward_weight = EXCLUDED.reward_weight, timestamp = EXCLUDED.timestamp
            returning (xmax = 0) as inserted;
            "#,
        )
        .bind(self.hotspot_key)
        .bind(self.cbsd_id)
        .bind(self.reward_weight)
        .bind(self.timestamp)
        .fetch_one(exec)
        .await
        .map(|result| result.inserted)
        .map_err(Error::from)
    }
}

/// Validate a heartbeat in the given epoch.
fn validate_heartbeat(
    heartbeat: &CellHeartbeat,
    epoch: &Range<DateTime<Utc>>,
) -> std::result::Result<CellType, proto::HeartbeatValidity> {
    let cell_type = match CellType::from_cbsd_id(&heartbeat.cbsd_id) {
        Some(ty) => ty,
        _ => return Err(proto::HeartbeatValidity::BadCbsdId),
    };

    if !heartbeat.operation_mode {
        return Err(proto::HeartbeatValidity::NotOperational);
    }

    if !epoch.contains(&heartbeat.timestamp) {
        return Err(proto::HeartbeatValidity::HeartbeatOutsideRange);
    }

    Ok(cell_type)
}
