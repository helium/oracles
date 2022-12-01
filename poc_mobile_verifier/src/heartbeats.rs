//! Heartbeat storage

use crate::cell_type::CellType;
use chrono::{DateTime, NaiveDateTime, Timelike, Utc};
use file_store::{file_sink, file_sink_write, heartbeat::CellHeartbeat};
use futures::stream::{Stream, StreamExt};
use helium_crypto::PublicKey;
use helium_proto::services::poc_mobile as proto;
use rust_decimal::{prelude::ToPrimitive, Decimal};
use rust_decimal_macros::dec;
use sqlx::{Postgres, Transaction};
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

#[derive(Default)]
pub struct HeartbeatValue {
    pub reward_weight: Decimal,
    pub hours_seen: [bool; 24],
}

impl HeartbeatValue {
    pub fn heartbeat_count(&self) -> usize {
        self.hours_seen.iter().map(|x| *x as usize).sum()
    }
}

pub struct HeartbeatReward {
    pub hotspot_key: PublicKey,
    pub cbsd_id: String,
    pub reward_weight: Decimal,
}

#[derive(Default)]
pub struct Heartbeats {
    pub heartbeats: HashMap<HeartbeatKey, HeartbeatValue>,
}

/// Minimum number of heartbeats required to give a reward to the hotspot.
pub const MINIMUM_HEARTBEAT_COUNT: usize = 12;

impl Heartbeats {
    pub async fn validated(exec: impl sqlx::PgExecutor<'_>) -> Result<Self, sqlx::Error> {
        #[derive(sqlx::FromRow)]
        pub struct HeartbeatRow {
            hotspot_key: PublicKey,
            cbsd_id: String,
            reward_weight: Decimal,
            hours_seen: [bool; 24],
        }

        let heartbeats = sqlx::query_as::<_, HeartbeatRow>("SELECT * FROM heartbeats")
            .fetch_all(exec)
            .await?
            .into_iter()
            .map(|hb| {
                (
                    HeartbeatKey {
                        hotspot_key: hb.hotspot_key,
                        cbsd_id: hb.cbsd_id,
                    },
                    HeartbeatValue {
                        reward_weight: hb.reward_weight,
                        hours_seen: hb.hours_seen,
                    },
                )
            })
            .collect();
        Ok(Self { heartbeats })
    }

    pub fn into_rewardables(self) -> impl Iterator<Item = HeartbeatReward> + Send {
        self.heartbeats
            .into_iter()
            .filter(|(_, value)| value.heartbeat_count() >= MINIMUM_HEARTBEAT_COUNT)
            .map(|(key, value)| HeartbeatReward {
                hotspot_key: key.hotspot_key,
                cbsd_id: key.cbsd_id,
                reward_weight: value.reward_weight,
            })
    }
}

impl Extend<Heartbeat> for Heartbeats {
    fn extend<T>(&mut self, iter: T)
    where
        T: IntoIterator<Item = Heartbeat>,
    {
        for heartbeat in iter.into_iter() {
            if heartbeat.validity != proto::HeartbeatValidity::Valid {
                continue;
            }
            let entry = self
                .heartbeats
                .entry(HeartbeatKey {
                    hotspot_key: heartbeat.hotspot_key,
                    cbsd_id: heartbeat.cbsd_id,
                })
                .or_default();
            entry.reward_weight = heartbeat.reward_weight;
            entry.hours_seen[heartbeat.timestamp.hour() as usize] = true;
        }
    }
}

impl FromIterator<Heartbeat> for Heartbeats {
    fn from_iter<T>(iter: T) -> Self
    where
        T: IntoIterator<Item = Heartbeat>,
    {
        let mut heartbeats = Self::default();
        heartbeats.extend(iter);
        heartbeats
    }
}

#[derive(sqlx::FromRow)]
struct HeartbeatSaveResult {
    inserted: bool,
}

#[derive(thiserror::Error, Debug)]
#[error(transparent)]
pub struct SaveHeartbeatError(#[from] sqlx::Error);

impl Heartbeat {
    pub async fn validate_heartbeats<'a>(
        heartbeats: impl Stream<Item = CellHeartbeat> + 'a,
        epoch: &'a Range<DateTime<Utc>>,
    ) -> impl Stream<Item = Self> + 'a {
        heartbeats.map(move |heartbeat_report| {
            let (reward_weight, validity) = match validate_heartbeat(&heartbeat_report, epoch) {
                Ok(cell_type) => {
                    let reward_weight = cell_type.reward_weight();
                    (reward_weight, proto::HeartbeatValidity::Valid)
                }
                Err(validity) => (dec!(0), validity),
            };
            Heartbeat {
                hotspot_key: heartbeat_report.pubkey.clone(),
                reward_weight,
                cbsd_id: heartbeat_report.cbsd_id.clone(),
                timestamp: heartbeat_report.timestamp.naive_utc(),
                validity,
            }
        })
    }

    pub async fn write(&self, heartbeats_tx: &file_sink::MessageSender) -> file_store::Result {
        let cell_type = CellType::from_cbsd_id(&self.cbsd_id).unwrap_or(CellType::Nova436H) as i32;
        file_sink_write!(
            "heartbeat",
            heartbeats_tx,
            proto::Heartbeat {
                cbsd_id: self.cbsd_id.clone(),
                pub_key: self.hotspot_key.to_vec(),
                reward_multiplier: self.reward_weight.to_f32().unwrap_or(0.0),
                cell_type,
                validity: self.validity as i32,
                timestamp: self.timestamp.timestamp() as u64,
            }
        )
        .await?;
        Ok(())
    }

    pub async fn save(
        self,
        exec: &mut Transaction<'_, Postgres>,
    ) -> Result<bool, SaveHeartbeatError> {
        // If the heartbeat is not valid, do not save it
        if self.validity != proto::HeartbeatValidity::Valid {
            return Ok(false);
        }

        Ok(sqlx::query_as::<_, HeartbeatSaveResult>(
            r#"
            INSERT INTO heartbeats (hotspot_key, cbsd_id, reward_weight, hours_seen)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT cbsd_id DO UPDATE SET
            hours_seen = CASE WHEN hotspot_key = EXCLUDED.hotspot_key THEN
                            hours_seen[1:$5] || TRUE || hours_seen[($5 + 1):]
                         ELSE
                            $4
                         END,
            hotspot_key = EXCLUDED.hotspot_key
            RETURNING (xmax = 0) as inserted;
            "#,
        )
        .bind(self.hotspot_key)
        .bind(self.cbsd_id)
        .bind(self.reward_weight)
        .bind(new_hours_seen(&self.timestamp))
        .bind(self.timestamp.hour() as i32)
        .fetch_one(&mut *exec)
        .await?
        .inserted)
    }
}

fn new_hours_seen(timestamp: &NaiveDateTime) -> [bool; 24] {
    let mut hours_seen = [false; 24];
    hours_seen[timestamp.hour() as usize] = true;
    hours_seen
}

/// Validate a heartbeat in the given epoch.
fn validate_heartbeat(
    heartbeat: &CellHeartbeat,
    epoch: &Range<DateTime<Utc>>,
) -> Result<CellType, proto::HeartbeatValidity> {
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
