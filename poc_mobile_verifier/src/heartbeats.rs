//! Heartbeat storage

use crate::{shares::Share, Error, Result};
use chrono::{DateTime, NaiveDateTime, Utc};
use helium_crypto::PublicKey;
use rust_decimal::Decimal;
use std::collections::HashMap;

#[derive(sqlx::FromRow, Clone)]
pub struct Heartbeat {
    pub hotspot_key: PublicKey,
    pub cbsd_id: String,
    pub reward_weight: Decimal,
    pub timestamp: NaiveDateTime,
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
        let heartbeats =
            sqlx::query_as::<_, Heartbeat>("SELECT * FROM heartbeats WHERE timestamp >= $1")
                .bind(starting)
                .fetch_all(exec)
                .await?
                .into_iter()
                .map(
                    |Heartbeat {
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
            },
        )
    }
}

impl FromIterator<Share> for Heartbeats {
    fn from_iter<T>(iter: T) -> Self
    where
        T: IntoIterator<Item = Share>,
    {
        let heartbeats = iter
            .into_iter()
            .map(|share| {
                (
                    HeartbeatKey {
                        hotspot_key: share.pub_key,
                        cbsd_id: share.cbsd_id,
                    },
                    HeartbeatValue {
                        reward_weight: share.reward_weight,
                        timestamp: share.timestamp,
                    },
                )
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
    pub async fn save(self, exec: impl sqlx::PgExecutor<'_>) -> Result<bool> {
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

impl From<Share> for Heartbeat {
    fn from(share: Share) -> Self {
        Self {
            hotspot_key: share.pub_key,
            cbsd_id: share.cbsd_id,
            reward_weight: share.reward_weight,
            timestamp: share.timestamp,
        }
    }
}
