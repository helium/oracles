//! Heartbeat storage

use crate::{shares::Share, Error, Result};
use chrono::{DateTime, NaiveDateTime, Utc};
use futures::stream::TryStreamExt;
use helium_crypto::PublicKey;
use rust_decimal::Decimal;
use std::collections::HashMap;

#[derive(sqlx::FromRow)]
pub struct Heartbeat {
    pub id: PublicKey,
    pub reward_weight: Decimal,
    pub timestamp: NaiveDateTime,
}

#[derive(sqlx::FromRow)]
struct HeartbeatSaveResult {
    inserted: bool,
}

impl Heartbeat {
    pub async fn save(self, exec: impl sqlx::PgExecutor<'_>) -> Result<bool> {
        sqlx::query_as::<_, HeartbeatSaveResult>(
            r#"
            insert into heartbeats (id, reward_weight, timestamp)
            values ($1, $2, $3)
            on conflict (id) do update set
            reward_weight = EXCLUDED.reward_weight, timestamp = EXCLUDED.timestamp
            returning (xmax = 0) as inserted;
            "#,
        )
        .bind(self.id)
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
            id: share.pub_key,
            reward_weight: share.reward_weight,
            timestamp: share.timestamp,
        }
    }
}

#[derive(Clone)]
pub struct HeartbeatValue {
    pub reward_weight: Decimal,
    pub timestamp: NaiveDateTime,
}

#[derive(Default, Clone)]
pub struct Heartbeats {
    pub heartbeats: HashMap<PublicKey, HeartbeatValue>,
}

impl Heartbeats {
    /// Constructs a new heartbeats collection, starting by pulling every heartbeat
    /// since the end of the last rewardable period (`starting`).
    pub async fn validated(
        exec: impl sqlx::PgExecutor<'_> + Copy,
        starting: DateTime<Utc>,
    ) -> std::result::Result<Self, sqlx::Error> {
        let mut heartbeats = HashMap::new();
        let mut rows =
            sqlx::query_as::<_, Heartbeat>("SELECT * FROM heartbeats WHERE timestamp >= $1")
                .bind(starting.naive_utc())
                .fetch(exec);

        while let Some(Heartbeat {
            id,
            reward_weight,
            timestamp,
        }) = rows.try_next().await?
        {
            heartbeats.insert(
                id,
                HeartbeatValue {
                    reward_weight,
                    timestamp,
                },
            );
        }
        Ok(Self { heartbeats })
    }
}

impl FromIterator<Share> for Heartbeats {
    fn from_iter<T>(iter: T) -> Self
    where
        T: IntoIterator<Item = Share>,
    {
        let mut heartbeats = HashMap::default();
        for share in iter.into_iter() {
            let Heartbeat {
                id,
                reward_weight,
                timestamp,
            } = Heartbeat::from(share);
            heartbeats.insert(
                id,
                HeartbeatValue {
                    reward_weight,
                    timestamp,
                },
            );
        }
        Self { heartbeats }
    }
}
