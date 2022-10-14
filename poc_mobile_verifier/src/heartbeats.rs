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
    pub weight: Decimal,
    pub timestamp: NaiveDateTime,
}

#[derive(Clone)]
pub struct HeartbeatValue {
    pub weight: Decimal,
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
        exec: impl sqlx::Executor<'_, Database = sqlx::Postgres> + Copy,
        starting: DateTime<Utc>,
    ) -> std::result::Result<Self, sqlx::Error> {
        let mut heartbeats = HashMap::new();
        let mut rows =
            sqlx::query_as::<_, Heartbeat>("SELECT * FROM heartbeats WHERE timestamp >= $1")
                .bind(starting)
                .fetch(exec);

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
}

#[derive(sqlx::FromRow)]
struct HeartbeatSaveResult {
    inserted: bool,
}

impl Heartbeat {
    pub async fn save(self, exec: impl sqlx::PgExecutor<'_>) -> Result<bool> {
        sqlx::query_as::<_, HeartbeatSaveResult>(
            r#"
            insert into heartbeats (id, weight, timestamp)
            values ($1, $2, $3)
            on conflict (id) do update set
            weight = EXCLUDED.weight, timestamp = EXCLUDED.timestamp
            returning (xmax = 0) as inserted;
            "#,
        )
        .bind(self.id)
        .bind(self.weight)
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
            weight: share.weight,
            timestamp: share.timestamp,
        }
    }
}
