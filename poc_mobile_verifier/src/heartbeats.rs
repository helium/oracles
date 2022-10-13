//! Heartbeat storage

use chrono::{DateTime, NaiveDateTime, Utc};
use futures::stream::TryStreamExt;
use helium_crypto::PublicKey;
use rust_decimal::Decimal;
use sqlx::{Postgres, Transaction};
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
    /// Constructs a new heartbeats collection, starting by polling every heartbeat
    /// since the end of the last rewardable period (`starting`).
    pub async fn new(
        transaction: &mut Transaction<'_, Postgres>,
        starting: DateTime<Utc>,
    ) -> Result<Self, sqlx::Error> {
        let mut heartbeats = HashMap::new();
        let mut rows =
            sqlx::query_as::<_, Heartbeat>("SELECT * FROM heartbeats WHERE timestamp >= ?")
                .bind(starting)
                .fetch(transaction);

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
    pub async fn clear(
        &mut self,
        transaction: &mut Transaction<'_, Postgres>,
    ) -> Result<(), sqlx::Error> {
        // TODO: should the truncation be bound to a given epoch?
        // It's not intended that any heartbeats will exists outside the
        // current epoch, but it might be better to code defensively.
        sqlx::query("TRUNCATE TABLE heartbeats;")
            .execute(transaction)
            .await?;
        self.heartbeats.clear();
        Ok(())
    }
}
