use chrono::{DateTime, Utc};
use file_store::traits::TimestampDecode;
use futures::stream::TryStreamExt;
use helium_crypto::PublicKeyBinary;
use std::collections::HashMap;

#[derive(sqlx::FromRow, Debug)]
#[sqlx(type_name = "last_beacon")]
pub struct LastBeacon {
    pub id: Vec<u8>,
    pub timestamp: DateTime<Utc>,
}

#[derive(thiserror::Error, Debug)]
pub enum LastBeaconError {
    #[error("database error: {0}")]
    DatabaseError(#[from] sqlx::Error),
    #[error("file store error: {0}")]
    FileStoreError(#[from] file_store::Error),
}

#[derive(Default)]
pub struct LastBeacons {
    pub beacon_map: HashMap<PublicKeyBinary, DateTime<Utc>>,
}

impl LastBeacon {
    pub async fn insert_kv<'c, E>(
        executor: E,
        id: &[u8],
        val: &str,
    ) -> Result<Self, LastBeaconError>
    where
        E: sqlx::Executor<'c, Database = sqlx::Postgres>,
    {
        Ok(sqlx::query_as::<_, Self>(
            r#" insert into last_beacon ( id, timestamp )
            values ($1, $2)
            on conflict (key) do nothing
            returning *;
            "#,
        )
        .bind(id)
        .bind(val)
        .fetch_one(executor)
        .await?)
    }

    pub async fn get<'c, E>(executor: E, id: &[u8]) -> Result<Option<Self>, LastBeaconError>
    where
        E: sqlx::Executor<'c, Database = sqlx::Postgres>,
    {
        Ok(
            sqlx::query_as::<_, LastBeacon>(r#" select * from last_beacon where id = $1;"#)
                .bind(id)
                .fetch_optional(executor)
                .await?,
        )
    }

    pub async fn get_all_since<'c, E>(
        deadline: DateTime<Utc>,
        executor: E,
    ) -> Result<Vec<Self>, sqlx::Error>
    where
        E: sqlx::Executor<'c, Database = sqlx::Postgres> + 'c,
    {
        sqlx::query_as::<_, Self>(r#" select * from last_beacon where timestamp >= $1; "#)
            .bind(deadline)
            .fetch_all(executor)
            .await
    }

    pub async fn last_timestamp<'c, E>(
        executor: E,
        id: &[u8],
    ) -> Result<Option<DateTime<Utc>>, LastBeaconError>
    where
        E: sqlx::Executor<'c, Database = sqlx::Postgres>,
    {
        let height = sqlx::query_scalar::<_, String>(
            r#"
            select timestamp from last_beacon
            where id = $1
            "#,
        )
        .bind(id)
        .fetch_optional(executor)
        .await?
        .and_then(|v| {
            v.parse::<u64>()
                .map_or_else(|_| None, |secs| Some(secs.to_timestamp()))
        })
        .transpose()?;
        Ok(height)
    }

    pub async fn update_last_timestamp<'c, E>(
        executor: E,
        id: &[u8],
        timestamp: DateTime<Utc>,
    ) -> Result<(), LastBeaconError>
    where
        E: sqlx::Executor<'c, Database = sqlx::Postgres>,
    {
        let _ = sqlx::query(
            r#"
            insert into last_beacon (id, timestamp)
            values ($1, $2)
            on conflict (id) do update set
                timestamp = EXCLUDED.timestamp
            "#,
        )
        .bind(id)
        .bind(timestamp)
        .execute(executor)
        .await?;
        Ok(())
    }

    pub async fn get_all<'c, E>(executor: E) -> Result<LastBeacons, LastBeaconError>
    where
        E: sqlx::Executor<'c, Database = sqlx::Postgres>,
    {
        let mut beacon_map: HashMap<PublicKeyBinary, DateTime<Utc>> = HashMap::new();
        let mut rows = sqlx::query_as::<_, Self>(
            r#"
            select * from last_beacon
            "#,
        )
        .fetch(executor);
        while let Some(last_beacon) = rows.try_next().await? {
            beacon_map.insert(PublicKeyBinary::from(last_beacon.id), last_beacon.timestamp);
        }
        Ok(LastBeacons { beacon_map })
    }
}
