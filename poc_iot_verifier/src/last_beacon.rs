use crate::{datetime_from_epoch, Error, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(sqlx::FromRow, Deserialize, Serialize, Debug)]
#[sqlx(type_name = "last_beacon")]
pub struct LastBeacon {
    pub id: Vec<u8>,
    pub timestamp: DateTime<Utc>,
}

impl LastBeacon {
    pub async fn insert_kv<'c, E>(executor: E, id: &Vec<u8>, val: &str) -> Result<Self>
    where
        E: sqlx::Executor<'c, Database = sqlx::Postgres>,
    {
        sqlx::query_as::<_, Self>(
            r#" insert into last_beacon ( id, timestamp )
            values ($1, $2)
            on conflict (key) do nothing
            returning *;
            "#,
        )
        .bind(id)
        .bind(val)
        .fetch_one(executor)
        .await
        .map_err(Error::from)
    }

    pub async fn get<'c, E>(executor: E, id: &Vec<u8>) -> Result<Option<Self>>
    where
        E: sqlx::Executor<'c, Database = sqlx::Postgres>,
    {
        sqlx::query_as::<_, LastBeacon>(r#" select * from last_beacon where id = $1;"#)
            .bind(id)
            .fetch_optional(executor)
            .await
            .map_err(Error::from)
    }

    pub async fn last_timestamp<'c, E>(executor: E, id: &Vec<u8>) -> Result<Option<DateTime<Utc>>>
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
                .map_or_else(|_| None, |secs| Some(datetime_from_epoch(secs + 10)))
            //TODO: remove the hardcoded + 10 above
            //      fix resolution of datetime function, dropping millisecs resulting in files being reprocessed by loader
        });
        Ok(height)
    }

    pub async fn update_last_timestamp<'c, E>(
        executor: E,
        id: &Vec<u8>,
        timestamp: DateTime<Utc>,
    ) -> Result
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
}
