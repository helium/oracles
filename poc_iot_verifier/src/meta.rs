use crate::{Error, Result};
use chrono::{DateTime, Utc};
use file_store::{traits::TimestampDecode, FileType};
use serde::{Deserialize, Serialize};

#[derive(sqlx::FromRow, Deserialize, Serialize, Debug)]
#[sqlx(type_name = "meta")]
pub struct Meta {
    pub key: String,
    pub value: String,
}

impl Meta {
    pub async fn insert_kv<'c, E>(executor: E, key: &str, val: &str) -> Result<Self>
    where
        E: sqlx::Executor<'c, Database = sqlx::Postgres>,
    {
        sqlx::query_as::<_, Self>(
            r#" insert into meta ( key, value )
            values ($1, $2)
            on conflict (key) do nothing
            returning *;
            "#,
        )
        .bind(key)
        .bind(val)
        .fetch_one(executor)
        .await
        .map_err(Error::from)
    }

    pub async fn get<'c, E>(executor: E, key: &str) -> Result<Option<Self>>
    where
        E: sqlx::Executor<'c, Database = sqlx::Postgres>,
    {
        sqlx::query_as::<_, Meta>(r#" select * from meta where key = $1;"#)
            .bind(key)
            .fetch_optional(executor)
            .await
            .map_err(Error::from)
    }

    pub async fn last_timestamp<'c, E>(
        executor: E,
        file_type: FileType,
    ) -> Result<Option<DateTime<Utc>>>
    where
        E: sqlx::Executor<'c, Database = sqlx::Postgres>,
    {
        let last_timestamp = sqlx::query_scalar::<_, String>(
            r#"
            select value from meta
            where key = $1
            "#,
        )
        .bind(file_type.to_str())
        .fetch_optional(executor)
        .await?
        .and_then(|v| {
            v.parse::<u64>().map_or_else(
                |_| None,
                |ts| ts.to_timestamp_millis().map_or_else(|_| None, Some),
            )
        });
        Ok(last_timestamp)
    }

    pub async fn update_last_timestamp<'c, E>(
        executor: E,
        file_type: FileType,
        timestamp: Option<DateTime<Utc>>,
    ) -> Result
    where
        E: sqlx::Executor<'c, Database = sqlx::Postgres>,
    {
        let _ = sqlx::query(
            r#"
            insert into meta (key, value)
            values ($1, $2)
            on conflict (key) do update set
                value = EXCLUDED.value
            "#,
        )
        .bind(file_type.to_str())
        .bind(timestamp.map(|v| v.timestamp_millis().to_string()))
        .execute(executor)
        .await?;
        Ok(())
    }
}
