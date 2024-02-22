use chrono::{DateTime, Utc};
use file_store::traits::TimestampDecode;
use serde::{Deserialize, Serialize};

#[derive(sqlx::FromRow, Deserialize, Serialize, Debug)]
#[sqlx(type_name = "meta")]
pub struct Meta {
    pub key: String,
    pub value: String,
}

#[derive(thiserror::Error, Debug)]
#[error("meta error: {0}")]
pub struct MetaError(#[from] sqlx::Error);

impl Meta {
    pub async fn insert_kv<'c, E>(executor: E, key: &str, val: &str) -> Result<Self, MetaError>
    where
        E: sqlx::Executor<'c, Database = sqlx::Postgres>,
    {
        Ok(sqlx::query_as::<_, Self>(
            r#" insert into meta ( key, value )
            values ($1, $2)
            on conflict (key) do nothing
            returning *;
            "#,
        )
        .bind(key)
        .bind(val)
        .fetch_one(executor)
        .await?)
    }

    pub async fn get<'c, E>(executor: E, key: &str) -> Result<Option<Self>, MetaError>
    where
        E: sqlx::Executor<'c, Database = sqlx::Postgres>,
    {
        Ok(
            sqlx::query_as::<_, Meta>(r#" select * from meta where key = $1;"#)
                .bind(key)
                .fetch_optional(executor)
                .await?,
        )
    }

    pub async fn last_timestamp<'c, E>(
        executor: E,
        file_type: &str,
    ) -> Result<Option<DateTime<Utc>>, MetaError>
    where
        E: sqlx::Executor<'c, Database = sqlx::Postgres>,
    {
        let last_timestamp = sqlx::query_scalar::<_, String>(
            r#"
            select value from meta
            where key = $1
            "#,
        )
        .bind(file_type)
        .fetch_optional(executor)
        .await?
        .and_then(|v| {
            v.parse::<u64>()
                .map_or_else(|_| None, |ts| ts.to_timestamp_millis().ok())
        });
        Ok(last_timestamp)
    }

    pub async fn update_last_timestamp<'c, E>(
        executor: E,
        file_type: &str,
        timestamp: Option<DateTime<Utc>>,
    ) -> Result<(), MetaError>
    where
        E: sqlx::Executor<'c, Database = sqlx::Postgres>,
    {
        sqlx::query(
            r#"
            insert into meta (key, value)
            values ($1, $2)
            on conflict (key) do update set
                value = EXCLUDED.value
            "#,
        )
        .bind(file_type)
        .bind(timestamp.map(|v| v.timestamp_millis().to_string()))
        .execute(executor)
        .await?;
        Ok(())
    }
}
