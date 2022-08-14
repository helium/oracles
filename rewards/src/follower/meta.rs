use crate::{error::DecodeError, Error, Result};
use serde::{Deserialize, Serialize};

#[derive(sqlx::FromRow, Deserialize, Serialize, Debug)]
#[sqlx(type_name = "meta")]
pub struct Meta {
    pub key: String,
    pub value: String,
}

impl Meta {
    const UPSERT: &'static str = r#"
        insert into meta (key, value) 
        values ($1, $2) 
        on conflict (key) do update set 
        value = EXCLUDED.value;
    "#;
    const SELECT_VALUE: &'static str = r#"select value from meta where key = $1;"#;
    const INSERT: &'static str = r#"
        insert into meta ( key, value ) 
        values ($1, $2) 
        on conflict (key) do nothing 
        returning *;
    "#;

    pub async fn insert_kv<'c, E>(executor: E, key: &str, val: &str) -> Result<Self>
    where
        E: sqlx::Executor<'c, Database = sqlx::Postgres>,
    {
        sqlx::query_as::<_, Self>(Self::INSERT)
            .bind(key)
            .bind(val)
            .fetch_one(executor)
            .await
            .map_err(Error::from)
    }

    pub async fn update<'c, E>(executor: E, key: &str, val: &str) -> Result
    where
        E: sqlx::Executor<'c, Database = sqlx::Postgres>,
    {
        let _ = sqlx::query(Self::UPSERT)
            .bind(key)
            .bind(val)
            .execute(executor)
            .await?;
        Ok(())
    }

    pub async fn last_reward_end_time<'c, E>(executor: E) -> Result<Option<i64>>
    where
        E: sqlx::Executor<'c, Database = sqlx::Postgres>,
    {
        sqlx::query_scalar::<_, String>(Self::SELECT_VALUE)
            .bind("last_reward_end_time")
            .fetch_optional(executor)
            .await?
            .map_or_else(
                || Ok(None),
                |v| v.parse::<i64>().map_err(DecodeError::from).map(Some),
            )
            .map_err(Error::from)
    }

    pub async fn last_reward_height<'c, E>(executor: E) -> Result<Option<i64>>
    where
        E: sqlx::Executor<'c, Database = sqlx::Postgres>,
    {
        sqlx::query_scalar::<_, String>(Self::SELECT_VALUE)
            .bind("last_reward_height")
            .fetch_optional(executor)
            .await?
            .map_or_else(
                || Ok(None),
                |v| v.parse::<i64>().map_err(DecodeError::from).map(Some),
            )
            .map_err(Error::from)
    }

    pub async fn last_height<'c, E>(executor: E, start_block: i64) -> Result<i64>
    where
        E: sqlx::Executor<'c, Database = sqlx::Postgres>,
    {
        let height = sqlx::query_scalar::<_, String>(Self::SELECT_VALUE)
            .bind("last_height")
            .fetch_optional(executor)
            .await?
            .and_then(|v| v.parse::<i64>().map_or_else(|_| None, Some))
            .unwrap_or(start_block);
        Ok(height)
    }
}
