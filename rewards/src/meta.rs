use crate::{error::DecodeError, Error, Result};
use futures::TryFutureExt;
use serde::{Deserialize, Serialize};
use std::str::FromStr;

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
            r#"
            insert into meta ( key, value ) 
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

    pub async fn update_all<'c, 'i, E, T>(executor: E, tuples: T) -> Result
    where
        E: sqlx::Executor<'c, Database = sqlx::Postgres>,
        T: IntoIterator<Item = &'i (&'i str, String)>,
    {
        let (keys, values): (Vec<&str>, Vec<String>) = tuples.into_iter().cloned().unzip();
        sqlx::query(
            r#"
            insert into meta (key, value) 
            select * from unnest($1::text[], $2::text[]) 
            on conflict (key) do update set 
            value = EXCLUDED.value;
            "#,
        )
        .bind(keys)
        .bind(values)
        .execute(executor)
        .map_ok(|_| ())
        .map_err(Error::from)
        .await
    }

    pub async fn update<'c, E>(executor: E, key: &str, val: String) -> Result
    where
        E: sqlx::Executor<'c, Database = sqlx::Postgres>,
    {
        sqlx::query(
            r#"
            insert into meta (key, value) 
            values ($1, $2) 
            on conflict (key) do update set 
            value = EXCLUDED.value;
            "#,
        )
        .bind(key)
        .bind(val)
        .execute(executor)
        .map_ok(|_| ())
        .map_err(Error::from)
        .await
    }

    async fn fetch_scalar<'c, E, T>(executor: E, key: &str) -> Result<Option<T>>
    where
        E: sqlx::Executor<'c, Database = sqlx::Postgres>,
        T: FromStr,
        DecodeError: From<<T as FromStr>::Err>,
    {
        sqlx::query_scalar::<_, String>(
            r#"
            select value from meta where key = $1;
            "#,
        )
        .bind(key)
        .fetch_optional(executor)
        .await?
        .map_or_else(
            || Ok(None),
            |v| v.parse::<T>().map_err(DecodeError::from).map(Some),
        )
        .map_err(Error::from)
    }

    pub async fn last_reward_end_time<'c, E>(executor: E) -> Result<Option<i64>>
    where
        E: sqlx::Executor<'c, Database = sqlx::Postgres>,
    {
        Self::fetch_scalar::<E, i64>(executor, "last_reward_end_time").await
    }

    pub async fn last_follower_height<'c, E>(executor: E) -> Result<Option<i64>>
    where
        E: sqlx::Executor<'c, Database = sqlx::Postgres>,
    {
        Self::fetch_scalar::<E, i64>(executor, "last_follower_height").await
    }
}
