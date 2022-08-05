use crate::{Error, Result};
use serde::{Deserialize, Serialize};

#[derive(sqlx::FromRow, Deserialize, Serialize, Debug)]
#[sqlx(type_name = "follower_meta")]
pub struct FollowerMeta {
    pub key: String,
    pub value: String,
}

impl FollowerMeta {
    pub async fn insert_kv<'c, E>(executor: E, key: &str, val: &str) -> Result<Option<Self>>
    where
        E: sqlx::Executor<'c, Database = sqlx::Postgres>,
    {
        sqlx::query_as::<_, Self>(
            r#" insert into follower_meta ( key, value ) 
            values ($1, $2) 
            on conflict (key) do nothing 
            returning *;
            "#,
        )
        .bind(key)
        .bind(val)
        .fetch_optional(executor)
        .await
        .map_err(Error::from)
    }

    pub async fn get<'c, E>(executor: E, key: &str) -> Result<Option<Self>>
    where
        E: sqlx::Executor<'c, Database = sqlx::Postgres>,
    {
        sqlx::query_as::<_, FollowerMeta>(r#" select * from follower_meta where key = $1;"#)
            .bind(key)
            .fetch_optional(executor)
            .await
            .map_err(Error::from)
    }

    pub async fn last_height<'c, E>(executor: E, start_block: i64) -> Result<i64>
    where
        E: sqlx::Executor<'c, Database = sqlx::Postgres>,
    {
        let height = sqlx::query_scalar::<_, String>(
            r#"
            select value from follower_meta
            where key = 'last_height'
            "#,
        )
        .fetch_optional(executor)
        .await?
        .and_then(|v| v.parse::<i64>().map_or_else(|_| None, Some))
        .unwrap_or(start_block);
        Ok(height)
    }

    pub async fn update_last_height<'c, E>(executor: E, height: i64) -> Result
    where
        E: sqlx::Executor<'c, Database = sqlx::Postgres>,
    {
        let _ = sqlx::query(
            r#"
            insert into follower_meta (key, value)
            values ('last_height', $1)
            on conflict (key) do update set
                value = EXCLUDED.value
            "#,
        )
        .bind(height.to_string())
        .execute(executor)
        .await?;
        Ok(())
    }
}
