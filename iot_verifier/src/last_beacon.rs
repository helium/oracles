use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(sqlx::FromRow, Deserialize, Serialize, Debug)]
#[sqlx(type_name = "last_beacon")]
pub struct LastBeacon {
    pub id: Vec<u8>,
    pub timestamp: DateTime<Utc>,
}

impl LastBeacon {
    pub async fn insert_kv<'c, E>(executor: E, id: &[u8], val: &str) -> anyhow::Result<Self>
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

    pub async fn get<'c, E>(executor: E, id: &[u8]) -> anyhow::Result<Option<Self>>
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
        executor: E,
        timestamp: DateTime<Utc>,
    ) -> anyhow::Result<Vec<Self>>
    where
        E: sqlx::Executor<'c, Database = sqlx::Postgres> + 'c,
    {
        Ok(
            sqlx::query_as::<_, Self>(r#" select * from last_beacon where timestamp >= $1; "#)
                .bind(timestamp)
                .fetch_all(executor)
                .await?,
        )
    }

    pub async fn last_timestamp<'c, E>(
        executor: E,
        id: &[u8],
    ) -> anyhow::Result<Option<DateTime<Utc>>>
    where
        E: sqlx::Executor<'c, Database = sqlx::Postgres>,
    {
        let height = sqlx::query_scalar(
            r#"
            select timestamp from last_beacon
            where id = $1
            "#,
        )
        .bind(id)
        .fetch_optional(executor)
        .await?;
        Ok(height)
    }

    pub async fn update_last_timestamp<'c, E>(
        executor: E,
        id: &[u8],
        timestamp: DateTime<Utc>,
    ) -> anyhow::Result<()>
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
