use chrono::{DateTime, Utc};
use helium_crypto::PublicKeyBinary;
use serde::{Deserialize, Serialize};
use sqlx::{postgres::PgRow, FromRow, Row};

#[derive(Deserialize, Serialize, Debug)]
pub struct LastBeacon {
    pub id: PublicKeyBinary,
    pub timestamp: DateTime<Utc>,
}

impl FromRow<'_, PgRow> for LastBeacon {
    fn from_row(row: &PgRow) -> sqlx::Result<Self> {
        Ok(Self {
            id: row.get::<Vec<u8>, &str>("id").into(),
            timestamp: row.get::<DateTime<Utc>, &str>("timestamp"),
        })
    }
}

impl LastBeacon {
    pub async fn insert_kv<'c, E>(
        executor: E,
        id: &PublicKeyBinary,
        val: &str,
    ) -> anyhow::Result<Self>
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
        .bind(id.as_ref())
        .bind(val)
        .fetch_one(executor)
        .await?)
    }

    pub async fn get<'c, E>(executor: E, id: &PublicKeyBinary) -> anyhow::Result<Option<Self>>
    where
        E: sqlx::Executor<'c, Database = sqlx::Postgres>,
    {
        Ok(
            sqlx::query_as::<_, LastBeacon>(r#" select * from last_beacon where id = $1;"#)
                .bind(id.as_ref())
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
        id: &PublicKeyBinary,
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
        .bind(id.as_ref())
        .fetch_optional(executor)
        .await?;
        Ok(height)
    }

    pub async fn update_last_timestamp<'c, E>(
        executor: E,
        id: &PublicKeyBinary,
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
        .bind(id.as_ref())
        .bind(timestamp)
        .execute(executor)
        .await?;
        Ok(())
    }
}
