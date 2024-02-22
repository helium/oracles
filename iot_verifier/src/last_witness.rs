use chrono::{DateTime, Utc};

use helium_crypto::PublicKeyBinary;
use serde::{Deserialize, Serialize};

#[derive(sqlx::FromRow, Deserialize, Serialize, Debug)]
#[sqlx(type_name = "last_witness")]
pub struct LastWitness {
    pub id: Vec<u8>,
    pub timestamp: DateTime<Utc>,
}

impl LastWitness {
    pub async fn insert_kv<'c, E>(executor: E, id: &[u8], val: &str) -> anyhow::Result<Self>
    where
        E: sqlx::Executor<'c, Database = sqlx::Postgres>,
    {
        Ok(sqlx::query_as::<_, Self>(
            r#" insert into last_witness ( id, timestamp )
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
            sqlx::query_as::<_, LastWitness>(r#" select * from last_witness where id = $1;"#)
                .bind(id)
                .fetch_optional(executor)
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
            select timestamp from last_witness
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
            insert into last_witness (id, timestamp)
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

    pub async fn bulk_update_last_timestamps(
        db: impl sqlx::PgExecutor<'_> + sqlx::Acquire<'_, Database = sqlx::Postgres> + Copy,
        ids: Vec<(PublicKeyBinary, DateTime<Utc>)>,
    ) -> anyhow::Result<()> {
        const NUMBER_OF_FIELDS_IN_QUERY: u16 = 2;
        const MAX_BATCH_ENTRIES: usize = (u16::MAX / NUMBER_OF_FIELDS_IN_QUERY) as usize;
        let mut txn = db.begin().await?;
        for updates in ids.chunks(MAX_BATCH_ENTRIES) {
            let mut query_builder: sqlx::QueryBuilder<sqlx::Postgres> =
                sqlx::QueryBuilder::new(" insert into last_witness (id, timestamp) ");
            query_builder.push_values(updates, |mut builder, (id, ts)| {
                builder.push_bind(id.as_ref()).push_bind(ts);
            });
            query_builder.push(" on conflict (id) do update set timestamp = EXCLUDED.timestamp ");
            query_builder.build().execute(&mut *txn).await?;
        }
        txn.commit().await?;
        Ok(())
    }
}
