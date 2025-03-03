//
// DB functions related to the last_witness_reciprocity table
// the last_witness_reciprocity table is used to manage the witnessing reciprocity timestamp for a given gateway
// The last witness reciprocity time is used in the poc verifications to determine if a gateway
// has valid reciprocity
//

use chrono::{DateTime, Utc};
use helium_crypto::PublicKeyBinary;
use serde::{Deserialize, Serialize};
use sqlx::{postgres::PgRow, FromRow, Row};

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct LastWitness {
    pub id: PublicKeyBinary,
    pub timestamp: DateTime<Utc>,
}

impl FromRow<'_, PgRow> for LastWitness {
    fn from_row(row: &PgRow) -> sqlx::Result<Self> {
        Ok(Self {
            id: row.get::<Vec<u8>, &str>("id").into(),
            timestamp: row.get::<DateTime<Utc>, &str>("timestamp"),
        })
    }
}

impl LastWitness {
    pub async fn insert_kv<'c, E>(
        executor: E,
        id: &PublicKeyBinary,
        val: &str,
    ) -> anyhow::Result<Self>
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
            sqlx::query_as::<_, LastWitness>(r#" select * from last_witness where id = $1;"#)
                .bind(id.as_ref())
                .fetch_optional(executor)
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
            select timestamp from last_witness
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
            insert into last_witness (id, timestamp)
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

    pub async fn bulk_update_last_timestamps(
        db: impl sqlx::PgExecutor<'_> + sqlx::Acquire<'_, Database = sqlx::Postgres> + Copy,
        ids: Vec<&LastWitness>,
    ) -> anyhow::Result<()> {
        const NUMBER_OF_FIELDS_IN_QUERY: u16 = 2;
        const MAX_BATCH_ENTRIES: usize = (u16::MAX / NUMBER_OF_FIELDS_IN_QUERY) as usize;
        let mut txn = db.begin().await?;
        for updates in ids.chunks(MAX_BATCH_ENTRIES) {
            let mut query_builder: sqlx::QueryBuilder<sqlx::Postgres> =
                sqlx::QueryBuilder::new(" insert into last_witness (id, timestamp) ");
            query_builder.push_values(updates, |mut builder, last_witness| {
                builder
                    .push_bind(last_witness.id.as_ref())
                    .push_bind(last_witness.timestamp);
            });
            query_builder.push(" on conflict (id) do update set timestamp = EXCLUDED.timestamp ");
            query_builder.build().execute(&mut *txn).await?;
        }
        txn.commit().await?;
        Ok(())
    }
}
