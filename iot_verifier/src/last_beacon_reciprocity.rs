use chrono::{DateTime, Utc};
use helium_crypto::PublicKeyBinary;
use serde::{Deserialize, Serialize};
use sqlx::{postgres::PgRow, FromRow, Postgres, Row, Transaction};

#[derive(Deserialize, Serialize, Debug)]
pub struct LastBeaconReciprocity {
    pub id: PublicKeyBinary,
    pub timestamp: DateTime<Utc>,
}

impl FromRow<'_, PgRow> for LastBeaconReciprocity {
    fn from_row(row: &PgRow) -> sqlx::Result<Self> {
        Ok(Self {
            id: row.get::<Vec<u8>, &str>("id").into(),
            timestamp: row.get::<DateTime<Utc>, &str>("timestamp"),
        })
    }
}

impl LastBeaconReciprocity {
    pub async fn get<'c, E>(executor: E, id: &PublicKeyBinary) -> anyhow::Result<Option<Self>>
    where
        E: sqlx::Executor<'c, Database = Postgres>,
    {
        Ok(sqlx::query_as::<_, LastBeaconReciprocity>(
            r#" select * from last_beacon_recip where id = $1;"#,
        )
        .bind(id.as_ref())
        .fetch_optional(executor)
        .await?)
    }

    pub async fn get_all_since<'c, E>(
        executor: E,
        timestamp: DateTime<Utc>,
    ) -> anyhow::Result<Vec<Self>>
    where
        E: sqlx::Executor<'c, Database = sqlx::Postgres> + 'c,
    {
        Ok(
            sqlx::query_as::<_, Self>(
                r#" select * from last_beacon_recip where timestamp >= $1; "#,
            )
            .bind(timestamp)
            .fetch_all(executor)
            .await?,
        )
    }

    pub async fn update_last_timestamp(
        txn: &mut Transaction<'_, Postgres>,
        id: &PublicKeyBinary,
        timestamp: DateTime<Utc>,
    ) -> anyhow::Result<()> {
        let _ = sqlx::query(
            r#"
            insert into last_beacon_recip (id, timestamp)
            values ($1, $2)
            on conflict (id) do update set
                timestamp = EXCLUDED.timestamp
            "#,
        )
        .bind(id.as_ref())
        .bind(timestamp)
        .execute(txn)
        .await?;
        Ok(())
    }
}
