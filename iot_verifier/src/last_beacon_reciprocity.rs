//
// DB functions related to the last_beacon_reciprocity table
// the last_beacon_reciprocity table is used to manage the beacon reciprocity timestamp for a given gateway
// This data in this table is similar to that last_beacon table, but
// is used for a different purpose and is updated according to differing rules
// This timestamp is used as part of the poc verifications to determine if a gateway
// has valid reciprocity
//
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
    pub async fn get(
        executor: impl sqlx::PgExecutor<'_>,
        id: &PublicKeyBinary,
    ) -> anyhow::Result<Option<Self>> {
        Ok(sqlx::query_as::<_, LastBeaconReciprocity>(
            r#" select * from last_beacon_recip where id = $1;"#,
        )
        .bind(id.as_ref())
        .fetch_optional(executor)
        .await?)
    }

    pub async fn get_all_since(
        executor: impl sqlx::PgExecutor<'_>,
        timestamp: DateTime<Utc>,
    ) -> anyhow::Result<Vec<Self>> {
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
        .execute(&mut **txn)
        .await?;
        Ok(())
    }
}
