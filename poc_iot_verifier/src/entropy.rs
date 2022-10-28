use crate::{Error, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// measurement in seconds of a piece of entropy
/// its lifespan will be valid from entropy.timestamp to entropy.timestamp + ENTROPY_LIFESPAN
/// any beacon or witness report received after this period and before the ENTROPY_STALE_PERIOD
/// defined in the purger module will be rejected due to being outside of the entropy lifespan
/// TODO: determine a sane value here
pub const ENTROPY_LIFESPAN: i64 = 90;

#[derive(sqlx::Type, Serialize, Deserialize, Debug)]
#[sqlx(type_name = "report_type", rename_all = "lowercase")]
pub enum ReportType {
    Witness,
    Beacon,
}

#[derive(sqlx::FromRow, Deserialize, Serialize, Debug)]
#[sqlx(type_name = "entropy")]
pub struct Entropy {
    pub id: Vec<u8>,
    pub data: Vec<u8>,
    pub timestamp: DateTime<Utc>,
    pub version: i32,
    pub created_at: DateTime<Utc>,
}

impl Entropy {
    pub async fn insert_into<'c, E>(
        executor: E,
        id: &Vec<u8>,
        data: &Vec<u8>,
        timestamp: &DateTime<Utc>,
        version: i32,
    ) -> Result
    where
        E: sqlx::Executor<'c, Database = sqlx::Postgres>,
    {
        sqlx::query(
            r#"
        insert into entropy (
            id,
            data,
            timestamp,
            version
        ) values ($1, $2, $3, $4)
        on conflict (id) do nothing
            "#,
        )
        .bind(id)
        .bind(data)
        .bind(timestamp)
        .bind(version)
        .execute(executor)
        .await
        .map(|_| ())
        .map_err(Error::from)
    }

    pub async fn get<'c, E>(executor: E, id: &Vec<u8>) -> Result<Option<Self>>
    where
        E: sqlx::Executor<'c, Database = sqlx::Postgres>,
    {
        sqlx::query_as::<_, Self>(
            r#"
            select * from entropy
            where id = $1
            order by created_at asc
            "#,
        )
        .bind(id)
        .fetch_optional(executor)
        .await
        .map_err(Error::from)
    }

    pub async fn purge<'c, 'q, E>(executor: E, stale_period: i32) -> Result
    where
        E: sqlx::Executor<'c, Database = sqlx::Postgres> + Clone,
    {
        sqlx::query(
            r#"
            delete from entropy
            where timestamp < (NOW() - INTERVAL '$1 MINUTES')
            "#,
        )
        .bind(stale_period)
        .execute(executor.clone())
        .await
        .map(|_| ())
        .map_err(Error::from)
    }
}
