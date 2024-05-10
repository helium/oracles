use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// measurement in seconds of a piece of entropy
/// its lifespan will be valid from entropy.timestamp to entropy.timestamp + ENTROPY_LIFESPAN
/// any beacon or witness report received after this period and before the ENTROPY_STALE_PERIOD
/// defined in the purger module will be rejected due to being outside of the entropy lifespan
/// TODO: determine a sane value here
pub const ENTROPY_LIFESPAN: Duration = Duration::from_secs(180);

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

#[derive(thiserror::Error, Debug)]
#[error("entropy error: {0}")]
pub struct EntropyError(#[from] sqlx::Error);

impl Entropy {
    pub async fn insert_into<'c, E>(
        executor: E,
        id: &Vec<u8>,
        data: &Vec<u8>,
        timestamp: &DateTime<Utc>,
        version: i32,
    ) -> Result<(), EntropyError>
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
        .await?;
        Ok(())
    }

    pub async fn get<'c, E>(executor: E, id: &Vec<u8>) -> Result<Option<Self>, EntropyError>
    where
        E: sqlx::Executor<'c, Database = sqlx::Postgres>,
    {
        Ok(sqlx::query_as::<_, Self>(
            r#"
            select * from entropy
            where id = $1
            "#,
        )
        .bind(id)
        .fetch_optional(executor)
        .await?)
    }

    pub async fn purge<'c, 'q, E>(executor: E, stale_period: Duration) -> Result<(), EntropyError>
    where
        E: sqlx::Executor<'c, Database = sqlx::Postgres> + Clone,
    {
        let stale_time = Utc::now() - stale_period;
        sqlx::query(
            r#"
            delete from entropy
            where timestamp < $1
            "#,
        )
        .bind(stale_time)
        .execute(executor.clone())
        .await?;
        Ok(())
    }
}
