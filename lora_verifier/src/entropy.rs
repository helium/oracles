use crate::{Error, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

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
    pub created_at: DateTime<Utc>,
}

impl Entropy {
    pub async fn insert_into<'c, E>(
        executor: E,
        id: &Vec<u8>,
        data: &Vec<u8>,
        timestamp: &DateTime<Utc>,
    ) -> Result
    where
        E: sqlx::Executor<'c, Database = sqlx::Postgres>,
    {
        sqlx::query(
            r#"
        insert into entropy (
            id
            data,
            timestamp
        ) values ($1, $2, $3)
            "#,
        )
        .bind(id)
        .bind(data)
        .bind(timestamp)
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

    pub async fn purge<'c, 'q, E>(executor: E, timestamp: &'q DateTime<Utc>) -> Result
    where
        E: sqlx::Executor<'c, Database = sqlx::Postgres> + Clone,
    {
        sqlx::query(
            r#"
            delete from entropy
            where timestamp > $1
            "#,
        )
        .bind(timestamp)
        .execute(executor.clone())
        .await
        .map(|_| ())
        .map_err(Error::from)
    }
}
