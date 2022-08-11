use crate::{Error, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(sqlx::Type, Serialize, Deserialize, Debug)]
#[sqlx(type_name = "status", rename_all = "lowercase")]
pub enum Status {
    Cleared,
    Created,
    Failed,
    Pending,
}

impl Status {
    const SELECT: &'static str = r#" select * from pending_txn where status = $1; "#;
    const UPDATE: &'static str = r#" update pending_txn set status = $1 where hash = $2; "#;

    fn select_query(&self) -> &'static str { Self::SELECT }
    fn update_query(&self) -> &'static str { Self::UPDATE }
}

#[derive(sqlx::FromRow, Deserialize, Serialize, Debug)]
#[sqlx(type_name = "pending_txn")]
pub struct PendingTxn {
    pub hash: String,
    pub status: Status,
    pub failed_reason: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl PendingTxn {
    pub async fn new(hash: String) -> PendingTxn {
        PendingTxn {
            hash,
            status: Status::Pending,

            failed_reason: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    pub async fn insert_into<'c, E>(&self, executor: E) -> Result
    where
        E: sqlx::Executor<'c, Database = sqlx::Postgres>,
    {
        sqlx::query(
            r#" insert into pending_txn ( hash, status) 
            values ($1, $2) 
            on conflict (hash) do nothing;
            "#,
        )
        .bind(&self.hash)
        .bind(&self.status)
        .execute(executor)
        .await
        .map(|_| ())
        .map_err(Error::from)
    }

    pub async fn update<'c, E>(executor: E, hash: &str, status: Status) -> Result
    where
        E: sqlx::Executor<'c, Database = sqlx::Postgres>,
    {
        let updated_rows = sqlx::query(status.update_query())
            .bind(status)
            .bind(&hash)
            .execute(executor)
            .await
            .map(|res| res.rows_affected())
            .map_err(Error::from)?;
        if updated_rows == 0 {
            Err(Error::not_found(format!("txn {hash} not found")))
        } else {
            Ok(())
        }
    }

    pub async fn list<'c, E>(executor: E, status: Status) -> Result<Option<Vec<Self>>>
    where
        E: sqlx::Executor<'c, Database = sqlx::Postgres>,
    {
        let result = sqlx::query_as::<_, PendingTxn>(status.select_query())
            .bind(status)
            .fetch_all(executor)
            .await
            .map_err(Error::from);
        match result {
            Ok(res) => {
                if res.is_empty() {
                    return Ok(None);
                }
                Ok(Some(res))
            }
            Err(e) => Err(e),
        }
    }
}
