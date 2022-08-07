use crate::{Error, PublicKey, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(sqlx::Type, Serialize, Deserialize, Debug)]
#[sqlx(type_name = "status", rename_all = "lowercase")]
pub enum Status {
    Cleared,
    Pending,
    Failed,
}

#[derive(sqlx::FromRow, Deserialize, Serialize, Debug)]
#[sqlx(type_name = "pending_txn")]
pub struct PendingTxn {
    pub address: PublicKey,
    pub hash: String,
    pub status: Status,
    pub failed_reason: Option<String>,
    pub created_at: Option<DateTime<Utc>>,
    pub updated_at: Option<DateTime<Utc>>,
}

impl PendingTxn {
    pub async fn new(address: PublicKey, hash: String) -> PendingTxn {
        PendingTxn {
            address,
            hash,
            status: Status::Pending,

            failed_reason: None,
            created_at: None,
            updated_at: None,
        }
    }

    pub async fn insert_into<'c, E>(&self, executor: E) -> Result
    where
        E: sqlx::Executor<'c, Database = sqlx::Postgres>,
    {
        sqlx::query(
            r#" insert into pending_txn ( address, hash, status) 
            values ($1, $2, $3) 
            on conflict (hash) do nothing;
            "#,
        )
        .bind(&self.address)
        .bind(&self.hash)
        .bind(&self.status)
        .execute(executor)
        .await
        .map(|_| ())
        .map_err(Error::from)
    }

    pub async fn get_all_failed_pending_txns<'c, E>(executor: E) -> Result<Option<Vec<Self>>>
    where
        E: sqlx::Executor<'c, Database = sqlx::Postgres>,
    {
        let result =
            sqlx::query_as::<_, PendingTxn>(r#" select * from pending_txn where status = $1;"#)
                .bind(Status::Failed)
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
