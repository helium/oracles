use crate::{Error, PublicKey, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(sqlx::Type, Serialize, Deserialize, Debug)]
#[sqlx(type_name = "status")]
#[sqlx(rename_all = "lowercase")]
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
    pub failed_reason: Option<String>,
    pub status: Status,

    #[serde(skip_deserializing)]
    pub created_at: Option<DateTime<Utc>>,
    #[serde(skip_deserializing)]
    pub updated_at: Option<DateTime<Utc>>,
}

impl PendingTxn {
    pub async fn insert_into<'c, E>(&self, executor: E) -> Result
    where
        E: sqlx::Executor<'c, Database = sqlx::Postgres>,
    {
        sqlx::query(
            r#"
        insert into pending_txn (
            address, 
            hash, 
            status
        ) values ($1, $2, $3, $4)
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
}
