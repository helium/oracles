use crate::{Error, Result};
use chrono::{DateTime, Utc};
use futures::TryFutureExt;
use serde::{Deserialize, Serialize};

#[derive(sqlx::Type, Serialize, Deserialize, Debug)]
#[sqlx(type_name = "status", rename_all = "lowercase")]
pub enum Status {
    Created,
    Pending,
    Cleared,
    Failed,
}

impl Status {
    fn update_query(&self) -> &'static str {
        match self {
            Self::Created => {
                r#" 
                update pending_txn set 
                    status = $1
                where hash = $2; 
                "#
            }
            Self::Pending => {
                r#" 
                update pending_txn set 
                    status = $1, 
                    submitted_at = $3 
                where hash = $2; 
                "#
            }
            Self::Cleared | Self::Failed => {
                r#" 
                update pending_txn set 
                    status = $1, 
                    completed_at = $3 
                where hash = $2; 
                "#
            }
        }
    }

    fn update_all_query(&self) -> &'static str {
        match self {
            Self::Created => {
                r#" 
                update pending_txn set 
                    status = $1
                where hash = any($3); 
                "#
            }
            Self::Pending => {
                r#" 
                update pending_txn set 
                    status = $1, 
                    submitted_at = $2 
                where hash = any($3); 
                "#
            }
            Self::Cleared | Self::Failed => {
                r#" 
                update pending_txn set 
                    status = $1, 
                    completed_at = $2
                where hash = any($3); 
                "#
            }
        }
    }
}

#[derive(sqlx::FromRow, Deserialize, Serialize, Debug)]
#[sqlx(type_name = "pending_txn")]
pub struct PendingTxn {
    pub hash: String,
    pub status: Status,
    pub failed_reason: Option<String>,

    pub submitted_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,

    pub created_at: Option<DateTime<Utc>>,
    pub updated_at: Option<DateTime<Utc>>,
}

impl PendingTxn {
    pub fn pending_key(&self) -> Result<Vec<u8>> {
        self.created_at
            .map(|ts| ts.timestamp_millis().to_be_bytes().to_vec())
            .ok_or_else(|| Error::not_found("no created at in pending txn"))
    }

    pub fn submitted_at(&self) -> Result<DateTime<Utc>> {
        self.submitted_at
            .ok_or_else(|| Error::not_found("no pending submitted_at present"))
    }

    pub fn created_at(&self) -> Result<DateTime<Utc>> {
        self.created_at
            .ok_or_else(|| Error::not_found("no pending created_at present"))
    }

    pub async fn insert_new<'c, E>(executor: E, hash: String) -> Result<Self>
    where
        E: sqlx::Executor<'c, Database = sqlx::Postgres>,
    {
        let pt = PendingTxn {
            hash,
            status: Status::Created,

            failed_reason: None,
            created_at: None,
            updated_at: None,

            submitted_at: None,
            completed_at: None,
        };
        pt.insert_into(executor).await
    }

    pub async fn insert_into<'c, E>(&self, executor: E) -> Result<Self>
    where
        E: sqlx::Executor<'c, Database = sqlx::Postgres>,
    {
        sqlx::query_as::<_, Self>(
            r#" 
            insert into pending_txn ( hash, status) 
            values ($1, $2) 
            on conflict (hash) do nothing
            returning *;
            "#,
        )
        .bind(&self.hash)
        .bind(&self.status)
        .fetch_one(executor)
        .map_err(Error::from)
        .await
    }

    pub async fn update<'c, E, T>(executor: E, hash: &str, status: Status, timestamp: T) -> Result
    where
        E: sqlx::Executor<'c, Database = sqlx::Postgres>,
        T: Into<DateTime<Utc>>,
    {
        sqlx::query(status.update_query())
            .bind(status)
            .bind(hash)
            .bind(timestamp.into())
            .execute(executor)
            .map_ok(|_| ())
            .map_err(Error::from)
            .await
    }

    pub async fn update_all<'c, E, T>(
        executor: E,
        hashes: Vec<String>,
        status: Status,
        timestamp: T,
    ) -> Result
    where
        E: sqlx::Executor<'c, Database = sqlx::Postgres>,
        T: Into<DateTime<Utc>>,
    {
        sqlx::query(status.update_all_query())
            .bind(status)
            .bind(hashes)
            .bind(timestamp.into())
            .execute(executor)
            .map_ok(|_| ())
            .map_err(Error::from)
            .await
    }

    pub async fn list<'c, E>(executor: E, status: Status) -> Result<Vec<Self>>
    where
        E: sqlx::Executor<'c, Database = sqlx::Postgres>,
    {
        sqlx::query_as::<_, PendingTxn>(
            r#"
            select * from pending_txn where status = $1;
            "#,
        )
        .bind(status)
        .fetch_all(executor)
        .map_err(Error::from)
        .await
    }
}
