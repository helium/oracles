use chrono::{DateTime, Utc};
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
    pub txn_bin: Vec<u8>,
    pub status: Status,

    pub submitted_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,

    pub created_at: Option<DateTime<Utc>>,
    pub updated_at: Option<DateTime<Utc>>,
}

#[derive(thiserror::Error, Debug)]
pub enum PendingTxnError {
    #[error("{0} not found")]
    NotFound(&'static str),
    #[error("database error: {0}")]
    DatabaseError(#[from] sqlx::Error),
}

impl PendingTxn {
    pub fn pending_key(&self) -> Result<Vec<u8>, PendingTxnError> {
        self.created_at
            .map(|ts| ts.timestamp_millis().to_be_bytes().to_vec())
            .ok_or(PendingTxnError::NotFound("created_at"))
    }

    pub fn submitted_at(&self) -> Result<DateTime<Utc>, PendingTxnError> {
        self.submitted_at
            .ok_or(PendingTxnError::NotFound("submitted_at"))
    }

    pub fn created_at(&self) -> Result<DateTime<Utc>, PendingTxnError> {
        self.created_at
            .ok_or(PendingTxnError::NotFound("created_at"))
    }

    pub async fn insert_new<'c, E>(
        executor: E,
        hash: &str,
        txn_bin: Vec<u8>,
    ) -> Result<Self, PendingTxnError>
    where
        E: sqlx::Executor<'c, Database = sqlx::Postgres>,
    {
        let pt = PendingTxn {
            hash: hash.to_string(),
            txn_bin,
            status: Status::Created,

            created_at: None,
            updated_at: None,

            submitted_at: None,
            completed_at: None,
        };
        pt.insert_into(executor).await
    }

    pub async fn insert_into<'c, E>(&self, executor: E) -> Result<Self, PendingTxnError>
    where
        E: sqlx::Executor<'c, Database = sqlx::Postgres>,
    {
        Ok(sqlx::query_as::<_, Self>(
            r#" 
            insert into pending_txn ( hash, txn_bin ) 
            values ($1, $2) 
            on conflict (hash) do nothing
            returning *;
            "#,
        )
        .bind(&self.hash)
        .bind(&self.txn_bin)
        .fetch_one(executor)
        .await?)
    }

    pub async fn update<'c, E, T>(
        executor: E,
        hash: &str,
        status: Status,
        timestamp: T,
    ) -> Result<(), PendingTxnError>
    where
        E: sqlx::Executor<'c, Database = sqlx::Postgres>,
        T: Into<DateTime<Utc>>,
    {
        sqlx::query(status.update_query())
            .bind(status)
            .bind(hash)
            .bind(timestamp.into())
            .execute(executor)
            .await?;
        Ok(())
    }

    pub async fn update_all<'c, E, T>(
        executor: E,
        hashes: Vec<String>,
        status: Status,
        timestamp: T,
    ) -> Result<(), PendingTxnError>
    where
        E: sqlx::Executor<'c, Database = sqlx::Postgres>,
        T: Into<DateTime<Utc>>,
    {
        sqlx::query(status.update_all_query())
            .bind(status)
            .bind(timestamp.into())
            .bind(hashes)
            .execute(executor)
            .await?;
        Ok(())
    }

    pub async fn list<'c, E>(executor: E, status: Status) -> Result<Vec<Self>, PendingTxnError>
    where
        E: sqlx::Executor<'c, Database = sqlx::Postgres>,
    {
        Ok(sqlx::query_as::<_, PendingTxn>(
            r#"
            select * from pending_txn where status = $1;
            "#,
        )
        .bind(status)
        .fetch_all(executor)
        .await?)
    }
}
