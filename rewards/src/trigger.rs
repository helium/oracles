use crate::{Error, Result};
use serde::{Deserialize, Serialize};

#[derive(sqlx::FromRow, Deserialize, Serialize, Debug, Clone, PartialEq, Eq)]
pub struct Trigger {
    pub block_height: i64,
    pub block_timestamp: i64,
}

impl Trigger {
    pub fn new(block_height: i64, block_timestamp: i64) -> Self {
        Self {
            block_height,
            block_timestamp,
        }
    }

    pub async fn insert_into<'c, E>(&self, executor: E) -> Result
    where
        E: sqlx::Executor<'c, Database = sqlx::Postgres>,
    {
        sqlx::query(
            r#" INSERT INTO cg_trigger ( block_height, block_timestamp )
            VALUES ($1, $2)
            ON CONFLICT ( block_height ) DO NOTHING;
            "#,
        )
        .bind(&self.block_height)
        .bind(&self.block_timestamp)
        .execute(executor)
        .await
        .map(|_| ())
        .map_err(Error::from)
    }
}
