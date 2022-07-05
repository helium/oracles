use crate::{Error, PublicKey, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::PgConnection;

#[derive(sqlx::FromRow, Deserialize, Serialize)]
pub struct Maker {
    pub pubkey: PublicKey,
    pub description: Option<String>,

    #[serde(skip_deserializing)]
    pub created_at: Option<DateTime<Utc>>,
}

impl Maker {
    pub fn new(pubkey: PublicKey, description: Option<String>) -> Self {
        Self {
            pubkey,
            description,
            created_at: None,
        }
    }

    pub async fn remove<'c, E>(pubkey: &PublicKey, executor: E) -> Result
    where
        E: sqlx::Executor<'c, Database = sqlx::Postgres>,
    {
        let _ = sqlx::query(
            r#"
            delete from maker where pubkey = $1
            "#,
        )
        .bind(pubkey)
        .fetch_optional(executor)
        .await
        .map_err(Error::from)?;
        Ok(())
    }

    pub async fn insert_into<'c, E>(&self, executor: E) -> Result<Self>
    where
        E: sqlx::Executor<'c, Database = sqlx::Postgres>,
    {
        sqlx::query_as::<_, Self>(
            r#"
            insert into maker (pubkey, description)
            values ($1, $2)
            on conflict (pubkey) do update set
                description = EXCLUDED.description
            returning *;
            "#,
        )
        .bind(&self.pubkey)
        .bind(&self.description)
        .fetch_one(executor)
        .await
        .map_err(Error::from)
    }

    pub async fn get(conn: &mut PgConnection, pubkey: &PublicKey) -> Result<Option<Self>> {
        sqlx::query_as::<_, Self>(
            r#"
            select * from maker 
            where pubkey = $1
            "#,
        )
        .bind(pubkey)
        .fetch_optional(conn)
        .await
        .map_err(Error::from)
    }

    pub async fn list<'e, 'c, E>(executor: E) -> Result<Vec<Self>>
    where
        E: 'e + sqlx::Executor<'c, Database = sqlx::Postgres>,
    {
        sqlx::query_as::<_, Self>(
            r#"
            select * from maker 
            order by created_at desc
            "#,
        )
        .fetch_all(executor)
        .await
        .map_err(Error::from)
    }
}
