use crate::{Error, Imsi, PublicKey, Result, Uuid};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{PgConnection, Row};

#[derive(sqlx::FromRow, Deserialize, Serialize)]
pub struct CellAttachEvent {
    #[serde(skip_deserializing)]
    pub id: Uuid,
    pub imsi: Imsi,
    #[serde(alias = "publicAddress")]
    pub pubkey: PublicKey,
    #[serde(alias = "iso_timestamp")]
    pub timestamp: DateTime<Utc>,

    #[serde(skip_deserializing)]
    pub created_at: Option<DateTime<Utc>>,
}

impl CellAttachEvent {
    pub async fn insert_into(&self, conn: &mut PgConnection) -> Result<Uuid> {
        sqlx::query(
            r#"
        insert into attach_events (pubkey, imsi, timestamp)
        values ($1, $2, $3)
        returning id
            "#,
        )
        .bind(&self.pubkey)
        .bind(&self.imsi)
        .bind(self.timestamp)
        .fetch_one(conn)
        .await
        .and_then(|row| row.try_get("id"))
        .map_err(Error::from)
    }

    pub async fn get(conn: &mut PgConnection, id: &Uuid) -> Result<Option<Self>> {
        sqlx::query_as::<_, Self>(
            r#"
            select * from attach_events 
            where id = $1::uuid
            "#,
        )
        .bind(id.to_string())
        .fetch_optional(conn)
        .await
        .map_err(Error::from)
    }
}
