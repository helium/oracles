use crate::{
    api::{api_error, DatabaseConnection},
    Error, Imsi, PublicKey, Result,
};
use axum::{http::StatusCode, Json};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sqlx::Row;

pub async fn create_cell_attach_event(
    Json(event): Json<CellAttachEvent>,
    DatabaseConnection(mut conn): DatabaseConnection,
) -> std::result::Result<Json<Value>, (StatusCode, String)> {
    event
        .insert_into(&mut conn)
        .await
        .map(|pubkey: PublicKey| {
            json!({
                "pubkey": pubkey,
            })
        })
        .map(Json)
        .map_err(api_error)
}

#[derive(sqlx::FromRow, Deserialize, Serialize)]
pub struct CellAttachEvent {
    pub imsi: Imsi,
    #[serde(alias = "publicAddress")]
    pub pubkey: PublicKey,
    #[serde(alias = "iso_timestamp")]
    pub timestamp: DateTime<Utc>,
}

impl CellAttachEvent {
    pub async fn insert_into<'e, 'c, E>(&self, executor: E) -> Result<PublicKey>
    where
        E: 'e + sqlx::Executor<'c, Database = sqlx::Postgres>,
    {
        sqlx::query(
            r#"
        insert into gateways (pubkey, owner, payer, height, txn_hash, block_timestamp, last_heartbeat, last_speedtest, last_attach)
        values ($1, NULL, NULL, 0, NULL, NULL, NULL, NULL, $2)
        on conflict (pubkey) do update set
            last_attach = EXCLUDED.last_attach
        returning pubkey
            "#,
        )
        .bind(&self.pubkey)
        .bind(&self.timestamp)
        .fetch_one(executor)
        .await
        .and_then(|row| row.try_get("pubkey"))
        .map_err(Error::from)
    }
}
