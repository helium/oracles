use crate::{Error, PublicKey, Result, Since, Uuid};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{PgConnection, Row};
use std::{cmp::min, time::SystemTime};

pub const DEFAULT_HEARTBEAT_COUNT: usize = 100;
pub const MAX_HEARTBEAT_COUNT: u32 = 1000;

#[derive(sqlx::FromRow, Deserialize, Serialize)]
pub struct CellHeartbeat {
    #[serde(alias = "pubKey")]
    pub pubkey: PublicKey,
    pub hotspot_type: String,
    pub cell_id: i32,
    pub timestamp: DateTime<Utc>,
    #[serde(alias = "longitude")]
    pub lon: f64,
    #[serde(alias = "latitude")]
    pub lat: f64,
    pub operation_mode: bool,
    pub cbsd_category: String,
    pub cbsd_id: String,

    #[serde(skip_deserializing)]
    pub id: Uuid,
    #[serde(skip_deserializing)]
    pub created_at: Option<DateTime<Utc>>,
}

impl CellHeartbeat {
    pub async fn insert_into<'e, 'c, E>(&self, executor: E) -> Result<Uuid>
    where
        E: 'e + sqlx::Executor<'c, Database = sqlx::Postgres>,
    {
        sqlx::query(
            r#"
        insert into cell_heartbeat (pubkey, hotspot_type, cell_id, timestamp, lat, lon, operation_mode, cbsd_category)
        values ($1, $2, $3, $4, $5, $6, $7, $8)
        returning id
            "#,
        )
        .bind(&self.pubkey)
        .bind(&self.hotspot_type)
        .bind(&self.cell_id)
        .bind(self.timestamp)
        .bind(&self.lat)
        .bind(&self.lon)
        .bind(&self.operation_mode)
        .bind(&self.cbsd_category)
        .fetch_one(executor)
        .await
        .and_then(|row| row.try_get("id"))
        .map_err(Error::from)
    }

    pub async fn get(conn: &mut PgConnection, id: &Uuid) -> Result<Option<Self>> {
        sqlx::query_as::<_, Self>(
            r#"
            select * from cell_heartbeat 
            where id = $1::uuid
            "#,
        )
        .bind(id)
        .fetch_optional(conn)
        .await
        .map_err(Error::from)
    }

    pub async fn for_hotspot_since(
        conn: &mut PgConnection,
        id: &str,
        since: &Since,
    ) -> Result<Vec<Self>> {
        sqlx::query_as::<_, Self>(
            r#"
            select * from cell_heartbeat 
            where pubkey = $1 and timestamp > $2
            order by timestamp asc
            limit $3
            "#,
        )
        .bind(id)
        .bind(
            since
                .since
                .unwrap_or_else(|| DateTime::<Utc>::from(SystemTime::UNIX_EPOCH)),
        )
        .bind(min(
            MAX_HEARTBEAT_COUNT as i32,
            since.count.unwrap_or(DEFAULT_HEARTBEAT_COUNT) as i32,
        ))
        .fetch_all(conn)
        .await
        .map_err(Error::from)
    }

    pub async fn for_hotspot_last(conn: &mut PgConnection, id: &str) -> Result<Option<Self>> {
        sqlx::query_as::<_, Self>(
            r#"
            select * from cell_heartbeat 
            where pubkey = $1
            order by timestamp desc
            limit 1
            "#,
        )
        .bind(id)
        .fetch_optional(conn)
        .await
        .map_err(Error::from)
    }
}
