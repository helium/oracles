use crate::{pagination::Since, Error, PublicKey, Result, Uuid};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{PgConnection, Row};
use std::{cmp::min, time::SystemTime};

pub const DEFAULT_SPEEDTEST_COUNT: usize = 100;
pub const MAX_SPEEDTEST_COUNT: u32 = 1000;

#[derive(sqlx::FromRow, Deserialize, Serialize)]
pub struct CellSpeedtest {
    #[serde(alias = "pubKey")]
    pub pubkey: PublicKey,
    pub serial: String,
    pub timestamp: DateTime<Utc>,
    #[serde(alias = "uploadSpeed")]
    pub upload_speed: i64,
    #[serde(alias = "downloadSpeed")]
    pub download_speed: i64,
    pub latency: i32,

    #[serde(skip_deserializing)]
    pub id: Uuid,
    #[serde(skip_deserializing)]
    pub created_at: Option<DateTime<Utc>>,
}

impl CellSpeedtest {
    pub async fn insert_into(&self, conn: &mut PgConnection) -> Result<Uuid> {
        sqlx::query(
            r#"
        insert into cell_speedtest (pubkey, serial, timestamp, upload_speed, download_speed, latency)
        values ($1, $2, $3, $4, $5, $6)
        returning id
            "#,
        )
        .bind(&self.pubkey)
        .bind(&self.serial)
        .bind(self.timestamp)
        .bind(&self.upload_speed)
        .bind(&self.download_speed)
        .bind(&self.latency)
        .fetch_one(conn)
        .await
        .and_then(|row| row.try_get("id"))
        .map_err(Error::from)
    }

    pub async fn get(conn: &mut PgConnection, id: &Uuid) -> Result<Option<Self>> {
        sqlx::query_as::<_, Self>(
            r#"
            select * from cell_speedtest 
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
            select * from cell_speedtest 
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
            MAX_SPEEDTEST_COUNT,
            since.count.unwrap_or(DEFAULT_SPEEDTEST_COUNT) as u32,
        ))
        .fetch_all(conn)
        .await
        .map_err(Error::from)
    }

    pub async fn for_hotspot_last(conn: &mut PgConnection, id: &str) -> Result<Option<Self>> {
        sqlx::query_as::<_, Self>(
            r#"
            select * from cell_speedtest 
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
