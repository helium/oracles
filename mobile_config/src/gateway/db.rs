use chrono::{DateTime, Utc};
use futures::Stream;
use helium_crypto::PublicKeyBinary;
use sqlx::{postgres::PgRow, FromRow, PgPool, Row};
use std::convert::TryFrom;

// Postgres enum: device_type
#[derive(Debug, Clone, Copy, PartialEq, Eq, sqlx::Type)]
#[sqlx(type_name = "device_type")]
pub enum DeviceType {
    #[sqlx(rename = "cbrs")]
    Cbrs,
    #[sqlx(rename = "wifiIndoor")]
    WifiIndoor,
    #[sqlx(rename = "wifiOutdoor")]
    WifiOutdoor,
    #[sqlx(rename = "wifiDataOnly")]
    WifiDataOnly,
}

#[derive(Debug)]
pub struct Gateway {
    pub address: PublicKeyBinary,
    pub device_type: DeviceType,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub refreshed_at: DateTime<Utc>,
    pub antenna: Option<u32>,
    pub elevation: Option<u32>,
    pub azimuth: Option<u32>,
    pub radio_id: Option<String>,
    pub location: Option<u64>,
    pub location_changed_at: Option<DateTime<Utc>>,
    pub location_asserts: Option<u32>,
}

impl Gateway {
    pub async fn insert(&self, pool: &PgPool) -> anyhow::Result<()> {
        sqlx::query(
            r#"
            INSERT INTO gateways (
                address,
                device_type,
                created_at,
                updated_at,
                refreshed_at,
                antena,
                elevation,
                azimuth,
                radio_id,
                location,
                location_changed_at,
                location_asserts
            )
            VALUES (
                $1, $2, $3, $4, $5,
                $6, $7, $8, $9, $10, $11, $12
            )
            ON CONFLICT (address)
            DO UPDATE SET
                updated_at = EXCLUDED.updated_at,
                refreshed_at = EXCLUDED.refreshed_at,
                antena = EXCLUDED.antena,
                elevation = EXCLUDED.elevation,
                azimuth = EXCLUDED.azimuth,
                radio_id = EXCLUDED.radio_id,
                location = EXCLUDED.location,
                location_changed_at = EXCLUDED.location_changed_at,
                location_asserts = EXCLUDED.location_asserts
            "#,
        )
        .bind(self.address.as_ref())
        .bind(self.device_type)
        .bind(self.created_at)
        .bind(self.updated_at)
        .bind(self.refreshed_at)
        .bind(self.antenna.map(|v| v as i64))
        .bind(self.elevation.map(|v| v as i64))
        .bind(self.azimuth.map(|v| v as i64))
        .bind(&self.radio_id)
        .bind(self.location.map(|v| v as i64))
        .bind(self.location_changed_at)
        .bind(self.location_asserts.map(|v| v as i64))
        .execute(pool)
        .await?;

        Ok(())
    }

    pub fn stream_gateways(pool: &PgPool) -> impl Stream<Item = Result<Gateway, sqlx::Error>> + '_ {
        sqlx::query_as::<_, Gateway>(
            r#"
        SELECT
            address,
            device_type,
            created_at,
            updated_at,
            refreshed_at,
            antenna,
            elevation,
            azimuth,
            radio_id,
            location,
            location_changed_at,
            location_asserts
        FROM gateways
        ORDER BY address
        "#,
        )
        .fetch(pool)
    }
}

impl FromRow<'_, PgRow> for Gateway {
    fn from_row(row: &PgRow) -> sqlx::Result<Self> {
        // helpers to map Option<i64> -> Option<u32/u64>
        let to_u32 = |v: Option<i64>| -> Option<u32> { v.map(|x| x as u32) };
        let to_u64 = |v: Option<i64>| -> Option<u64> { v.map(|x| x as u64) };

        Ok(Self {
            address: PublicKeyBinary::try_from(row.try_get::<Vec<u8>, _>("address")?)
                .map_err(|e| sqlx::Error::Decode(Box::new(e)))?,
            device_type: row.try_get("device_type")?,
            created_at: row.try_get("created_at")?,
            updated_at: row.try_get("updated_at")?,
            refreshed_at: row.try_get("refreshed_at")?,
            antenna: to_u32(row.try_get::<Option<i64>, _>("antena")?),
            elevation: to_u32(row.try_get::<Option<i64>, _>("elevation")?),
            azimuth: to_u32(row.try_get::<Option<i64>, _>("azimuth")?),
            radio_id: row.try_get::<Option<String>, _>("radio_id")?,
            location: to_u64(row.try_get::<Option<i64>, _>("location")?),
            location_changed_at: row.try_get::<Option<DateTime<Utc>>, _>("location_changed_at")?,
            location_asserts: to_u32(row.try_get::<Option<i64>, _>("location_asserts")?),
        })
    }
}
