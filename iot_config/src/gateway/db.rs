use chrono::{DateTime, Utc};
use futures::{Stream, StreamExt, TryStreamExt};
use helium_crypto::PublicKeyBinary;
use sqlx::{postgres::PgRow, FromRow, PgExecutor, PgPool, Postgres, QueryBuilder, Row};

#[derive(Debug, Clone)]
pub struct Gateway {
    pub address: PublicKeyBinary,
    // When the record was first created from metadata DB
    pub created_at: DateTime<Utc>,
    pub elevation: Option<u32>,
    pub gain: Option<u32>,
    pub hash: String,
    pub is_active: Option<bool>,
    pub is_full_hotspot: Option<bool>,
    // When location or hash last changed, set to refreshed_at (updated via SQL query see Gateway::insert)
    pub last_changed_at: DateTime<Utc>,
    pub location: Option<u64>,
    pub location_asserts: Option<u32>,
    // When location last changed, set to refreshed_at (updated via SQL query see Gateway::insert)
    pub location_changed_at: Option<DateTime<Utc>>,
    // When record was last updated from metadata DB (could be set to now if no metadata DB info)
    pub refreshed_at: Option<DateTime<Utc>>,
    // When record was last updated
    pub updated_at: DateTime<Utc>,
}

impl Gateway {
    pub async fn insert_bulk(pool: &PgPool, rows: &[Gateway]) -> anyhow::Result<u64> {
        if rows.is_empty() {
            return Ok(0);
        }
        let mut qb = QueryBuilder::<Postgres>::new(
            "INSERT INTO gateways (
                address,
                created_at,
                elevation,
                gain,
                hash,
                is_active,
                is_full_hotspot,
                last_changed_at,
                location,
                location_asserts,
                location_changed_at,
                refreshed_at,
                updated_at
            ) ",
        );

        qb.push_values(rows, |mut b, g| {
            b.push_bind(g.address.as_ref())
                .push_bind(g.created_at)
                .push_bind(g.elevation.map(|v| v as i32))
                .push_bind(g.gain.map(|v| v as i32))
                .push_bind(g.hash.clone())
                .push_bind(g.is_active)
                .push_bind(g.is_full_hotspot)
                .push_bind(g.last_changed_at)
                .push_bind(g.location.map(|v| v as i64))
                .push_bind(g.location_asserts.map(|v| v as i32))
                .push_bind(g.location_changed_at)
                .push_bind(g.refreshed_at)
                .push_bind(g.updated_at);
        });

        qb.push(
            " ON CONFLICT (address) DO UPDATE SET 
                created_at = EXCLUDED.created_at,
                elevation = EXCLUDED.elevation,
                gain = EXCLUDED.gain,
                hash = EXCLUDED.hash,
                is_active = EXCLUDED.is_active,
                is_full_hotspot = EXCLUDED.is_full_hotspot,
                last_changed_at = CASE 
                    WHEN gateways.location IS DISTINCT FROM EXCLUDED.location 
                      OR gateways.hash     IS DISTINCT FROM EXCLUDED.hash 
                    THEN EXCLUDED.refreshed_at 
                    ELSE gateways.last_changed_at 
                END,
                location = EXCLUDED.location,
                location_asserts = EXCLUDED.location_asserts,
                location_changed_at = CASE 
                    WHEN gateways.location IS DISTINCT FROM EXCLUDED.location 
                    THEN EXCLUDED.refreshed_at 
                    ELSE gateways.location_changed_at 
                END,
                refreshed_at = EXCLUDED.refreshed_at,
                updated_at = EXCLUDED.updated_at",
        );

        let res = qb.build().execute(pool).await?;
        Ok(res.rows_affected())
    }

    pub async fn get_by_address<'a>(
        db: impl PgExecutor<'a>,
        address: &PublicKeyBinary,
    ) -> anyhow::Result<Option<Self>> {
        let gateway = sqlx::query_as::<_, Self>(
            r#"
            SELECT
                address,
                created_at,
                elevation,
                gain,
                hash,
                is_active,
                is_full_hotspot,
                last_changed_at,
                location,
                location_asserts,
                location_changed_at,
                refreshed_at,
                updated_at
            FROM gateways
            WHERE address = $1
            "#,
        )
        .bind(address.as_ref())
        .fetch_optional(db)
        .await?;

        Ok(gateway)
    }

    pub fn stream<'a>(
        db: impl PgExecutor<'a> + 'a,
        min_last_changed_at: DateTime<Utc>,
        min_location_changed_at: Option<DateTime<Utc>>,
    ) -> impl Stream<Item = Self> + 'a {
        sqlx::query_as::<_, Self>(
            r#"
                SELECT
                    address,
                    created_at,
                    elevation,
                    gain,
                    hash,
                    is_active,
                    is_full_hotspot,
                    last_changed_at,
                    location,
                    location_asserts,
                    location_changed_at,
                    refreshed_at,
                    updated_at
                FROM gateways
                WHERE last_changed_at >= $1
                AND (
                    $2::timestamptz IS NULL
                    OR (location IS NOT NULL AND location_changed_at >= $2)
                )
            "#,
        )
        .bind(min_last_changed_at)
        .bind(min_location_changed_at)
        .fetch(db)
        .map_err(anyhow::Error::from)
        .inspect_err(|err| tracing::error!("Error streaming {:?}", err))
        .filter_map(|res| async move { res.ok() })
    }
}

impl FromRow<'_, PgRow> for Gateway {
    fn from_row(row: &PgRow) -> sqlx::Result<Self> {
        // helpers to map Option<i64> -> Option<u32/u64>
        let to_u64 = |v: Option<i64>| -> Option<u64> { v.map(|x| x as u64) };
        let to_u32 = |v: Option<i32>| -> Option<u32> { v.map(|x| x as u32) };

        Ok(Self {
            address: PublicKeyBinary::from(row.try_get::<Vec<u8>, _>("address")?),
            created_at: row.try_get("created_at")?,
            elevation: to_u32(row.try_get("elevation")?),
            gain: to_u32(row.try_get("gain")?),
            hash: row.try_get("hash")?,
            is_active: row.try_get("is_active")?,
            is_full_hotspot: row.try_get("is_full_hotspot")?,
            last_changed_at: row.try_get("last_changed_at")?,
            location: to_u64(row.try_get("location")?),
            location_asserts: to_u32(row.try_get("location_asserts")?),
            location_changed_at: row.try_get("location_changed_at")?,
            refreshed_at: row.try_get("refreshed_at")?,
            updated_at: row.try_get("updated_at")?,
        })
    }
}
