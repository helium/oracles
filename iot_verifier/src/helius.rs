use helium_crypto::PublicKeyBinary;
use sqlx::postgres::PgRow;
use sqlx::{FromRow, Row};

#[derive(Debug, Clone)]
pub struct GatewayInfo {
    pub address: PublicKeyBinary,
    pub location: Option<u64>,
    pub elevation: Option<i32>,
    pub gain: i32,
    pub is_full_hotspot: bool,
}

impl<'c> FromRow<'c, PgRow> for GatewayInfo {
    fn from_row(row: &'c PgRow) -> Result<Self, sqlx::Error> {
        Ok(GatewayInfo {
            address: row.get(0),
            //TODO: fix location, None value is not being handled here
            location: Some(row.get::<i64, _>("location") as u64),
            elevation: row.get(2),
            gain: row.get(3),
            is_full_hotspot: row.get(4),
        })
    }
}

#[derive(thiserror::Error, Debug)]
#[error("report error: {0}")]
pub struct SqlError(#[from] sqlx::Error);

impl GatewayInfo {
    pub async fn resolve_gateway<'c, E>(
        executor: E,
        id: &[u8],
    ) -> Result<Option<GatewayInfo>, sqlx::Error>
    where
        E: sqlx::Executor<'c, Database = sqlx::Postgres>,
    {
        let result = sqlx::query_as::<_, Self>(
            r#"
            select address, location, elevation, gain, is_full_hotspot from gateways where address = $1
            "#,
        )
        .bind(id)
        .fetch_optional(executor)
        .await?;
        Ok(result)
    }
}
