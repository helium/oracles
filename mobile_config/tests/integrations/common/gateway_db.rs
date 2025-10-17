use chrono::{DateTime, Utc};
use helium_crypto::PublicKeyBinary;
use mobile_config::gateway::db::GatewayType;
use sqlx::PgPool;

#[derive(Debug, Clone)]
pub struct PreHistoricalGateway {
    pub address: PublicKeyBinary,
    pub gateway_type: GatewayType,
    // When the record was first created from metadata DB
    pub created_at: DateTime<Utc>,
    // When record was last updated
    pub updated_at: DateTime<Utc>,
    // When record was last updated from metadata DB (could be set to now if no metadata DB info)
    pub refreshed_at: DateTime<Utc>,
    // When location or hash last changed, set to refreshed_at (updated via SQL query see Gateway::insert)
    pub last_changed_at: DateTime<Utc>,
    pub hash: String,
    pub antenna: Option<u32>,
    pub elevation: Option<u32>,
    pub azimuth: Option<u32>,
    pub location: Option<u64>,
    // When location last changed, set to refreshed_at (updated via SQL query see Gateway::insert)
    pub location_changed_at: Option<DateTime<Utc>>,
    pub location_asserts: Option<u32>,
}

impl PreHistoricalGateway {
    pub async fn insert(&self, pool: &PgPool) -> anyhow::Result<()> {
        sqlx::query(
            r#"
            INSERT INTO gateways (
                address,
                gateway_type,
                created_at,
                updated_at,
                refreshed_at,
                last_changed_at,
                hash,
                antenna,
                elevation,
                azimuth,
                location,
                location_changed_at,
                location_asserts
            )
            VALUES (
                $1, $2, $3, $4, $5, $6, $7,
                $8, $9, $10, $11, $12, $13
            )
            "#,
        )
        .bind(self.address.as_ref())
        .bind(self.gateway_type)
        .bind(self.created_at)
        .bind(self.updated_at)
        .bind(self.refreshed_at)
        .bind(self.last_changed_at)
        .bind(self.hash.as_str())
        .bind(self.antenna.map(|v| v as i64))
        .bind(self.elevation.map(|v| v as i64))
        .bind(self.azimuth.map(|v| v as i64))
        .bind(self.location.map(|v| v as i64))
        .bind(self.location_changed_at)
        .bind(self.location_asserts.map(|v| v as i64))
        .execute(pool)
        .await?;

        Ok(())
    }
}
