use chrono::{DateTime, Utc};
use derive_builder::Builder;
use helium_crypto::{PublicKey, PublicKeyBinary};
use mobile_config::gateway::db::{Gateway, GatewayType};
use sqlx::PgPool;

#[derive(Builder)]
#[builder(setter(into))]
pub struct TestGateway {
    pub address: PublicKey,
    pub gateway_type: GatewayType,
    #[builder(default)]
    created_at: DateTime<Utc>,
    #[builder(default)]
    inserted_at: DateTime<Utc>,
    #[builder(default)]
    refreshed_at: DateTime<Utc>,
    #[builder(default)]
    last_changed_at: DateTime<Utc>,
    #[builder(default)]
    hash: String,
    #[builder(default = "Some(18)")]
    antenna: Option<u32>,
    #[builder(default)]
    elevation: Option<u32>,
    #[builder(default)]
    azimuth: Option<u32>,
    #[builder(default)]
    location: Option<u64>,
    #[builder(default)]
    location_changed_at: Option<DateTime<Utc>>,
    #[builder(default)]
    location_asserts: Option<u32>,
    #[builder(default)]
    owner: Option<String>,
    #[builder(default)]
    owner_changed_at: Option<DateTime<Utc>>,
}

impl From<TestGateway> for Gateway {
    fn from(tg: TestGateway) -> Self {
        Gateway {
            address: tg.address.into(),
            gateway_type: tg.gateway_type,
            created_at: tg.created_at,
            inserted_at: tg.inserted_at,
            refreshed_at: tg.refreshed_at,
            last_changed_at: tg.last_changed_at,
            hash: tg.hash,
            antenna: tg.antenna,
            elevation: tg.elevation,
            azimuth: tg.azimuth,
            location: tg.location,
            location_changed_at: tg.location_changed_at,
            location_asserts: tg.location_asserts,
            owner: tg.owner,
            owner_changed_at: tg.owner_changed_at,
        }
    }
}

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
