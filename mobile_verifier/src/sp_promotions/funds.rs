use std::collections::HashMap;

use chrono::Utc;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use sqlx::{PgPool, Postgres, Transaction};

use super::ServiceProviderId;

#[derive(Debug, Default)]
pub struct ServiceProviderFunds(HashMap<ServiceProviderId, u16>);

impl ServiceProviderFunds {
    pub fn fetch_incentive_escrow_fund_percent(
        &self,
        service_provider_id: ServiceProviderId,
    ) -> Decimal {
        let bps = self
            .0
            .get(&service_provider_id)
            .cloned()
            .unwrap_or_default();
        Decimal::from(bps) / dec!(10_000)
    }
}

pub async fn get_promotion_funds(pool: &PgPool) -> anyhow::Result<ServiceProviderFunds> {
    #[derive(Debug, sqlx::FromRow)]
    struct PromotionFund {
        #[sqlx(try_from = "i64")]
        pub service_provider: ServiceProviderId,
        #[sqlx(try_from = "i64")]
        pub basis_points: u16,
    }

    let funds = sqlx::query_as::<_, PromotionFund>(
        r#"
            SELECT
                service_provider, basis_points
            FROM
                service_provider_promotion_funds
            "#,
    )
    .fetch_all(pool)
    .await?;

    let funds = funds
        .into_iter()
        .map(|fund| (fund.service_provider, fund.basis_points))
        .collect();

    Ok(ServiceProviderFunds(funds))
}

pub async fn save_promotion_fund(
    transaction: &mut Transaction<'_, Postgres>,
    service_provider_id: ServiceProviderId,
    basis_points: u16,
) -> anyhow::Result<()> {
    sqlx::query(
        r#"
            INSERT INTO service_provider_promotion_funds
                (service_provider, basis_points, inserted_at)
            VALUES
                ($1, $2, $3)
        "#,
    )
    .bind(service_provider_id)
    .bind(basis_points as i64)
    .bind(Utc::now())
    .execute(transaction)
    .await?;

    Ok(())
}

pub async fn delete_promotion_fund(
    pool: &PgPool,
    service_provider_id: ServiceProviderId,
) -> anyhow::Result<()> {
    sqlx::query(
        r#"
            DELETE FROM service_provider_promotion_funds
            WHERE service_provider = $1
        "#,
    )
    .bind(service_provider_id)
    .execute(pool)
    .await?;

    Ok(())
}
