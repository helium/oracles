use std::ops::Range;

use chrono::{DateTime, Utc};
use file_store::promotion_reward::{Entity, PromotionReward};

use mobile_config::client::{carrier_service_client::CarrierServiceVerifier, ClientError};
use rust_decimal::Decimal;
use sqlx::{PgPool, Postgres, Transaction};

use crate::service_provider::ServiceProviderId;

#[derive(Debug, Default, Clone, PartialEq)]
pub struct ServiceProviderPromotions(Vec<PromotionRewardShare>);

impl ServiceProviderPromotions {
    pub fn for_service_provider(
        &self,
        service_provider_id: ServiceProviderId,
    ) -> ServiceProviderPromotions {
        let promotions = self
            .0
            .iter()
            .filter(|x| x.service_provider_id == service_provider_id)
            .cloned()
            .collect();
        Self(promotions)
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn total_shares(&self) -> Decimal {
        self.0.iter().map(|x| Decimal::from(x.shares)).sum()
    }

    pub fn iter(&self) -> impl Iterator<Item = &PromotionRewardShare> {
        self.0.iter()
    }
}

impl<F> From<F> for ServiceProviderPromotions
where
    F: IntoIterator<Item = PromotionRewardShare>,
{
    fn from(promotions: F) -> Self {
        Self(promotions.into_iter().collect())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct PromotionRewardShare {
    pub service_provider_id: ServiceProviderId,
    pub rewardable_entity: Entity,
    pub shares: u64,
}

pub async fn save_promotion_reward(
    transaction: &mut Transaction<'_, Postgres>,
    promotion_reward: &PromotionReward,
) -> anyhow::Result<()> {
    match &promotion_reward.entity {
        Entity::SubscriberId(subscriber_id) => {
            sqlx::query(
            r#"
                INSERT INTO subscriber_promotion_rewards (time_of_reward, subscriber_id, carrier_key, shares)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT DO NOTHING
                "#
        )
        .bind(promotion_reward.timestamp)
        .bind(subscriber_id)
        .bind(&promotion_reward.carrier_pub_key)
        .bind(promotion_reward.shares as i64)
        .execute(&mut *transaction)
        .await?;
        }
        Entity::GatewayKey(gateway_key) => {
            sqlx::query(
            r#"
                INSERT INTO gateway_promotion_rewards (time_of_reward, gateway_key, carrier_key, shares)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT DO NOTHING
                "#
        )
        .bind(promotion_reward.timestamp)
        .bind(gateway_key)
        .bind(&promotion_reward.carrier_pub_key)
        .bind(promotion_reward.shares as i64)
        .execute(&mut *transaction)
        .await?;
        }
    }
    Ok(())
}

pub async fn clear_promotion_rewards(
    tx: &mut Transaction<'_, Postgres>,
    timestamp: &DateTime<Utc>,
) -> Result<(), sqlx::Error> {
    sqlx::query("DELETE FROM subscriber_promotion_rewards WHERE time_of_reward < $1")
        .bind(timestamp)
        .execute(&mut *tx)
        .await?;
    sqlx::query("DELETE FROM gateway_promotion_rewards WHERE time_of_reward < $1")
        .bind(timestamp)
        .execute(&mut *tx)
        .await?;
    Ok(())
}

pub async fn fetch_promotion_rewards(
    _pool: &PgPool,
    _carrier: &impl CarrierServiceVerifier<Error = ClientError>,
    _epoch: &Range<DateTime<Utc>>,
) -> anyhow::Result<ServiceProviderPromotions> {
    Ok(ServiceProviderPromotions::default())

    // let rewards = sqlx::query_as(
    //     r#"
    //     SELECT
    //         subscriber_id, NULL as gateway_key, SUM(shares)::bigint as shares, carrier_key
    //     FROM
    //         subscriber_promotion_rewards
    //     WHERE
    //         time_of_reward >= $1 AND time_of_reward < $2
    //     GROUP BY
    //         subscriber_id, carrier_key
    //     UNION
    //     SELECT
    //         NULL as subscriber_id, gateway_key, SUM(shares)::bigint as shares, carrier_key
    //     FROM
    //         gateway_promotion_rewards
    //     WHERE
    //         time_of_reward >= $1 AND time_of_reward < $2
    //     GROUP
    //         BY gateway_key, carrier_key
    //     "#,
    // )
    // .bind(epoch.start)
    // .bind(epoch.end)
    // .fetch(pool)
    // .map_err(anyhow::Error::from)
    // .and_then(|x: DbPromotionRewardShares| async move {
    //     let service_provider_id = carrier
    //         .payer_key_to_service_provider(&x.carrier_key.to_string())
    //         .await?;
    //     Ok(PromotionRewardShare {
    //         service_provider_id: service_provider_id as ServiceProviderId,
    //         rewardable_entity: x.rewardable_entity,
    //         shares: x.shares,
    //     })
    // })
    // .try_collect()
    // .await?;

    // Ok(ServiceProviderPromotions(rewards))
}

// struct DbPromotionRewardShares {
//     pub carrier_key: PublicKeyBinary,
//     pub rewardable_entity: Entity,
//     pub shares: u64,
// }

// impl sqlx::FromRow<'_, PgRow> for DbPromotionRewardShares {
//     fn from_row(row: &PgRow) -> sqlx::Result<Self> {
//         let subscriber_id: Option<Vec<u8>> = row.try_get("subscriber_id")?;
//         let shares: i64 = row.try_get("shares")?;
//         Ok(Self {
//             rewardable_entity: if let Some(subscriber_id) = subscriber_id {
//                 Entity::SubscriberId(subscriber_id)
//             } else {
//                 Entity::GatewayKey(row.try_get("gateway_key")?)
//             },
//             shares: shares as u64,
//             carrier_key: row.try_get("carrier_key")?,
//         })
//     }
// }
