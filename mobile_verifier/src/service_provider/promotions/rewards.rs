use std::ops::Range;

use chrono::{DateTime, Utc};
use file_store::promotion_reward::{Entity, PromotionReward};
use futures::TryStreamExt;
use helium_crypto::PublicKeyBinary;
use mobile_config::client::{carrier_service_client::CarrierServiceVerifier, ClientError};
use sqlx::{postgres::PgRow, PgPool, Postgres, Row, Transaction};

use crate::service_provider::ServiceProviderId;

#[derive(Debug, Clone, PartialEq)]
pub struct PromotionRewardShares {
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

pub async fn fetch_promotion_rewards(
    pool: &PgPool,
    carrier: &impl CarrierServiceVerifier<Error = ClientError>,
    epoch: &Range<DateTime<Utc>>,
) -> anyhow::Result<Vec<PromotionRewardShares>> {
    let rewards = sqlx::query_as(
        r#"
        SELECT
            subscriber_id, NULL as gateway_key, SUM(shares)::bigint as shares, carrier_key
        FROM
            subscriber_promotion_rewards
        WHERE
            time_of_reward >= $1 AND time_of_reward < $2
        GROUP BY
            subscriber_id, carrier_key
        UNION
        SELECT
            NULL as subscriber_id, gateway_key, SUM(shares)::bigint as shares, carrier_key
        FROM
            gateway_promotion_rewards
        WHERE
            time_of_reward >= $1 AND time_of_reward < $2
        GROUP
            BY gateway_key, carrier_key
        "#,
    )
    .bind(epoch.start)
    .bind(epoch.end)
    .fetch(pool)
    .map_err(anyhow::Error::from)
    .and_then(|x: DbPromotionRewardShares| async move {
        let service_provider_id = carrier
            .payer_key_to_service_provider(&x.carrier_key.to_string())
            .await?;
        Ok(PromotionRewardShares {
            service_provider_id: service_provider_id as ServiceProviderId,
            rewardable_entity: x.rewardable_entity,
            shares: x.shares,
        })
    })
    .try_collect()
    .await?;

    Ok(rewards)
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

struct DbPromotionRewardShares {
    pub carrier_key: PublicKeyBinary,
    pub rewardable_entity: Entity,
    pub shares: u64,
}

impl sqlx::FromRow<'_, PgRow> for DbPromotionRewardShares {
    fn from_row(row: &PgRow) -> sqlx::Result<Self> {
        let subscriber_id: Option<Vec<u8>> = row.try_get("subscriber_id")?;
        let shares: i64 = row.try_get("shares")?;
        Ok(Self {
            rewardable_entity: if let Some(subscriber_id) = subscriber_id {
                Entity::SubscriberId(subscriber_id)
            } else {
                Entity::GatewayKey(row.try_get("gateway_key")?)
            },
            shares: shares as u64,
            carrier_key: row.try_get("carrier_key")?,
        })
    }
}
