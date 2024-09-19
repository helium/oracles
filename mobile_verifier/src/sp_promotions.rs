use chrono::{DateTime, Utc};
use file_store::{
    file_info_poller::{FileInfoStream, LookbackBehavior},
    file_sink::FileSinkClient,
    file_source,
    file_upload::FileUpload,
    promotion_reward::{Entity, PromotionReward},
    traits::{FileSinkCommitStrategy, FileSinkRollTime, FileSinkWriteExt, TimestampEncode},
    FileType,
};
use futures::{StreamExt, TryFutureExt, TryStreamExt};
use helium_crypto::PublicKeyBinary;
use helium_proto::{
    services::{
        mobile_config::NetworkKeyRole,
        poc_mobile::{
            PromotionRewardIngestReportV1, PromotionRewardStatus, VerifiedPromotionRewardV1,
        },
    },
    ServiceProviderPromotionFundV1,
};
use mobile_config::{
    client::{
        authorization_client::AuthorizationVerifier,
        carrier_service_client::CarrierServiceVerifier, entity_client::EntityVerifier,
        AuthorizationClient, ClientError, EntityClient,
    },
    GatewayClient,
};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use sqlx::{postgres::PgRow, PgPool, Postgres, Row, Transaction};
use std::{
    collections::HashMap,
    ops::Range,
    time::{Duration, Instant},
};
use task_manager::{ManagedTask, TaskManager};
use tokio::sync::mpsc::Receiver;

use crate::{GatewayResolver, Settings};

pub use rewards_db::clear_promotion_rewards;

// This type is used in lieu of the helium_proto::ServiceProvider enum so we can
// handle more than a single value without adding a hard deploy dependency to
// mobile-verifier when a new carrier is added..
pub type ServiceProviderId = i32;

pub struct SpPromotionDaemon {
    pool: PgPool,
    gateway_info_resolver: GatewayClient,
    authorization_verifier: AuthorizationClient,
    entity_verifier: EntityClient,
    promotion_funds: Receiver<FileInfoStream<ServiceProviderPromotionFundV1>>,
    promotion_rewards: Receiver<FileInfoStream<PromotionReward>>,
    promotion_rewards_sink: FileSinkClient<VerifiedPromotionRewardV1>,
}

impl SpPromotionDaemon {
    pub async fn create_managed_task(
        pool: PgPool,
        settings: &Settings,
        file_upload: FileUpload,
        file_store: file_store::FileStore,
        gateway_info_resolver: GatewayClient,
        authorization_verifier: AuthorizationClient,
        entity_verifier: EntityClient,
    ) -> anyhow::Result<impl ManagedTask> {
        let (promotion_rewards_sink, valid_promotion_rewards_server) =
            VerifiedPromotionRewardV1::file_sink(
                settings.store_base_path(),
                file_upload.clone(),
                FileSinkCommitStrategy::Automatic,
                FileSinkRollTime::Duration(Duration::from_secs(15 * 60)),
                env!("CARGO_PKG_NAME"),
            )
            .await?;

        let (promotion_rewards, promotion_rewards_server) =
            file_source::Continuous::msg_source::<PromotionReward, _>()
                .state(pool.clone())
                .store(file_store.clone())
                .lookback(LookbackBehavior::StartAfter(settings.start_after))
                .prefix(FileType::PromotionRewardIngestReport.to_string())
                .create()
                .await?;

        let (promotion_funds, promotion_funds_server) =
            file_source::Continuous::prost_source::<ServiceProviderPromotionFundV1, _>()
                .state(pool.clone())
                .store(file_store)
                .lookback(LookbackBehavior::StartAfter(settings.start_after))
                .prefix(FileType::ServiceProviderPromotionFund.to_string())
                .create()
                .await?;

        let promotion_reward_daemon = Self {
            pool,
            gateway_info_resolver,
            authorization_verifier,
            entity_verifier,
            promotion_funds,
            promotion_rewards,
            promotion_rewards_sink,
        };

        Ok(TaskManager::builder()
            .add_task(valid_promotion_rewards_server)
            .add_task(promotion_funds_server)
            .add_task(promotion_rewards_server)
            .add_task(promotion_reward_daemon)
            .build())
    }

    async fn run(mut self, shutdown: triggered::Listener) -> anyhow::Result<()> {
        loop {
            tokio::select! {
                _ = shutdown.clone() => {
                    tracing::info!("PromotionRewardDaemon shutting down");
                    break;
                }
                Some(file) = self.promotion_rewards.recv() => {
                    let start = Instant::now();
                    self.process_rewards_file(file).await?;
                    metrics::histogram!("promotion_reward_processing_time").record(start.elapsed());
                }
                Some(file) = self.promotion_funds.recv() => {
                    let start = Instant::now();
                    self.process_funds_file(file).await?;
                    metrics::histogram!("promotion_funds_processing_time").record(start.elapsed());
                }
            }
        }

        Ok(())
    }

    async fn process_rewards_file(
        &self,
        file: FileInfoStream<PromotionReward>,
    ) -> anyhow::Result<()> {
        tracing::info!(key = file.file_info.key, "Processing promotion reward file");

        let mut transaction = self.pool.begin().await?;
        let mut promotion_rewards = file.into_stream(&mut transaction).await?;

        while let Some(promotion_reward) = promotion_rewards.next().await {
            let promotion_reward_status = validate_promotion_reward(
                &promotion_reward,
                &self.authorization_verifier,
                &self.gateway_info_resolver,
                &self.entity_verifier,
            )
            .await?;

            if promotion_reward_status == PromotionRewardStatus::Valid {
                rewards_db::save_promotion_reward(&mut transaction, &promotion_reward).await?;
            }

            write_promotion_reward(
                &self.promotion_rewards_sink,
                &promotion_reward,
                promotion_reward_status,
            )
            .await?;
        }

        self.promotion_rewards_sink.commit().await?;
        transaction.commit().await?;

        Ok(())
    }

    async fn process_funds_file(
        &self,
        file: FileInfoStream<ServiceProviderPromotionFundV1>,
    ) -> anyhow::Result<()> {
        tracing::info!(key = file.file_info.key, "Processing promotion funds file");

        let mut txn = self.pool.begin().await?;

        let mut promotion_funds = file.into_stream(&mut txn).await?;
        while let Some(promotion_fund) = promotion_funds.next().await {
            funds_db::save_promotion_fund(
                &mut txn,
                promotion_fund.service_provider,
                promotion_fund.bps as u16,
            )
            .await?;
        }

        txn.commit().await?;

        Ok(())
    }
}

impl ManagedTask for SpPromotionDaemon {
    fn start_task(
        self: Box<Self>,
        shutdown: triggered::Listener,
    ) -> futures::prelude::future::LocalBoxFuture<'static, anyhow::Result<()>> {
        let handle = tokio::spawn(self.run(shutdown));
        Box::pin(
            handle
                .map_err(anyhow::Error::from)
                .and_then(|result| async move { result.map_err(anyhow::Error::from) }),
        )
    }
}

async fn validate_promotion_reward(
    promotion_reward: &PromotionReward,
    authorization_verifier: &impl AuthorizationVerifier,
    gateway_info_resolver: &impl GatewayResolver,
    entity_verifier: &impl EntityVerifier,
) -> anyhow::Result<PromotionRewardStatus> {
    if authorization_verifier
        .verify_authorized_key(
            &promotion_reward.carrier_pub_key,
            NetworkKeyRole::MobileCarrier,
        )
        .await
        .is_err()
    {
        return Ok(PromotionRewardStatus::InvalidCarrierKey);
    }
    match &promotion_reward.entity {
        Entity::SubscriberId(ref subscriber_id)
            if entity_verifier
                .verify_rewardable_entity(subscriber_id)
                .await
                .is_err() =>
        {
            Ok(PromotionRewardStatus::InvalidSubscriberId)
        }
        Entity::GatewayKey(ref gateway_key)
            if gateway_info_resolver
                .resolve_gateway(gateway_key)
                .await?
                .is_not_found() =>
        {
            Ok(PromotionRewardStatus::InvalidGatewayKey)
        }
        _ => Ok(PromotionRewardStatus::Valid),
    }
}

async fn write_promotion_reward(
    file_sink: &FileSinkClient<VerifiedPromotionRewardV1>,
    promotion_reward: &PromotionReward,
    status: PromotionRewardStatus,
) -> anyhow::Result<()> {
    file_sink
        .write(
            VerifiedPromotionRewardV1 {
                report: Some(PromotionRewardIngestReportV1 {
                    received_timestamp: promotion_reward
                        .received_timestamp
                        .encode_timestamp_millis(),
                    report: Some(promotion_reward.clone().into()),
                }),
                status: status as i32,
                timestamp: Utc::now().encode_timestamp_millis(),
            },
            &[("validity", status.as_str_name())],
        )
        .await?;
    Ok(())
}

pub mod rewards_db {
    use super::*;

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

    pub async fn get_promotion_rewards_by_carrier(
        pool: &PgPool,
        carrier: &impl CarrierServiceVerifier<Error = ClientError>,
        epoch: &Range<DateTime<Utc>>,
    ) -> anyhow::Result<HashMap<i32, Vec<ServiceProviderPromotionRewardShares>>> {
        let rewards = sqlx::query_as::<_, PromotionRewardShares>(
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
        .fetch_all(pool)
        .await?;

        let mut map = HashMap::new();
        for row in rewards {
            let service_provider_id = carrier
                .payer_key_to_service_provider(&row.carrier_key.to_string())
                .await?;

            let entry: &mut Vec<ServiceProviderPromotionRewardShares> =
                map.entry(service_provider_id as i32).or_default();

            entry.push(ServiceProviderPromotionRewardShares {
                service_provider_id: service_provider_id as ServiceProviderId,
                rewardable_entity: row.rewardable_entity,
                shares: row.shares,
            });
        }

        Ok(map)
    }

    pub async fn get_promotion_rewards(
        pool: &PgPool,
        carrier: &impl CarrierServiceVerifier<Error = ClientError>,
        epoch: &Range<DateTime<Utc>>,
    ) -> anyhow::Result<Vec<ServiceProviderPromotionRewardShares>> {
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
        .and_then(|x: PromotionRewardShares| async move {
            let service_provider_id = carrier
                .payer_key_to_service_provider(&x.carrier_key.to_string())
                .await?;
            Ok(ServiceProviderPromotionRewardShares {
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

    struct PromotionRewardShares {
        pub carrier_key: PublicKeyBinary,
        pub rewardable_entity: Entity,
        pub shares: u64,
    }

    impl sqlx::FromRow<'_, PgRow> for PromotionRewardShares {
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
}

#[derive(Debug, Clone, PartialEq)]
pub struct ServiceProviderPromotionRewardShares {
    pub service_provider_id: ServiceProviderId,
    pub rewardable_entity: Entity,
    pub shares: u64,
}

pub mod funds_db {
    use super::*;

    #[derive(Debug, Clone, Default)]
    pub struct ServiceProviderFunds(pub HashMap<ServiceProviderId, u16>);

    impl ServiceProviderFunds {
        pub fn get_fund_percent(&self, service_provider_id: ServiceProviderId) -> Decimal {
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
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use super::*;
    use crate::reward_shares::{
        ServiceProviderPromotionRewards, ServiceProviderReward, ServiceProviderRewards,
    };
    use chrono::Duration;
    use helium_proto::services::poc_mobile::{
        self as proto, mobile_reward_share::Reward, promotion_reward::Entity as ProtoEntity,
        PromotionReward,
    };
    use rust_decimal_macros::dec;

    fn aggregate_subcriber_rewards(
        rewards: impl Iterator<Item = (u64, proto::MobileRewardShare)>,
    ) -> HashMap<Vec<u8>, (u64, u64)> {
        let mut aggregated = HashMap::<Vec<u8>, (u64, u64)>::new();
        for (_, reward) in rewards {
            if let Some(Reward::PromotionReward(PromotionReward {
                entity: Some(ProtoEntity::SubscriberId(subscriber_id)),
                service_provider_amount,
                matched_amount,
            })) = reward.reward
            {
                let entry = aggregated.entry(subscriber_id).or_default();
                entry.0 += service_provider_amount;
                entry.1 += matched_amount;
            }
        }
        aggregated
    }

    #[test]
    fn ensure_no_rewards_if_none_allocated() {
        let now = Utc::now();
        let epoch = now - Duration::hours(24)..now;
        let mut rewards = HashMap::new();
        rewards.insert(
            0_i32,
            ServiceProviderReward {
                for_service_provider: dec!(100),
                for_promotions: dec!(0),
            },
        );
        let mut sp_rewards = ServiceProviderRewards { rewards };
        let promotion_rewards = ServiceProviderPromotionRewards {
            rewards: vec![ServiceProviderPromotionRewardShares {
                service_provider_id: 0,
                rewardable_entity: Entity::SubscriberId(vec![0]),
                shares: 1,
            }],
        };
        let result = aggregate_subcriber_rewards(promotion_rewards.into_rewards(
            &mut sp_rewards,
            dec!(0),
            &epoch,
        ));
        assert!(result.is_empty());
    }

    #[test]
    fn ensure_no_matched_rewards_if_no_unallocated() {
        let now = Utc::now();
        let epoch = now - Duration::hours(24)..now;
        let mut rewards = HashMap::new();
        rewards.insert(
            0_i32,
            ServiceProviderReward {
                for_service_provider: dec!(100),
                for_promotions: dec!(100),
            },
        );
        let mut sp_rewards = ServiceProviderRewards { rewards };
        let promotion_rewards = ServiceProviderPromotionRewards {
            rewards: vec![ServiceProviderPromotionRewardShares {
                service_provider_id: 0,
                rewardable_entity: Entity::SubscriberId(vec![0]),
                shares: 1,
            }],
        };
        let result = aggregate_subcriber_rewards(promotion_rewards.into_rewards(
            &mut sp_rewards,
            dec!(0), // 0 unallocated here
            &epoch,
        ));
        assert_eq!(sp_rewards.rewards.get(&0).unwrap().for_promotions, dec!(0));
        let result = result.get(&vec![0]).unwrap();
        assert_eq!(result.0, 100);
        assert_eq!(result.1, 0);
    }

    #[test]
    fn ensure_fully_matched_rewards_and_correctly_divided() {
        let now = Utc::now();
        let epoch = now - Duration::hours(24)..now;
        let mut rewards = HashMap::new();
        rewards.insert(
            0_i32,
            ServiceProviderReward {
                for_service_provider: dec!(100),
                for_promotions: dec!(1000),
            },
        );
        let mut sp_rewards = ServiceProviderRewards { rewards };
        let promotion_rewards = ServiceProviderPromotionRewards {
            rewards: vec![
                ServiceProviderPromotionRewardShares {
                    service_provider_id: 0,
                    rewardable_entity: Entity::SubscriberId(vec![0]),
                    shares: 1,
                },
                ServiceProviderPromotionRewardShares {
                    service_provider_id: 0,
                    rewardable_entity: Entity::SubscriberId(vec![1]),
                    shares: 2,
                },
            ],
        };
        let result = aggregate_subcriber_rewards(promotion_rewards.into_rewards(
            &mut sp_rewards,
            dec!(100),
            &epoch,
        ));
        assert_eq!(sp_rewards.rewards.get(&0).unwrap().for_promotions, dec!(0));
        let result1 = result.get(&vec![0]).unwrap();
        assert_eq!(result1.0, 333); // promotion rewards 1/3 of a 1000
        assert_eq!(result1.1, 33); // matched rewards 1/3 of a 100
        let result2 = result.get(&vec![1]).unwrap();
        assert_eq!(result2.0, 666);
        assert_eq!(result2.1, 66);
    }

    #[test]
    fn unallocated_does_not_exceed_promotion() {
        let now = Utc::now();
        let epoch = now - Duration::hours(24)..now;
        let mut rewards = HashMap::new();
        rewards.insert(
            0_i32,
            ServiceProviderReward {
                for_service_provider: dec!(100),
                for_promotions: dec!(100),
            },
        );
        let mut sp_rewards = ServiceProviderRewards { rewards };
        let promotion_rewards = ServiceProviderPromotionRewards {
            rewards: vec![
                ServiceProviderPromotionRewardShares {
                    service_provider_id: 0,
                    rewardable_entity: Entity::SubscriberId(vec![0]),
                    shares: 1,
                },
                ServiceProviderPromotionRewardShares {
                    service_provider_id: 0,
                    rewardable_entity: Entity::SubscriberId(vec![1]),
                    shares: 2,
                },
            ],
        };
        let result = aggregate_subcriber_rewards(promotion_rewards.into_rewards(
            &mut sp_rewards,
            dec!(1000),
            &epoch,
        ));
        assert_eq!(sp_rewards.rewards.get(&0).unwrap().for_promotions, dec!(0));
        let result1 = result.get(&vec![0]).unwrap();
        assert_eq!(result1.0, 33); // promotion rewards 1/3 of a 100
        assert_eq!(result1.1, 33); // matched rewards 1/3 of a 100 (not 1000 cause of promotion is 100)
        let result2 = result.get(&vec![1]).unwrap();
        assert_eq!(result2.0, 66);
        assert_eq!(result2.1, 66);
    }

    #[test]
    fn ensure_properly_scaled_unallocated_rewards() {
        let now = Utc::now();
        let epoch = now - Duration::hours(24)..now;
        let mut rewards = HashMap::new();
        rewards.insert(
            0,
            ServiceProviderReward {
                for_service_provider: dec!(100),
                for_promotions: dec!(100),
            },
        );
        rewards.insert(
            1,
            ServiceProviderReward {
                for_service_provider: dec!(100),
                for_promotions: dec!(200),
            },
        );
        let mut sp_rewards = ServiceProviderRewards { rewards };
        let promotion_rewards = ServiceProviderPromotionRewards {
            rewards: vec![
                ServiceProviderPromotionRewardShares {
                    service_provider_id: 0,
                    rewardable_entity: Entity::SubscriberId(vec![0]),
                    shares: 1,
                },
                ServiceProviderPromotionRewardShares {
                    service_provider_id: 1,
                    rewardable_entity: Entity::SubscriberId(vec![1]),
                    shares: 1,
                },
            ],
        };
        let result = aggregate_subcriber_rewards(promotion_rewards.into_rewards(
            &mut sp_rewards,
            dec!(100),
            &epoch,
        ));
        assert_eq!(sp_rewards.rewards.get(&0).unwrap().for_promotions, dec!(0));
        let result1 = result.get(&vec![0]).unwrap();
        assert_eq!(result1.0, 100);
        assert_eq!(result1.1, 33); // this is due to sp0 only giving 100 (1/3) out of 300 total for_promotions
        let result2 = result.get(&vec![1]).unwrap();
        assert_eq!(result2.0, 200);
        assert_eq!(result2.1, 66); // this is due to sp1 giving 200 (2/3)
    }
}
