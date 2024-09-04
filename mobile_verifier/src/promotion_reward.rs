use chrono::{DateTime, Utc};
use file_store::{
    file_info_poller::{FileInfoStream, LookbackBehavior},
    file_sink::FileSinkClient,
    file_source,
    file_upload::FileUpload,
    promotion_reward::{Entity, PromotionReward},
    traits::{FileSinkWriteExt, TimestampEncode},
    FileType,
};
use futures::{Stream, StreamExt, TryFutureExt, TryStreamExt};
use helium_crypto::PublicKeyBinary;
use helium_proto::services::{
    mobile_config::NetworkKeyRole,
    poc_mobile::{
        self as proto, PromotionRewardIngestReportV1, PromotionRewardStatus,
        VerifiedPromotionRewardV1,
    },
};
use mobile_config::{
    client::{
        authorization_client::AuthorizationVerifier,
        carrier_service_client::CarrierServiceVerifier, entity_client::EntityVerifier,
        AuthorizationClient, ClientError, EntityClient,
    },
    GatewayClient,
};
use rust_decimal::{prelude::*, Decimal, RoundingStrategy};
use sqlx::{postgres::PgRow, PgPool, Postgres, Row, Transaction};
use std::{
    collections::HashMap,
    ops::Range,
    pin::pin,
    time::{Duration, Instant},
};
use task_manager::{ManagedTask, TaskManager};
use tokio::sync::mpsc::Receiver;

use crate::{
    reward_shares::{ServiceProviderId, ServiceProviderRewards},
    GatewayResolver, Settings,
};

pub struct PromotionRewardDaemon {
    pool: PgPool,
    gateway_info_resolver: GatewayClient,
    authorization_verifier: AuthorizationClient,
    entity_verifier: EntityClient,
    promotion_rewards: Receiver<FileInfoStream<PromotionReward>>,
    promotion_rewards_sink: FileSinkClient<VerifiedPromotionRewardV1>,
}

impl PromotionRewardDaemon {
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
                Some(Duration::from_secs(15 * 60)),
                env!("CARGO_PKG_NAME"),
            )
            .await?;

        let (promotion_rewards, promotion_rewards_server) =
            file_source::continuous_source::<PromotionReward, _>()
                .state(pool.clone())
                .store(file_store)
                .lookback(LookbackBehavior::StartAfter(settings.start_after))
                .prefix(FileType::PromotionRewardIngestReport.to_string())
                .create()
                .await?;

        let promotion_reward_daemon = Self {
            pool,
            gateway_info_resolver,
            authorization_verifier,
            entity_verifier,
            promotion_rewards,
            promotion_rewards_sink,
        };

        Ok(TaskManager::builder()
            .add_task(valid_promotion_rewards_server)
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
                    self.process_file(file).await?;
                    metrics::histogram!("promotion_reward_processing_time")
                        .record(start.elapsed());
                }
            }
        }

        Ok(())
    }

    async fn process_file(&self, file: FileInfoStream<PromotionReward>) -> anyhow::Result<()> {
        tracing::info!("Processing promotion reward file {}", file.file_info.key);

        let mut transaction = self.pool.begin().await?;
        let reports = file.into_stream(&mut transaction).await?;

        let mut verified_promotion_rewards =
            pin!(ValidatedPromotionReward::validate_promotion_rewards(
                reports,
                &self.authorization_verifier,
                &self.gateway_info_resolver,
                &self.entity_verifier
            ));

        while let Some(promotion_reward) = verified_promotion_rewards.try_next().await? {
            promotion_reward.write(&self.promotion_rewards_sink).await?;
            if promotion_reward.is_valid() {
                promotion_reward.save(&mut transaction).await?;
            }
        }

        self.promotion_rewards_sink.commit().await?;
        transaction.commit().await?;

        Ok(())
    }
}

impl ManagedTask for PromotionRewardDaemon {
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

pub struct ValidatedPromotionReward {
    validity: PromotionRewardStatus,
    promotion_reward: PromotionReward,
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

impl ValidatedPromotionReward {
    async fn validate(
        promotion_reward: PromotionReward,
        authorization_verifier: &impl AuthorizationVerifier,
        gateway_info_resolver: &impl GatewayResolver,
        entity_verifier: &impl EntityVerifier,
    ) -> anyhow::Result<Self> {
        let validity = validate_promotion_reward(
            &promotion_reward,
            authorization_verifier,
            gateway_info_resolver,
            entity_verifier,
        )
        .await?;
        Ok(Self {
            validity,
            promotion_reward,
        })
    }

    fn validate_promotion_rewards<'a>(
        promotion_rewards: impl Stream<Item = PromotionReward> + 'a,
        authorization_verifier: &'a impl AuthorizationVerifier,
        gateway_info_resolver: &'a impl GatewayResolver,
        entity_verifier: &'a impl EntityVerifier,
    ) -> impl Stream<Item = anyhow::Result<Self>> + 'a {
        promotion_rewards.then(move |promotion_reward| async move {
            Self::validate(
                promotion_reward,
                authorization_verifier,
                gateway_info_resolver,
                entity_verifier,
            )
            .await
        })
    }

    fn is_valid(&self) -> bool {
        matches!(self.validity, PromotionRewardStatus::Valid)
    }

    async fn write(
        &self,
        promotion_rewards: &FileSinkClient<VerifiedPromotionRewardV1>,
    ) -> anyhow::Result<()> {
        promotion_rewards
            .write(
                VerifiedPromotionRewardV1 {
                    report: Some(PromotionRewardIngestReportV1 {
                        received_timestamp: self
                            .promotion_reward
                            .received_timestamp
                            .encode_timestamp_millis(),
                        report: Some(self.promotion_reward.clone().into()),
                    }),
                    status: self.validity as i32,
                    timestamp: Utc::now().encode_timestamp_millis(),
                },
                &[("validity", self.validity.as_str_name())],
            )
            .await?;
        Ok(())
    }

    async fn save(self, transaction: &mut Transaction<'_, Postgres>) -> anyhow::Result<()> {
        match self.promotion_reward.entity {
            Entity::SubscriberId(subscriber_id) => {
                sqlx::query(
                    r#"
                    INSERT INTO subscriber_promotion_rewards (time_of_reward, subscriber_id, carrier_key, shares)
                    VALUES ($1, $2, $3, $4)
                    ON CONFLICT DO NOTHING
                    "#
                )
                .bind(self.promotion_reward.timestamp)
                .bind(subscriber_id)
                .bind(self.promotion_reward.carrier_pub_key)
                .bind(self.promotion_reward.shares as i64)
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
                .bind(self.promotion_reward.timestamp)
                .bind(gateway_key)
                .bind(self.promotion_reward.carrier_pub_key)
                .bind(self.promotion_reward.shares as i64)
                .execute(&mut *transaction)
                .await?;
            }
        }
        Ok(())
    }
}

pub async fn clear_promotion_rewards(
    tx: &mut Transaction<'_, Postgres>,
    timestamp: &DateTime<Utc>,
) -> Result<(), sqlx::Error> {
    sqlx::query("DELETE FROM subscriber_promotion_rewards WHERE time_of_rewards < $1")
        .bind(timestamp)
        .execute(&mut *tx)
        .await?;
    sqlx::query("DELETE FROM gateway_promotion_rewards WHERE time_of_rewards < $1")
        .bind(timestamp)
        .execute(&mut *tx)
        .await?;
    Ok(())
}

pub struct AggregatePromotionRewards {
    pub rewards: Vec<ServiceProviderPromotionRewardShares>,
}

pub struct PromotionRewardShares {
    pub carrier_key: PublicKeyBinary,
    pub rewardable_entity: Entity,
    pub shares: u64,
}

pub struct ServiceProviderPromotionRewardShares {
    pub service_provider_id: ServiceProviderId,
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

// This could use a better name
#[derive(Copy, Clone, Default)]
struct SpPromotionRewardShares {
    shares_per_reward: Decimal,
    shares_per_matched_reward: Decimal,
}

impl AggregatePromotionRewards {
    pub async fn aggregate(
        pool: &PgPool,
        carrier: &impl CarrierServiceVerifier<Error = ClientError>,
        epoch: &Range<DateTime<Utc>>,
    ) -> anyhow::Result<Self> {
        let rewards = sqlx::query_as(
            r#"
            SELECT subscriber_id, NULL as gateway_key, SUM(shares), carrier_key
            FROM subscriber_promotion_rewards
            WHERE time_of_reward >= $1 AND time_of_reward < $2
            GROUP BY subscriber_id, carrier_key
            UNION
            SELECT NULL as subscriber_id, gateway_key, SUM(shares), carrier_key
            FROM gateway_promotion_rewards
            WHERE time_of_reward >= $1 AND time_of_reward < $2
            GROUP BY gateway_key, carrier_key
            "#,
        )
        .bind(epoch.start)
        .bind(epoch.end)
        .fetch(pool)
        .map_err(anyhow::Error::from)
        .and_then(|x: PromotionRewardShares| async move {
            let service_provider_id = carrier
                .payer_key_to_service_provider(&x.carrier_key.to_string())
                .await? as ServiceProviderId;
            Ok(ServiceProviderPromotionRewardShares {
                service_provider_id,
                rewardable_entity: x.rewardable_entity,
                shares: x.shares,
            })
        })
        .try_collect()
        .await?;
        Ok(Self { rewards })
    }

    pub fn into_rewards<'a>(
        self,
        sp_rewards: &mut ServiceProviderRewards,
        unallocated_sp_rewards: Decimal,
        reward_period: &'a Range<DateTime<Utc>>,
    ) -> impl Iterator<Item = (u64, proto::MobileRewardShare)> + 'a {
        let total_promotion_rewards_allocated =
            sp_rewards.get_total_rewards_allocated_for_promotion();
        let total_shares_per_service_provider = self.rewards.iter().fold(
            HashMap::<i32, Decimal>::default(),
            |mut shares, promotion_reward_share| {
                *shares
                    .entry(promotion_reward_share.service_provider_id)
                    .or_default() += Decimal::from(promotion_reward_share.shares);
                shares
            },
        );
        let sp_promotion_reward_shares: HashMap<_, _> = total_shares_per_service_provider
            .iter()
            .map(|(sp, total_shares)| {
                if total_promotion_rewards_allocated.is_zero() || total_shares.is_zero() {
                    (*sp, SpPromotionRewardShares::default())
                } else {
                    let rewards_allocated_for_promotion =
                        sp_rewards.take_rewards_allocated_for_promotion(sp);
                    let share_of_unallocated_pool =
                        rewards_allocated_for_promotion / total_promotion_rewards_allocated;
                    let sp_share = SpPromotionRewardShares {
                        shares_per_reward: rewards_allocated_for_promotion / total_shares,
                        shares_per_matched_reward: unallocated_sp_rewards
                            * share_of_unallocated_pool
                            / total_shares,
                    };
                    (*sp, sp_share)
                }
            })
            .collect();

        self.rewards
            .into_iter()
            .map(move |reward| {
                let shares = Decimal::from(reward.shares);
                let sp_promotion_reward_share = sp_promotion_reward_shares
                    .get(&reward.service_provider_id)
                    .copied()
                    .unwrap_or_default();
                let service_provider_amount = sp_promotion_reward_share.shares_per_reward * shares;
                let matched_amount = (sp_promotion_reward_share.shares_per_matched_reward * shares)
                    .min(service_provider_amount);
                proto::PromotionReward {
                    entity: Some(reward.rewardable_entity.into()),
                    service_provider_amount: service_provider_amount
                        .round_dp_with_strategy(0, RoundingStrategy::ToZero)
                        .to_u64()
                        .unwrap_or(0),
                    matched_amount: matched_amount
                        .round_dp_with_strategy(0, RoundingStrategy::ToZero)
                        .to_u64()
                        .unwrap_or(0),
                }
            })
            .filter(|x| x.service_provider_amount > 0)
            .map(move |reward| {
                (
                    reward.service_provider_amount + reward.matched_amount,
                    proto::MobileRewardShare {
                        start_period: reward_period.start.encode_timestamp(),
                        end_period: reward_period.end.encode_timestamp(),
                        reward: Some(proto::mobile_reward_share::Reward::PromotionReward(reward)),
                    },
                )
            })
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::reward_shares::ServiceProviderReward;
    use chrono::Duration;
    use helium_proto::services::poc_mobile::{
        mobile_reward_share::Reward, promotion_reward::Entity as ProtoEntity, PromotionReward,
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
        let promotion_rewards = AggregatePromotionRewards {
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
        let promotion_rewards = AggregatePromotionRewards {
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
        let promotion_rewards = AggregatePromotionRewards {
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
        let promotion_rewards = AggregatePromotionRewards {
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
        let promotion_rewards = AggregatePromotionRewards {
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
